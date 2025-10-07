use std::{
    any::Any,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use axum::async_trait;
use collection_macros::btreemap;
use color_eyre::{
    Result,
    eyre::{Context, ContextCompat},
};
use ctor::ctor;
use futures_util::{StreamExt, TryStreamExt, stream};
use miso_connectors::{
    Collection, Connector, ConnectorState, QueryHandle, QueryResponse, Split,
    stats::{CollectionStats, ConnectorStats, FieldStats},
};
use miso_kql::parse;
use miso_optimizations::Optimizer;
use miso_server::query_to_workflow::to_workflow_steps;
use miso_workflow::Workflow;
use miso_workflow_types::{expr::Expr, field::Field, field_unwrap, json, log::Log, value::Value};
use serde::{Deserialize, Serialize};
use test_case::test_case;
use tokio::{task::spawn_blocking, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::info;

const QUERY_SLEEP_PRE_CANCEL_TIME: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize, Deserialize)]
pub struct TestSplit {}

#[typetag::serde]
impl Split for TestSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[typetag::serde]
impl QueryHandle for TestHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl fmt::Display for TestHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "?")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TestHandle {}

#[derive(Debug, Serialize, Deserialize)]
struct TestConnector {
    collections: BTreeMap<String, Vec<Log>>,

    #[serde(skip_serializing, skip_deserializing)]
    apply_filter_tx: Option<std::sync::mpsc::Sender<Expr>>,

    #[serde(skip_serializing, skip_deserializing)]
    query_sleep_pre_cancel: bool,
}

impl TestConnector {
    fn new(
        apply_filter_tx: Option<std::sync::mpsc::Sender<Expr>>,
        query_sleep_pre_cancel: bool,
    ) -> Self {
        Self {
            collections: BTreeMap::new(),
            apply_filter_tx,
            query_sleep_pre_cancel,
        }
    }

    fn insert(&mut self, collection: String, logs: Vec<Log>) {
        self.collections.insert(collection, logs);
    }
}

#[async_trait]
#[typetag::serde]
impl Connector for TestConnector {
    fn get_collection(&self, collection: &str) -> Option<Collection> {
        self.collections
            .contains_key(collection)
            .then_some(Collection::default())
    }

    fn get_handle(&self, _: &str) -> Result<Box<dyn QueryHandle>> {
        Ok(Box::new(TestHandle {}))
    }

    fn get_splits(&self) -> Vec<Box<dyn Split>> {
        vec![Box::new(TestSplit {}) as Box<dyn Split>]
    }

    async fn query(
        &self,
        collection: &str,
        _handle: &dyn QueryHandle,
        _split: Option<&dyn Split>,
    ) -> Result<QueryResponse> {
        if self.query_sleep_pre_cancel {
            sleep(QUERY_SLEEP_PRE_CANCEL_TIME).await;
        }

        let logs = self
            .collections
            .get(collection)
            .context("collection to exist")?
            .clone();
        Ok(QueryResponse::Logs(Box::pin(stream::iter(logs).map(Ok))))
    }

    fn apply_filter(&self, ast: &Expr, _handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        if let Some(tx) = &self.apply_filter_tx {
            tx.send(ast.clone()).expect("send() apply dynamic filter");
        }
        None
    }

    async fn fetch_stats(&self) -> Option<ConnectorStats> {
        let mut stats = ConnectorStats::new();
        for (collection, logs) in self.collections.clone() {
            let mut collection_stats = CollectionStats::new();
            for (field, distinct_count) in distinct_field_values(logs) {
                collection_stats.insert(
                    field,
                    FieldStats {
                        distinct_count: Some(distinct_count),
                    },
                );
            }
            stats.insert(collection, collection_stats);
        }
        Some(stats)
    }

    async fn close(&self) {}
}

fn distinct_field_values(logs: Vec<Log>) -> Vec<(String, u32)> {
    let mut field_values: BTreeMap<String, BTreeSet<Value>> = BTreeMap::new();

    for log in logs {
        for (key, value) in log {
            field_values.entry(key).or_default().insert(value);
        }
    }

    field_values
        .into_iter()
        .map(|(k, v)| (k, v.len() as u32))
        .collect()
}

#[ctor]
fn init() {
    color_eyre::install().unwrap();
    tracing_subscriber::fmt::init();
}

async fn check_multi_connectors(
    query: &str,
    input: BTreeMap<&str, BTreeMap<&str, &str>>,
    views_raw: BTreeMap<&str, &str>,
    expected: &str,
    should_cancel: bool,
    apply_filter_tx: Option<std::sync::mpsc::Sender<Expr>>,
) -> Result<()> {
    let expected_logs = {
        let mut v: Vec<_> = serde_json::from_str::<Vec<Value>>(expected)
            .context("parse expected output logs from json")?
            .into_iter()
            .collect();
        v.sort();
        v
    };

    let mut test_connectors = BTreeMap::new();
    for (connector_name, collections) in input {
        for (collection, raw_logs) in collections {
            let input_logs: Vec<Log> =
                serde_json::from_str(raw_logs).context("parse input logs from json")?;
            test_connectors
                .entry(connector_name.to_string())
                .or_insert_with(|| TestConnector::new(apply_filter_tx.clone(), should_cancel))
                .insert(collection.to_string(), input_logs);
        }
    }

    let test_connectors = test_connectors
        .into_iter()
        .map(|(name, connector)| (name, Arc::new(connector)))
        .collect::<BTreeMap<String, Arc<TestConnector>>>();

    let mut connectors = BTreeMap::new();
    for (name, connector) in test_connectors {
        let stats = connector.fetch_stats().await;
        let connector_state = Arc::new(ConnectorState::new_with_static_stats(connector, stats));
        connectors.insert(name, connector_state);
    }

    let views = {
        let mut views = BTreeMap::new();
        for (name, view_raw) in views_raw {
            let steps = parse(view_raw).expect("parse view query");
            views.insert(name.to_string(), steps);
        }
        views
    };

    let steps = to_workflow_steps(&connectors, &views, parse(query).expect("parse query"))
        .expect("workflow steps to compile");

    let optimizer = Optimizer::default();

    let steps_cloned = steps.clone();
    let optimized_steps = spawn_blocking(move || optimizer.optimize(steps_cloned)).await?;

    let no_optimizations_workflow = Workflow::new(steps);
    info!("No optimizations:\n{no_optimizations_workflow}");
    let optimizations_workflow = Workflow::new(optimized_steps);
    info!("Optimized:\n{optimizations_workflow}");

    let cancel = CancellationToken::new();
    if should_cancel {
        cancel.cancel();
    }
    let mut logs_stream = no_optimizations_workflow
        .execute(cancel)
        .context("non optimized workflow execute")?;

    let mut logs = Vec::new();
    while let Some(log) = logs_stream.try_next().await.context("log stream")? {
        logs.push(json!(log));
    }
    logs.sort();

    assert_eq!(
        expected_logs, logs,
        "non optimized workflow streamed logs not equal to expected (left is expected, right is what we received)",
    );

    let cancel = CancellationToken::new();
    if should_cancel {
        cancel.cancel();
    }
    let mut logs_stream = optimizations_workflow
        .execute(cancel)
        .context("optimized workflow execute")?;

    let mut optimized_logs = Vec::with_capacity(logs.len());
    while let Some(log) = logs_stream.try_next().await? {
        optimized_logs.push(json!(log));
    }
    optimized_logs.sort();

    assert_eq!(
        logs, optimized_logs,
        "results of workflow should equal results of optimized workflow query, even after sorting"
    );

    Ok(())
}

#[bon::builder]
async fn check_multi_collection(
    query: &str,
    input: BTreeMap<&str, &str>,
    views: Option<BTreeMap<&str, &str>>,
    expect: &str,
    cancel: Option<bool>,
    apply_filter_tx: Option<std::sync::mpsc::Sender<Expr>>,
) -> Result<()> {
    check_multi_connectors(
        query,
        btreemap! {"test" => input},
        views.unwrap_or_default(),
        expect,
        cancel.unwrap_or(false),
        apply_filter_tx,
    )
    .await
}

/// Creates a test connector named 'test' and a collection named 'c' which will include logs
/// given by |input|. Tests that the logs returned by running |query| are equal to |expected|.
async fn check(query: &str, input: &str, expected: &str) -> Result<()> {
    check_multi_collection()
        .query(query)
        .input(btreemap! {"c" => input})
        .expect(expected)
        .call()
        .await
}

async fn check_cancel(query: &str, input: &str) -> Result<()> {
    check_multi_collection()
        .query(query)
        .input(btreemap! {"c" => input})
        .expect("[]")
        .cancel(true)
        .call()
        .await
}

#[tokio::test]
async fn scan() -> Result<()> {
    let logs = r#"[{"hello": "world"}]"#;
    check("test.c", logs, logs).await
}

#[tokio::test]
async fn cancel() -> Result<()> {
    check_cancel("test.c", r#"[{"hello": "world"}]"#).await
}

#[tokio::test]
async fn scan_view() -> Result<()> {
    check_multi_collection()
        .query(r#"views.v | where hello endswith "rld""#)
        .input(
            btreemap! {"c" => r#"[{"hello": "world"}, {"hello": "worrrr"}, {"hello2": "world2"}]"#},
        )
        .views(btreemap! {
            "v" => r#"test.c | where hello startswith "wor""#
        })
        .expect(r#"[{"hello": "world"}]"#)
        .call()
        .await
}

#[tokio::test]
async fn filter_eq() -> Result<()> {
    check(
        r#"test.c | where world == 2"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}]"#,
        r#"[{"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_float() -> Result<()> {
    check(
        r#"test.c | where world == 2.5"#,
        r#"[{"hello": "world"}, {"world": 1.5}, {"world": 2.5}]"#,
        r#"[{"world": 2.5}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_string() -> Result<()> {
    check(
        r#"test.c | where world == "200""#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": "200"}]"#,
        r#"[{"world": "200"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_bool() -> Result<()> {
    check(
        r#"test.c | where world == false"#,
        r#"[{"hello": "world"}, {"world": 5}, {"world": true}, {"world": false}]"#,
        r#"[{"world": false}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_null() -> Result<()> {
    check(
        r#"test.c | where world == null"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": null}]"#,
        r#"[{"world": null}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_fields() -> Result<()> {
    check(
        r#"test.c | where world == world2"#,
        r#"[{"hello": "world"}, {"world": 1, "world2": 1}, {"world": "33", "world2": 33}]"#,
        r#"[{"world": 1, "world2": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_not_fields() -> Result<()> {
    check(
        r#"test.c | where w == not(w2)"#,
        r#"[{"hello": "world"}, {"w": true, "w2": 0}, {"w": false, "w2": "a"}, {"w": true, "w2": 22.6}]"#,
        r#"[{"w": true, "w2": 0}, {"w": false, "w2": "a"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_not_eq_fields() -> Result<()> {
    check(
        r#"test.c | where not(w == w2)"#,
        r#"[{"hello": "world"}, {"w": 100, "w2": 0}, {"w": "abc", "w2": "a"}, {"w": 100.3, "w2": 100.3}]"#,
        r#"[{"hello": "world"}, {"w": 100, "w2": 0}, {"w": "abc", "w2": "a"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_ne() -> Result<()> {
    check(
        r#"test.c | where world != 2"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}]"#,
        r#"[{"world": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_gt() -> Result<()> {
    check(
        r#"test.c | where world > 1"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 1}]"#,
        r#"[{"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_lt() -> Result<()> {
    check(
        r#"test.c | where world < 3"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 3}]"#,
        r#"[{"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_gte() -> Result<()> {
    check(
        r#"test.c | where world >= 2"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}, {"world": 3}]"#,
        r#"[{"world": 2}, {"world": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_lte() -> Result<()> {
    check(
        r#"test.c | where world <= 3"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 3}, {"world": 4}]"#,
        r#"[{"world": 2}, {"world": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_gt_now() -> Result<()> {
    check(
        r#"test.c | where now() > datetime(1980-01-01)"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 1}]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_add_sub() -> Result<()> {
    check(
        r#"test.c | where world == 3 + 2 - 4"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}]"#,
        r#"[{"world": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_mul_div() -> Result<()> {
    check(
        r#"test.c | where world == 3 * 2 / 4"#,
        r#"[{"hello": "world"}, {"world": 1.5}, {"world": 2}]"#,
        r#"[{"world": 1.5}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_and() -> Result<()> {
    check(
        r#"test.c | where world == 3 and hello == "world""#,
        r#"[{"hello": "world", "world": 3}, {"hello": "woold", "world": 3}, {"hello": "world", "world": 2}]"#,
        r#"[{"hello": "world", "world": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_or() -> Result<()> {
    check(
        r#"test.c | where world == 3 or hello == "world""#,
        r#"[{"hello": "world", "world": 3}, {"hello": "woold", "world": 3}, {"hello": "world", "world": 2}, {"hello": "woold", "world": 4}]"#,
        r#"[{"hello": "world", "world": 3}, {"hello": "woold", "world": 3}, {"hello": "world", "world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_in() -> Result<()> {
    check(
        r#"test.c | where world in (2, 4)"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 4}, {"world": 2}]"#,
        r#"[{"world": 4}, {"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_contains() -> Result<()> {
    check(
        r#"test.c | where hello contains "wor""#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "aaawora"}, {"hello": "woold"}]"#,
        r#"[{"hello": "world"}, {"hello": "aaawora"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_starts_with() -> Result<()> {
    check(
        r#"test.c | where hello startswith "wor""#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "aaawora"}, {"hello": "woold"}]"#,
        r#"[{"hello": "world"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_starts_with_on_object() -> Result<()> {
    check(
        r#"test.c | where hello.there startswith "wor""#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": {"there": "woold"}}, {"hello": {"there": "world"}}]"#,
        r#"[{"hello": {"there": "world"}}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_ends_with() -> Result<()> {
    check(
        r#"test.c | where hello endswith "ora""#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "aaawora"}, {"hello": "woold"}]"#,
        r#"[{"hello": "aaawora"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_has() -> Result<()> {
    check(
        r#"test.c | where hello has "there world""#,
        r#"[
            {"hello": "world"},
            {"hello": "there wor"},
            {"hello": "there.world"},
            {"world": 2},
            {"hello": "abc-THeRE world/def"},
            {"hello": "there world end"},
            {"hello": "start,tHere world"}
        ]"#,
        r#"[
            {"hello": "abc-THeRE world/def"},
            {"hello": "there world end"},
            {"hello": "start,tHere world"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn filter_has_cs() -> Result<()> {
    check(
        r#"test.c | where hello has_cs "there world""#,
        r#"[
            {"hello": "world"},
            {"hello": "there wor"},
            {"hello": "there.world"},
            {"world": 2},
            {"hello": "abc-THeRE world/def"},
            {"hello": "there world end"},
            {"hello": "start,tHere world"}
        ]"#,
        r#"[{"hello": "there world end"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_exists() -> Result<()> {
    check(
        r#"test.c | where exists(hello)"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "woold"}]"#,
        r#"[{"hello": "world"}, {"hello": "woold"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_exists_on_object() -> Result<()> {
    check(
        r#"test.c | where exists(hello.there)"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": {"there": "abc"}}, {"hello": {"world": "def"}}]"#,
        r#"[{"hello": {"there": "abc"}}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_exists_null() -> Result<()> {
    check(
        r#"test.c | where exists(hello)"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": null}]"#,
        r#"[{"hello": "world"}, {"hello": null}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_not_exists() -> Result<()> {
    check(
        r#"test.c | where not(exists(hello))"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "woold"}]"#,
        r#"[{"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn project_add() -> Result<()> {
    check(
        r#"test.c | project world=world, test=toreal(world) + 2"#,
        r#"[{"world": 2}, {"world": 1}, {"hello": "world"}]"#,
        r#"[{"world": 2, "test": 4.0}, {"world": 1, "test": 3.0}, {}]"#,
    )
    .await
}

#[tokio::test]
async fn rename() -> Result<()> {
    check(
        r#"test.c | project-rename out=world, here=hello"#,
        r#"[{"world": 2}, {"world": 1}, {"hello": "world", "random": 42}]"#,
        r#"[{"out": 2}, {"out": 1}, {"here": "world", "random": 42}]"#,
    )
    .await
}

#[tokio::test]
async fn project_filter() -> Result<()> {
    check(
        r#"
        test.c
        | project rename=world, literal=1, test=doesnt_exist
        | where rename == literal * 2
        "#,
        r#"[{"world": 2}, {"world": 1}, {"hello": "world"}]"#,
        r#"[{"rename": 2, "literal": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn project_array() -> Result<()> {
    check(
        r#"test.c | project world=world[0].x[1]"#,
        r#"[
            {"world": [{"x": [1, 2]}]},
            {"world": [{"x": [5, 6]}]},
            {"hello": ["world"]}
        ]"#,
        r#"[{"world": 2}, {"world": 6}, {}]"#,
    )
    .await
}

#[tokio::test]
async fn extend_add() -> Result<()> {
    check(
        r#"test.c | extend world=world, test=toreal(world) + 2"#,
        r#"[{"world": 2}, {"world": 1}, {"hello": "world"}]"#,
        r#"[{"world": 2, "test": 4.0}, {"world": 1, "test": 3.0}, {"hello": "world"}]"#,
    )
    .await
}

#[tokio::test]
async fn sort_asc_then_desc() -> Result<()> {
    check(
        r#"test.c | sort by world, test desc"#,
        r#"[{"world": 3, "test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"world": 2, "test": 6}, {"world": 2, "test": 3}, {"world": 3, "test": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn sort_nulls_order() -> Result<()> {
    check(
        r#"test.c | sort by world, test nulls first"#,
        r#"[{"world": 4, "test": 1}, {}, {"world": 3, "test": 1}, {"world": null, "test": 1}, {"world": 4, "test": null}]"#,
        r#"[{"world": 3, "test": 1}, {"world": 4, "test": null}, {"world": 4, "test": 1}, {}, {"world": null, "test": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn limit() -> Result<()> {
    check(
        r#"test.c | take 2"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}, {"world": 3}]"#,
        r#"[{"hello": "world"}, {"world": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn topn() -> Result<()> {
    check(
        r#"test.c | top 2 by world, test desc"#,
        r#"[{"world": 3, "test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"world": 2, "test": 6}, {"world": 2, "test": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn sort_limit() -> Result<()> {
    check(
        r#"test.c | sort by world, test desc | take 2"#,
        r#"[{"world": 3, "test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"world": 2, "test": 6}, {"world": 2, "test": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn sort_limit_count() -> Result<()> {
    check(
        r#"test.c | sort by world, test desc | take 2 | count"#,
        r#"[{"world": 3, "test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"count": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn summarize() -> Result<()> {
    check(
        r#"
        test.c
        | summarize max_x=max(x),
                    min_x=min(x),
                    sum_x=sum(x),
                    dcount_z=dcount(z),
                    c=count(),
                    cif=countif(z == 2)
          by y
        "#,
        r#"[{"x": 3, "y": 3, "z": 2}, {"x": 5, "y": 6, "z": 1}, {"x": 1, "y": 3, "z": 2}, {"x": 9, "y": 6, "z": 3}]"#,
        r#"[
            {"max_x": 3, "min_x": 1, "sum_x": 4.0, "dcount_z": 1, "c": 2, "cif": 2, "y": 3},
            {"max_x": 9, "min_x": 5, "sum_x": 14.0, "dcount_z": 2, "c": 2, "cif": 0, "y": 6}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn project_summarize() -> Result<()> {
    check(
        r#"
        test.c
        | project x=a, y=b, z=c, should_not_appear=doesnt_exist
        | summarize max_x=max(x),
                    min_x=min(x),
                    sum_x=sum(x),
                    dcount_z=dcount(z),
                    cnt=count(),
                    cif=countif(z == 2)
          by y
        "#,
        r#"[{"a": 3, "b": 3, "c": 2}, {"a": 5, "b": 6, "c": 1}, {"a": 1, "b": 3, "c": 2}, {"a": 9, "b": 6, "c": 3}]"#,
        r#"[
            {"max_x": 3, "min_x": 1, "sum_x": 4.0, "dcount_z": 1, "cnt": 2, "cif": 2, "y": 3},
            {"max_x": 9, "min_x": 5, "sum_x": 14.0, "dcount_z": 2, "cnt": 2, "cif": 0, "y": 6}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn summarize_bin() -> Result<()> {
    check(
        r#"
        test.c
        | summarize max_x=max(x),
                    min_x=min(x),
                    sum_x=sum(x),
                    dcount_x=dcount(x),
                    c=count()
          by bin(y, 2)
        "#,
        r#"[{"x": 3, "y": 0}, {"x": 5, "y": 1}, {"x": 1, "y": 4}, {"x": 9, "y": 5}, {"x": 5}]"#,
        r#"[
            {"max_x": 5, "min_x": 3, "sum_x": 8.0, "dcount_x": 2, "c": 2, "y": 0.0},
            {"max_x": 9, "min_x": 1, "sum_x": 10.0, "dcount_x": 2, "c": 2, "y": 4.0}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn project_summarize_bin() -> Result<()> {
    check(
        r#"
        test.c
        | project x=a, y=b, should_not_appear=doesnt_exist
        | summarize max_x=max(x),
                    min_x=min(x),
                    sum_x=sum(x),
                    dcount_x=dcount(x),
                    cnt=count()
          by bin(y, 2)
        "#,
        r#"[{"a": 3, "b": 0}, {"a": 5, "b": 1}, {"a": 1, "b": 4}, {"a": 9, "b": 5}, {"a": 5}]"#,
        r#"[
            {"max_x": 5, "min_x": 3, "sum_x": 8.0, "dcount_x": 2, "cnt": 2, "y": 0.0},
            {"max_x": 9, "min_x": 1, "sum_x": 10.0, "dcount_x": 2, "cnt": 2, "y": 4.0}
        ]"#,
    )
    .await
}

#[tokio::test]
#[test_case(1)]
#[test_case(10)]
async fn join_inner(partitions: usize) -> Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();

    check_multi_collection()
        .query(
            &format!(r#"test.left | join hint.partitions={partitions} (test.right) on id"#)
        )
        .input(btreemap!{
            "left"  => r#"[{"id": 1, "value": "one"}, {"id": 1, "value": "dup"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]"#,
            "right" => r#"[{"id": 1, "value": "ONE"}, {"id": 2, "value": "DUP"}, {"id": 2, "value": "TWO"}, {"id": 4, "value": "FOUR"}]"#,
        })
        .expect(
            r#"[
                {"id": 1, "value_left": "one", "value_right": "ONE"},
                {"id": 1, "value_left": "dup", "value_right": "ONE"},
                {"id": 2, "value_left": "two", "value_right": "TWO"},
                {"id": 2, "value_left": "two", "value_right": "DUP"}
            ]"#
        )
        .apply_filter_tx(tx)
        .call()
        .await
        .context("check multi collection")?;

    let ast = rx.recv().context("recv() apply dynamic filter")?;
    match ast {
        Expr::In(id_box, mut actual_vec) => {
            assert_eq!(id_box, Box::new(Expr::Field(field_unwrap!("id"))));

            let mut expected_vec = vec![
                Expr::Literal(1.into()),
                Expr::Literal(2.into()),
                Expr::Literal(3.into()),
            ];

            let compare_filter_asts = |a: &Expr, b: &Expr| -> Ordering {
                match (a, b) {
                    (Expr::Literal(val_a), Expr::Literal(val_b)) => val_a.cmp(val_b),
                    _ => {
                        panic!("Unexpected Expr variants in Vec during comparison: {a:?} vs {b:?}")
                    }
                }
            };

            actual_vec.sort_by(compare_filter_asts);
            expected_vec.sort_by(compare_filter_asts);

            assert_eq!(actual_vec, expected_vec);
        }
        _ => {
            panic!("Expected Expr::In variant, but got: {ast:?}");
        }
    }

    assert!(matches!(
        rx.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty | std::sync::mpsc::TryRecvError::Disconnected)
    ));

    Ok(())
}

#[tokio::test]
#[test_case(1)]
#[test_case(10)]
async fn join_outer(partitions: usize) -> Result<()> {
    check_multi_collection()
        .query(
            &format!(r#"test.left | join kind=outer hint.partitions={partitions} (test.right) on id"#)
        )
        .input(btreemap!{
            "left"  => r#"[{"id": 1, "value": "one"}, {"id": 1, "value": "dup"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]"#,
            "right" => r#"[{"id": 1, "value": "ONE"}, {"id": 2, "value": "TWO"}, {"id": 2, "value": "DUP"}, {"id": 4, "value": "FOUR"}]"#,
        })
        .expect(
            r#"[
                {"id": 1, "value_left": "one", "value_right": "ONE"},
                {"id": 1, "value_left": "dup", "value_right": "ONE"},
                {"id": 2, "value_left": "two", "value_right": "TWO"},
                {"id": 2, "value_left": "two", "value_right": "DUP"},
                {"id": 3, "value": "three"},
                {"id": 4, "value": "FOUR"}
            ]"#
        )
        .call()
        .await
}

#[tokio::test]
#[test_case(1)]
#[test_case(10)]
async fn join_left(partitions: usize) -> Result<()> {
    check_multi_collection()
        .query(
            &format!(r#"test.left | join kind=left hint.partitions={partitions} (test.right) on id"#)
        )
        .input(btreemap!{
            "left"  => r#"[{"id": 1, "value": "one"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]"#,
            "right" => r#"[{"id": 1, "value": "ONE"}, {"id": 2, "value": "TWO"}, {"id": 4, "value": "FOUR"}]"#,
        })
        .expect(
            r#"[
                {"id": 1, "value": "one"},
                {"id": 2, "value": "two"},
                {"id": 3, "value": "three"}
            ]"#
        )
        .call()
        .await
}

#[tokio::test]
#[test_case(1)]
#[test_case(10)]
async fn join_right(partitions: usize) -> Result<()> {
    check_multi_collection()
        .query(
            &format!(r#"test.left | join kind=right hint.partitions={partitions} (test.right) on id"#)
        )
        .input(btreemap!{
            "left"  => r#"[{"id": 1, "value": "one"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]"#,
            "right" => r#"[{"id": 1, "value": "ONE"}, {"id": 2, "value": "TWO"}, {"id": 4, "value": "FOUR"}]"#,
        })
        .expect(
            r#"[
                {"id": 1, "value": "ONE"},
                {"id": 2, "value": "TWO"},
                {"id": 4, "value": "FOUR"}
            ]"#
        )
        .call()
        .await
}

#[tokio::test]
async fn count() -> Result<()> {
    check(
        r#"test.c | count"#,
        r#"[{"world": 3}, {"test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"count": 4}]"#,
    )
    .await
}

#[tokio::test]
async fn count_on_count() -> Result<()> {
    check(
        r#"test.c | count | count"#,
        r#"[{"world": 3}, {"test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"count": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn union() -> Result<()> {
    check_multi_collection()
        .query(r#"test.x | union (test.y)"#)
        .input(btreemap!{
            "x" => r#"[{"id": 1, "value": "one"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]"#,
            "y" => r#"[{"id": 4, "value": "four"}, {"id": 5, "value": "five"}]"#,
        })
        .expect(
            r#"[
                {"id": 1, "value": "one"},
                {"id": 2, "value": "two"},
                {"id": 3, "value": "three"},
                {"id": 4, "value": "four"},
                {"id": 5, "value": "five"}
            ]"#
        )
        .call()
        .await
}

#[tokio::test]
async fn filter_non_existant_field_then_limit_after_union() -> Result<()> {
    check_multi_collection()
        .query(r#"test.x | union (test.y) | where id == 2 | take 4"#)
        .input(btreemap! {
            "x" => r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#,
            "y" => r#"[{"xd": 1}, {"xd": 2}, {"xd": 3}]"#,
        })
        .expect(r#"[{"id": 2}]"#)
        .call()
        .await
}

#[tokio::test]
async fn filter_exists_field_and_limit_after_union() -> Result<()> {
    check_multi_collection()
        .query(r#"test.x | union (test.y) | where not(exists(id)) or id == 2 | take 4"#)
        .input(btreemap! {
            "x" => r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#,
            "y" => r#"[{"xd": 1}, {"xd": 2}, {"xd": 3}]"#,
        })
        .expect(r#"[{"id": 2}, {"xd": 1}, {"xd": 2}, {"xd": 3}]"#)
        .call()
        .await
}

#[tokio::test]
async fn union_summarize() -> Result<()> {
    check_multi_collection()
        .query(
            r#"
            test.x
            | union (test.y)
            | summarize max_x=max(x),
                        min_x=min(x),
                        sum_x=sum(x),
                        c=count()
              by y
            "#,
        )
        .input(btreemap! {
            "x" => r#"[{"x": 3, "y": 3}, {"x": 5, "y": 6}, {"x": 1, "y": 3}, {"x": 9, "y": 6}]"#,
            "y" => r#"[{"x": 6, "y": 3}, {"x": 9, "y": 6}, {"x": 7, "y": 3}, {"x": 2, "y": 6}]"#,
        })
        .expect(
            r#"[
                {"max_x": 7, "min_x": 1, "sum_x": 17.0, "c": 4, "y": 3},
                {"max_x": 9, "min_x": 2, "sum_x": 25.0, "c": 4, "y": 6}
            ]"#,
        )
        .call()
        .await
}

#[tokio::test]
async fn union_count() -> Result<()> {
    check_multi_collection()
        .query(r#"test.x | union (test.y) | count"#)
        .input(btreemap! {
            "x" => r#"[{"x": 0}, {"x": 1}, {"x": 2}]"#,
            "y" => r#"[{"x": 3}, {"x": 4}, {"x": 5}, {"x": 6}]"#,
        })
        .expect(r#"[{"count": 7}]"#)
        .call()
        .await
}

#[tokio::test]
async fn expand_array() -> Result<()> {
    check(
        r#"test.c | mv-expand b"#,
        r#"[{"a": 1, "b": [10, 20]}, {"a": 2, "b": ["a", "b"]}]"#,
        r#"[
            {"a": 1, "b": 10},
            {"a": 1, "b": 20},
            {"a": 2, "b": "a"},
            {"a": 2, "b": "b"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_zip() -> Result<()> {
    check(
        r#"test.c | mv-expand b, c"#,
        r#"[{"a": 1, "b": ["x", "y"], "c": [5, 4, 3]}]"#,
        r#"[
            {"a": 1, "b": "x", "c": 5},
            {"a": 1, "b": "y", "c": 4},
            {"a": 1, "b": null, "c": 3}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_deeply_nested_path() -> Result<()> {
    check(
        r#"test.c | mv-expand a.b.c.d.e"#,
        r#"[{"a": {"b": {"c": {"d": {"e": [1, 2]}}}}}]"#,
        r#"[
            {"a": {"b": {"c": {"d": {"e": 1}}}}},
            {"a": {"b": {"c": {"d": {"e": 2}}}}}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_non_existent_field() -> Result<()> {
    check(
        r#"test.c | mv-expand missing"#,
        r#"[{"id": 1, "name": "test"}]"#,
        r#"[{"id": 1, "name": "test"}]"#,
    )
    .await
}
