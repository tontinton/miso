use std::{
    any::Any,
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
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
use miso_workflow::{
    MISO_METADATA_FIELD_NAME, Workflow,
    limits::WorkflowLimits,
    partial_stream::{PARTIAL_STREAM_DONE_FIELD_NAME, PartialStream},
    sort::SortError,
};
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

fn distinct_field_values(logs: Vec<Log>) -> Vec<(String, u64)> {
    let mut field_values: BTreeMap<String, BTreeSet<Value>> = BTreeMap::new();

    for log in logs {
        for (key, value) in log {
            field_values.entry(key).or_default().insert(value);
        }
    }

    field_values
        .into_iter()
        .map(|(k, v)| (k, v.len() as u64))
        .collect()
}

#[ctor]
fn init() {
    color_eyre::install().unwrap();
    tracing_subscriber::fmt::init();
}

async fn assert_workflows(
    no_optimizations_workflow: Workflow,
    optimizations_workflow: Workflow,
    expected_logs: &[Value],
    workflow_limits: WorkflowLimits,
    should_cancel: bool,
) -> Result<()> {
    let cancel = CancellationToken::new();
    if should_cancel {
        cancel.cancel();
    }
    let mut logs_stream = no_optimizations_workflow
        .execute(workflow_limits.clone(), cancel)
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
        .execute(workflow_limits, cancel)
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

#[allow(clippy::too_many_arguments)]
async fn check_multi_connectors(
    query: &str,
    input: BTreeMap<&str, BTreeMap<&str, &str>>,
    views_raw: BTreeMap<&str, &str>,
    expected: &str,
    should_cancel: bool,
    apply_filter_tx: Option<std::sync::mpsc::Sender<Expr>>,
    run_only_once: bool,
    workflow_limits: WorkflowLimits,
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

    let test_runs: usize = std::env::var("WORKFLOW_TEST_RUNS")
        .ok()
        .and_then(|test_run_str| test_run_str.parse().ok())
        .unwrap_or(1);

    if run_only_once || test_runs == 1 {
        assert_workflows(
            no_optimizations_workflow,
            optimizations_workflow,
            &expected_logs,
            workflow_limits,
            should_cancel,
        )
        .await?;
    } else {
        for _ in 0..test_runs {
            assert_workflows(
                no_optimizations_workflow.clone(),
                optimizations_workflow.clone(),
                &expected_logs,
                workflow_limits.clone(),
                should_cancel,
            )
            .await?;
        }
    }

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
    run_only_once: Option<bool>,
    workflow_limits: Option<WorkflowLimits>,
) -> Result<()> {
    check_multi_connectors(
        query,
        btreemap! {"test" => input},
        views.unwrap_or_default(),
        expect,
        cancel.unwrap_or(false),
        apply_filter_tx,
        run_only_once.unwrap_or(false),
        workflow_limits.unwrap_or_default(),
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

async fn check_with_limits(
    query: &str,
    input: &str,
    expected: &str,
    limits: WorkflowLimits,
) -> Result<()> {
    check_multi_collection()
        .query(query)
        .input(btreemap! {"c" => input})
        .expect(expected)
        .workflow_limits(limits)
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

struct PartialStreamResult {
    partials: Vec<Log>,
    finals: Vec<Log>,
}

#[bon::builder]
async fn check_partial_stream(
    query: &str,
    input: BTreeMap<&str, &str>,
    expect_final: &str,
) -> Result<PartialStreamResult> {
    let mut connector = TestConnector::new(None, false);
    for (collection, raw_logs) in input {
        let logs: Vec<Log> = serde_json::from_str(raw_logs)?;
        connector.insert(collection.to_string(), logs);
    }

    let connector = Arc::new(connector);
    let stats = connector.fetch_stats().await;
    let connector_state = Arc::new(ConnectorState::new_with_static_stats(connector, stats));
    let connectors = btreemap! { "test".to_string() => connector_state };

    let steps = to_workflow_steps(&connectors, &BTreeMap::new(), parse(query).unwrap()).unwrap();
    let optimizer = Optimizer::default();
    let optimized_steps = spawn_blocking(move || optimizer.optimize(steps)).await?;

    let workflow = Workflow::new_with_partial_stream(
        optimized_steps,
        Some(PartialStream {
            debounce: Duration::ZERO,
        }),
    );

    let mut stream = workflow.execute(WorkflowLimits::default(), CancellationToken::new())?;
    let mut all_logs = Vec::new();
    while let Some(log) = stream.try_next().await? {
        all_logs.push(log);
    }

    let mut partials: Vec<Log> = Vec::new();
    let mut done_count = 0usize;
    let mut finals: Vec<Log> = Vec::new();

    for log in all_logs {
        let metadata = log.get(MISO_METADATA_FIELD_NAME).and_then(|v| match v {
            Value::Object(obj) => Some(obj.clone()),
            _ => None,
        });

        if let Some(metadata) = metadata {
            if metadata.contains_key(PARTIAL_STREAM_DONE_FIELD_NAME) {
                done_count += 1;
            } else {
                let mut log = log.clone();
                log.remove(MISO_METADATA_FIELD_NAME);
                partials.push(log);
            }
        } else {
            finals.push(log);
        }
    }

    assert!(!partials.is_empty(), "expected at least one partial");
    assert!(done_count >= 1, "expected at least one done marker");

    let mut expected: Vec<Value> = serde_json::from_str(expect_final)?;
    let mut actual: Vec<Value> = finals.iter().cloned().map(Value::Object).collect();
    expected.sort();
    actual.sort();
    assert_eq!(actual, expected, "final logs mismatch");

    Ok(PartialStreamResult { partials, finals })
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
async fn filter_between() -> Result<()> {
    check(
        r#"test.c | where value between (10 .. 20)"#,
        r#"[{"value": 5}, {"value": 9}, {"value": 10}, {"value": 15}, {"value": 20}, {"value": 21}, {"value": 25}]"#,
        r#"[{"value": 10}, {"value": 15}, {"value": 20}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_not_between() -> Result<()> {
    check(
        r#"test.c | where value !between (10 .. 20)"#,
        r#"[{"value": 5}, {"value": 9}, {"value": 10}, {"value": 15}, {"value": 20}, {"value": 21}, {"value": 25}]"#,
        r#"[{"value": 5}, {"value": 9}, {"value": 21}, {"value": 25}]"#,
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
        r#"[{"Count": 2}]"#,
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
                    avg_x=avg(x),
                    dcount_z=dcount(z),
                    cnt=count(),
                    cif=countif(z == 2)
          by y
        "#,
        r#"[{"x": 3, "y": 3, "z": 2}, {"x": 5, "y": 6, "z": 1}, {"x": 1, "y": 3, "z": 2}, {"x": 9, "y": 6, "z": 3}]"#,
        r#"[
            {"max_x": 3, "min_x": 1, "sum_x": 4.0, "avg_x": 2.0, "dcount_z": 1, "cnt": 2, "cif": 2, "y": 3},
            {"max_x": 9, "min_x": 5, "sum_x": 14.0, "avg_x": 7.0, "dcount_z": 2, "cnt": 2, "cif": 0, "y": 6}
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
                    avg_x=avg(x),
                    dcount_z=dcount(z),
                    cnt=count(),
                    cif=countif(z == 2)
          by y
        "#,
        r#"[{"a": 3, "b": 3, "c": 2}, {"a": 5, "b": 6, "c": 1}, {"a": 1, "b": 3, "c": 2}, {"a": 9, "b": 6, "c": 3}]"#,
        r#"[
            {"max_x": 3, "min_x": 1, "sum_x": 4.0, "avg_x": 2.0, "dcount_z": 1, "cnt": 2, "cif": 2, "y": 3},
            {"max_x": 9, "min_x": 5, "sum_x": 14.0, "avg_x": 7.0, "dcount_z": 2, "cnt": 2, "cif": 0, "y": 6}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn summarize_without_by() -> Result<()> {
    check(
        r#"
        test.c
        | summarize max_x=max(x),
                    min_x=min(x),
                    sum_x=sum(x),
                    avg_x=avg(x),
                    dcount_z=dcount(z),
                    cnt=count(),
                    cif=countif(z == 2)
        "#,
        r#"[{"x": 3, "y": 3, "z": 2}, {"x": 5, "y": 6, "z": 1}, {"x": 1, "y": 3, "z": 2}, {"x": 9, "y": 6, "z": 3}]"#,
        r#"[
            {"max_x": 9, "min_x": 1, "sum_x": 18.0, "avg_x": 4.5, "dcount_z": 3, "cnt": 4, "cif": 2}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn project_summarize_without_by() -> Result<()> {
    check(
        r#"
        test.c
        | project x=a, y=b, z=c
        | summarize max_x=max(x),
                    min_x=min(x),
                    sum_x=sum(x),
                    avg_x=avg(x),
                    dcount_z=dcount(z),
                    cnt=count(),
                    cif=countif(z == 2)
        "#,
        r#"[{"a": 3, "b": 3, "c": 2}, {"a": 5, "b": 6, "c": 1}, {"a": 1, "b": 3, "c": 2}, {"a": 9, "b": 6, "c": 3}]"#,
        r#"[
            {"max_x": 9, "min_x": 1, "sum_x": 18.0, "avg_x": 4.5, "dcount_z": 3, "cnt": 4, "cif": 2}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn summarize_without_by_empty_data() -> Result<()> {
    check(
        r#"
        test.c
        | summarize max_x=max(x),
                    min_x=min(x),
                    sum_x=sum(x),
                    avg_x=avg(x),
                    cnt=count()
        "#,
        r#"[]"#,
        r#"[
            {"max_x": null, "min_x": null, "sum_x": 0.0, "avg_x": 0.0, "cnt": 0}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn summarize_without_by_single_row() -> Result<()> {
    check(
        r#"
        test.c
        | summarize max_x=max(x),
                    min_x=min(x),
                    sum_x=sum(x),
                    avg_x=avg(x),
                    cnt=count()
        "#,
        r#"[{"x": 42, "y": 1}]"#,
        r#"[
            {"max_x": 42, "min_x": 42, "sum_x": 42.0, "avg_x": 42.0, "cnt": 1}
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
            "right" => r#"[{"id": 1, "value": "ONE"}, {"id": 2, "value": "DUP"}, {"id": 2, "value": "TWO"}, {"id": 4, "value": "FOUR"}, {"id": 5, "value": "FIVE"}]"#,
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
        .run_only_once(true)
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
        r#"[{"Count": 4}]"#,
    )
    .await
}

#[tokio::test]
async fn count_on_count() -> Result<()> {
    check(
        r#"test.c | count | count"#,
        r#"[{"world": 3}, {"test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"Count": 1}]"#,
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
        .query(r#"test.x | union (test.y)| union (test.y)| union (test.y)| union (test.y)| union (test.y)| union (test.y)| union (test.y)| union (test.y)| union (test.y)| union (test.y) | count"#)
        .input(btreemap! {
            "x" => r#"[{"x": 0}, {"x": 1}, {"x": 2}]"#,
            "y" => r#"[{"x": 3}, {"x": 4}, {"x": 5}, {"x": 6}]"#,
        })
        .expect(r#"[{"Count": 43}]"#)
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

#[tokio::test]
async fn expand_object() -> Result<()> {
    check(
        r#"test.c | mv-expand metadata"#,
        r#"[{"id": 1, "metadata": {"env": "prod", "region": "us-east"}}]"#,
        r#"[
            {"id": 1, "metadata": {"env": "prod"}},
            {"id": 1, "metadata": {"region": "us-east"}}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_multiple_records() -> Result<()> {
    check(
        r#"test.c | mv-expand tags"#,
        r#"[
            {"name": "item1", "tags": {"color": "red", "size": "large"}},
            {"name": "item2", "tags": {"priority": "high"}}
        ]"#,
        r#"[
            {"name": "item1", "tags": {"color": "red"}},
            {"name": "item1", "tags": {"size": "large"}},
            {"name": "item2", "tags": {"priority": "high"}}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_with_nested_values() -> Result<()> {
    check(
        r#"test.c | mv-expand config"#,
        r#"[{"id": 1, "config": {"timeout": 30, "options": {"retry": true}}}]"#,
        r#"[
            {"id": 1, "config": {"timeout": 30}},
            {"id": 1, "config": {"options": {"retry": true}}}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_empty_object() -> Result<()> {
    check(
        r#"test.c | mv-expand data"#,
        r#"[{"id": 1, "data": {}}]"#,
        r#"[]"#,
    )
    .await
}

#[tokio::test]
async fn expand_mixed_array_and_object() -> Result<()> {
    check(
        r#"test.c | mv-expand items, metadata"#,
        r#"[{"id": 1, "items": ["a", "b"], "metadata": {"env": "prod", "version": "2.0"}}]"#,
        r#"[
            {"id": 1, "items": "a", "metadata": {"env": "prod"}},
            {"id": 1, "items": "b", "metadata": {"version": "2.0"}}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_zip_unequal_lengths() -> Result<()> {
    check(
        r#"test.c | mv-expand tags, flags"#,
        r#"[{"id": 1, "tags": {"a": 1, "b": 2, "c": 3}, "flags": {"x": true}}]"#,
        r#"[
            {"id": 1, "tags": {"a": 1}, "flags": {"x": true}},
            {"id": 1, "tags": {"b": 2}, "flags": null},
            {"id": 1, "tags": {"c": 3}, "flags": null}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_array_kind() -> Result<()> {
    check(
        r#"test.c | mv-expand kind=array metadata"#,
        r#"[{"id": 1, "metadata": {"env": "prod", "region": "us-east"}}]"#,
        r#"[
            {"id": 1, "metadata": "env"},
            {"id": 1, "metadata": "prod"},
            {"id": 1, "metadata": "region"},
            {"id": 1, "metadata": "us-east"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_array_kind_with_numbers() -> Result<()> {
    check(
        r#"test.c | mv-expand kind=array config"#,
        r#"[{"id": 1, "config": {"timeout": 30, "retries": 3}}]"#,
        r#"[
            {"id": 1, "config": "timeout"},
            {"id": 1, "config": 30},
            {"id": 1, "config": "retries"},
            {"id": 1, "config": 3}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_array_kind_with_nested_values() -> Result<()> {
    check(
        r#"test.c | mv-expand kind=array data"#,
        r#"[{"id": 1, "data": {"name": "test", "options": {"nested": true}}}]"#,
        r#"[
            {"id": 1, "data": "name"},
            {"id": 1, "data": "test"},
            {"id": 1, "data": "options"},
            {"id": 1, "data": {"nested": true}}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_array_kind_empty() -> Result<()> {
    check(
        r#"test.c | mv-expand kind=array tags"#,
        r#"[{"id": 1, "tags": {}}]"#,
        r#"[]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_array_kind_multiple_records() -> Result<()> {
    check(
        r#"test.c | mv-expand kind=array props"#,
        r#"[
            {"name": "item1", "props": {"a": 1, "b": 2}},
            {"name": "item2", "props": {"x": "hello"}}
        ]"#,
        r#"[
            {"name": "item1", "props": "a"},
            {"name": "item1", "props": 1},
            {"name": "item1", "props": "b"},
            {"name": "item1", "props": 2},
            {"name": "item2", "props": "x"},
            {"name": "item2", "props": "hello"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_array_kind_zip_with_regular_array() -> Result<()> {
    check(
        r#"test.c | mv-expand kind=array metadata, items"#,
        r#"[{"id": 1, "metadata": {"a": 1, "b": 2}, "items": ["x", "y", "z"]}]"#,
        r#"[
            {"id": 1, "metadata": "a", "items": "x"},
            {"id": 1, "metadata": 1, "items": "y"},
            {"id": 1, "metadata": "b", "items": "z"},
            {"id": 1, "metadata": 2, "items": null}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn expand_object_array_kind_single_entry() -> Result<()> {
    check(
        r#"test.c | mv-expand kind=array data"#,
        r#"[{"id": 1, "data": {"only": "one"}}]"#,
        r#"[
            {"id": 1, "data": "only"},
            {"id": 1, "data": "one"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn let_simple_variable() -> Result<()> {
    check(
        r#"
            let x = test.c | where id > 5;
            x
        "#,
        r#"[
            {"id": 1, "name": "alice"},
            {"id": 6, "name": "bob"},
            {"id": 10, "name": "charlie"}
        ]"#,
        r#"[
            {"id": 6, "name": "bob"},
            {"id": 10, "name": "charlie"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn let_chained_variables() -> Result<()> {
    check(
        r#"
        let filtered = test.c | where age > 20;
        let sorted = filtered | sort by age;
        sorted
        "#,
        r#"[
            {"name": "alice", "age": 25},
            {"name": "bob", "age": 35},
            {"name": "charlie", "age": 15}
        ]"#,
        r#"[
            {"name": "alice", "age": 25},
            {"name": "bob", "age": 35}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn let_variable_in_union() -> Result<()> {
    check(
        r#"
            let set1 = test.c | where id < 3;
            test.c | union (set1)
        "#,
        r#"[
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
            {"id": 3, "value": "c"}
        ]"#,
        r#"[
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
            {"id": 3, "value": "c"},
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn case_simple_conditions() -> Result<()> {
    check(
        r#"
            test.c
            | project result = case(x > 10, "big", x > 5, "medium", "small")
        "#,
        r#"[{"x": 3}, {"x": 6}, {"x": 12}]"#,
        r#"[
            {"result": "small"},
            {"result": "medium"},
            {"result": "big"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn case_with_null_and_boolean() -> Result<()> {
    check(
        r#"
            test.c
            | project result = case(flag == true, "yes", flag == false, "no", "unknown")
        "#,
        r#"[{"flag": true}, {"flag": false}, {"flag": null}]"#,
        r#"[
            {"result": "yes"},
            {"result": "no"},
            {"result": "unknown"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn case_nested() -> Result<()> {
    check(
        r#"
            test.c
            | project label = case(
                score >= 90, "A",
                score >= 80, "B",
                case(score >= 70, "C", "F")
            )
        "#,
        r#"[{"score": 95}, {"score": 83}, {"score": 76}, {"score": 60}]"#,
        r#"[
            {"label": "A"},
            {"label": "B"},
            {"label": "C"},
            {"label": "F"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn case_chained_with_other_ops() -> Result<()> {
    check(
        r#"
            test.c
            | project category = case(x < 0, "negative", x == 0, "zero", "positive")
            | summarize c=count() by category
        "#,
        r#"[{"x": -1}, {"x": -2}, {"x": 0}, {"x": 1}, {"x": 2}]"#,
        r#"[
            {"category": "negative", "c": 2},
            {"category": "zero", "c": 1},
            {"category": "positive", "c": 2}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn divide_zero_by_value() -> Result<()> {
    check(
        r#"test.c | where result == 0 / 5"#,
        r#"[{"result": 0.0}, {"result": 1.0}]"#,
        r#"[{"result": 0.0}]"#,
    )
    .await
}

#[tokio::test]
async fn negative_number_operations() -> Result<()> {
    check(
        r#"test.c | where result == -5 * 2"#,
        r#"[{"result": -10.0}, {"result": 10.0}]"#,
        r#"[{"result": -10.0}]"#,
    )
    .await
}

#[tokio::test]
async fn mixed_int_uint_arithmetic() -> Result<()> {
    check(
        r#"test.c | where result == toint(5) + toint(-3)"#,
        r#"[{"result": 2}, {"result": 3}]"#,
        r#"[{"result": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn null_in_gte_operator() -> Result<()> {
    check(
        r#"test.c | where x >= 5"#,
        r#"[{"x": 10}, {"x": 5}, {"x": null}, {"x": 3}]"#,
        r#"[{"x": 10}, {"x": 5}]"#,
    )
    .await
}

#[tokio::test]
async fn null_in_lte_operator() -> Result<()> {
    check(
        r#"test.c | where x <= 5"#,
        r#"[{"x": 10}, {"x": 5}, {"x": null}, {"x": 3}]"#,
        r#"[{"x": null}, {"x": 5}, {"x": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn null_in_and_operator() -> Result<()> {
    check(
        r#"test.c | where x == 5 and y == 10"#,
        r#"[{"x": 5, "y": 10}, {"x": 5, "y": null}, {"x": null, "y": 10}]"#,
        r#"[{"x": 5, "y": 10}]"#,
    )
    .await
}

#[tokio::test]
async fn null_in_or_operator() -> Result<()> {
    check(
        r#"test.c | where x == 5 or y == 10"#,
        r#"[{"x": 5, "y": 0}, {"x": null, "y": 10}, {"x": null, "y": null}, {"x": 1, "y": 2}]"#,
        r#"[{"x": 5, "y": 0}, {"x": null, "y": 10}]"#,
    )
    .await
}

#[tokio::test]
async fn comparison_cross_type() -> Result<()> {
    check(
        r#"test.c | where x == "5""#,
        r#"[{"x": 5}, {"x": "5"}, {"x": 5.0}, {"x": true}]"#,
        r#"[{"x": "5"}]"#,
    )
    .await
}

#[tokio::test]
async fn project_nonexistent_field_arithmetic() -> Result<()> {
    check(
        r#"test.c | project result=missing + 5"#,
        r#"[{"id": 1}, {"id": 2}]"#,
        r#"[{}, {}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_null_field_comparison() -> Result<()> {
    check(
        r#"test.c | where null > 5"#,
        r#"[{"id": 1}, {"id": 2}]"#,
        r#"[]"#,
    )
    .await
}

#[tokio::test]
async fn exists_missing_nested_path() -> Result<()> {
    check(
        r#"test.c | where exists(a.b.c)"#,
        r#"[{"a": {"b": {"c": 1}}}, {"a": {"x": 1}}, {"a": null}, {}]"#,
        r#"[{"a": {"b": {"c": 1}}}]"#,
    )
    .await
}

#[tokio::test]
async fn in_operator_with_null_in_list() -> Result<()> {
    check(
        r#"test.c | where x in (1, null, 3)"#,
        r#"[{"x": 1}, {"x": 2}, {"x": null}, {"x": 3}]"#,
        r#"[{"x": null}, {"x": 1}, {"x": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn between_null_endpoints() -> Result<()> {
    check(
        r#"test.c | where x between (null .. 10)"#,
        r#"[{"x": 5}, {"x": 15}]"#,
        r#"[{"x": 5}]"#,
    )
    .await
}

#[tokio::test]
async fn contains_empty_string() -> Result<()> {
    check(
        r#"test.c | where text contains """#,
        r#"[{"text": "hello"}, {"text": ""}, {"text": null}]"#,
        r#"[{"text": "hello"}, {"text": ""}]"#,
    )
    .await
}

#[tokio::test]
async fn startswith_empty_string() -> Result<()> {
    check(
        r#"test.c | where text startswith """#,
        r#"[{"text": "hello"}, {"text": ""}, {"text": null}]"#,
        r#"[{"text": "hello"}, {"text": ""}]"#,
    )
    .await
}

#[tokio::test]
async fn endswith_empty_string() -> Result<()> {
    check(
        r#"test.c | where text endswith """#,
        r#"[{"text": "hello"}, {"text": ""}, {"text": null}]"#,
        r#"[{"text": "hello"}, {"text": ""}]"#,
    )
    .await
}

#[tokio::test]
async fn summarize_countif_all_false() -> Result<()> {
    check(
        r#"
        test.c
        | summarize cif=countif(x > 100) by y
        "#,
        r#"[{"x": 1, "y": "a"}, {"x": 2, "y": "a"}, {"x": 3, "y": "b"}]"#,
        r#"[
            {"cif": 0, "y": "a"},
            {"cif": 0, "y": "b"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn summarize_min_max_all_nulls() -> Result<()> {
    check(
        r#"
        test.c
        | summarize min_x=min(x), max_x=max(x) by y
        "#,
        r#"[{"x": null, "y": "a"}, {"x": null, "y": "a"}, {"x": null, "y": "b"}]"#,
        r#"[
            {"min_x": null, "max_x": null, "y": "a"},
            {"min_x": null, "max_x": null, "y": "b"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn summarize_avg_single_value() -> Result<()> {
    check(
        r#"
        test.c
        | summarize avg_x=avg(x) by y
        "#,
        r#"[{"x": 10, "y": "a"}, {"x": 20, "y": "a"}]"#,
        r#"[
            {"avg_x": 15.0, "y": "a"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn dcount_with_nulls() -> Result<()> {
    check(
        r#"
        test.c
        | summarize dcount_x=dcount(x)
        "#,
        r#"[{"x": 1}, {"x": 1}, {"x": null}, {"x": null}, {"x": 2}]"#,
        r#"[{"dcount_x": 3}]"#,
    )
    .await
}

#[tokio::test]
#[test_case(1)]
#[test_case(10)]
async fn join_with_null_keys(partitions: usize) -> Result<()> {
    check_multi_collection()
        .query(&format!(
            r#"test.left | join hint.partitions={partitions} (test.right) on id"#
        ))
        .input(btreemap! {
            "left"  => r#"[{"id": 1, "value": "a"}, {"id": null, "value": "b"}]"#,
            "right" => r#"[{"id": 1, "value": "A"}, {"id": null, "value": "B"}]"#,
        })
        .expect(
            r#"[
                {"id": null, "value_left": "b", "value_right": "B"},
                {"id": 1, "value_left": "a", "value_right": "A"}
            ]"#,
        )
        .call()
        .await
}

#[tokio::test]
async fn join_empty_right_side() -> Result<()> {
    check_multi_collection()
        .query(r#"test.left | join (test.right) on id"#)
        .input(btreemap! {
            "left"  => r#"[{"id": 1, "value": "a"}]"#,
            "right" => r#"[]"#,
        })
        .expect(r#"[]"#)
        .call()
        .await
}

#[tokio::test]
async fn join_no_key_matches() -> Result<()> {
    check_multi_collection()
        .query(r#"test.left | join (test.right) on id"#)
        .input(btreemap! {
            "left"  => r#"[{"id": 1, "value": "a"}]"#,
            "right" => r#"[{"id": 99, "value": "b"}]"#,
        })
        .expect(r#"[]"#)
        .call()
        .await
}

#[tokio::test]
async fn join_outer_with_nulls() -> Result<()> {
    check_multi_collection()
        .query(r#"test.left | join kind=outer (test.right) on id"#)
        .input(btreemap! {
            "left"  => r#"[{"id": 1, "value": "a"}, {"id": null, "value": "b"}]"#,
            "right" => r#"[{"id": 1, "value": "A"}, {"id": null, "value": "B"}]"#,
        })
        .expect(
            r#"[
                {"id": null, "value_left": "b", "value_right": "B"},
                {"id": 1, "value_left": "a", "value_right": "A"}
            ]"#,
        )
        .call()
        .await
}

#[tokio::test]
async fn expand_null_value() -> Result<()> {
    check(
        r#"test.c | mv-expand items"#,
        r#"[{"id": 1, "items": null}, {"id": 2, "items": ["a", "b"]}]"#,
        r#"[{"id": 1, "items": null}, {"id": 2, "items": "a"}, {"id": 2, "items": "b"}]"#,
    )
    .await
}

#[tokio::test]
async fn expand_array_with_nulls() -> Result<()> {
    check(
        r#"test.c | mv-expand items"#,
        r#"[{"id": 1, "items": [1, null, 3]}]"#,
        r#"[{"id": 1, "items": 1}, {"id": 1, "items": null}, {"id": 1, "items": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn limit_zero() -> Result<()> {
    check(
        r#"test.c | take 0"#,
        r#"[{"id": 1}, {"id": 2}, {"id": 3}]"#,
        r#"[]"#,
    )
    .await
}

#[tokio::test]
async fn topn_with_ties_at_boundary() -> Result<()> {
    check(
        r#"test.c | top 2 by value desc"#,
        r#"[{"id": 1, "value": 10}, {"id": 2, "value": 10}, {"id": 3, "value": 5}]"#,
        r#"[{"id": 1, "value": 10}, {"id": 2, "value": 10}]"#,
    )
    .await
}

#[tokio::test]
async fn sort_by_all_null_field() -> Result<()> {
    check(
        r#"test.c | sort by x"#,
        r#"[{"id": 1, "x": null}, {"id": 2, "x": null}, {"id": 3, "x": null}]"#,
        r#"[{"id": 1, "x": null}, {"id": 2, "x": null}, {"id": 3, "x": null}]"#,
    )
    .await
}

#[tokio::test]
async fn case_no_conditions_match() -> Result<()> {
    check(
        r#"
            test.c
            | project result = case(x > 100, "big", x > 50, "medium", "small")
        "#,
        r#"[{"x": 25}, {"x": 75}, {"x": 150}]"#,
        r#"[
            {"result": "small"},
            {"result": "medium"},
            {"result": "big"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn filter_with_nested_null_logic() -> Result<()> {
    check(
        r#"test.c | where (x == 5 or y == null) and z > 0"#,
        r#"[
            {"x": 5, "y": 10, "z": 1},
            {"x": 1, "y": null, "z": 1},
            {"x": 1, "y": null, "z": -1},
            {"x": 1, "y": 10, "z": 1}
        ]"#,
        r#"[
            {"x": 5, "y": 10, "z": 1},
            {"x": 1, "y": null, "z": 1}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn short_circuit_where_false() -> Result<()> {
    check(
        r#"test.c | where false"#,
        r#"[{"x": 1}, {"x": 2}, {"x": 3}]"#,
        r#"[]"#,
    )
    .await
}

#[tokio::test]
async fn short_circuit_union_where_false() -> Result<()> {
    check_multi_collection()
        .query(r#"test.x | union (test.y | where false)"#)
        .input(btreemap! {
            "x" => r#"[{"id": 1}, {"id": 2}]"#,
            "y" => r#"[{"id": 3}, {"id": 4}]"#,
        })
        .expect(r#"[{"id": 1}, {"id": 2}]"#)
        .call()
        .await
}

#[tokio::test]
#[test_case("inner")]
#[test_case("right")]
async fn short_circuit_join_returns_empty(kind: &str) -> Result<()> {
    check_multi_collection()
        .query(&format!(
            r#"test.left | join kind={kind} (test.right | where false) on id"#
        ))
        .input(btreemap! {
            "left"  => r#"[{"id": 1, "value": "a"}]"#,
            "right" => r#"[{"id": 1, "value": "b"}]"#,
        })
        .expect(r#"[]"#)
        .call()
        .await
}

#[tokio::test]
#[test_case("left")]
#[test_case("outer")]
async fn short_circuit_join_returns_left_side(kind: &str) -> Result<()> {
    check_multi_collection()
        .query(&format!(
            r#"test.left | join kind={kind} (test.right | where false) on id"#
        ))
        .input(btreemap! {
            "left"  => r#"[{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]"#,
            "right" => r#"[{"id": 1, "value": "A"}]"#,
        })
        .expect(r#"[{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]"#)
        .call()
        .await
}

#[tokio::test]
async fn summarize_group_by_with_null_key() -> Result<()> {
    check(
        r#"
        test.c
        | summarize cnt=count() by y
        "#,
        r#"[{"x": 1, "y": "a"}, {"x": 2, "y": null}, {"x": 3, "y": "a"}, {"x": 4, "y": null}]"#,
        r#"[
            {"cnt": 2, "y": "a"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn summarize_group_by_missing_field() -> Result<()> {
    check(
        r#"
        test.c
        | summarize cnt=count() by y
        "#,
        r#"[{"x": 1, "y": "a"}, {"x": 2}, {"x": 3, "y": "a"}]"#,
        r#"[
            {"cnt": 2, "y": "a"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn summarize_group_by_all_nulls() -> Result<()> {
    check(
        r#"
        test.c
        | summarize cnt=count() by y
        "#,
        r#"[{"x": 1, "y": null}, {"x": 2, "y": null}]"#,
        r#"[]"#,
    )
    .await
}

#[tokio::test]
async fn partial_stream_count() -> Result<()> {
    let r = check_partial_stream()
        .query("test.a | union (test.b) | count")
        .input(btreemap! { "a" => r#"[{"x": 1}]"#, "b" => r#"[{"x": 2}]"# })
        .expect_final(r#"[{"Count": 2}]"#)
        .call()
        .await?;
    let final_count = r.finals[0].get("Count").unwrap().as_i64().unwrap();
    for p in &r.partials {
        assert!(p.get("Count").unwrap().as_i64().unwrap() <= final_count);
    }
    Ok(())
}

#[tokio::test]
async fn partial_stream_summarize() -> Result<()> {
    let r = check_partial_stream()
        .query("test.a | union (test.b) | summarize s=sum(x)")
        .input(btreemap! { "a" => r#"[{"x": 1}, {"x": 2}]"#, "b" => r#"[{"x": 3}]"# })
        .expect_final(r#"[{"s": 6}]"#)
        .call()
        .await?;
    let final_sum = r.finals[0].get("s").unwrap().as_i64().unwrap();
    for p in &r.partials {
        assert!(p.get("s").unwrap().as_i64().unwrap() <= final_sum);
    }
    Ok(())
}

#[tokio::test]
async fn partial_stream_summarize_group_by() -> Result<()> {
    let r = check_partial_stream()
        .query("test.a | union (test.b) | summarize cnt=count() by y")
        .input(btreemap! {
            "a" => r#"[{"x": 1, "y": "foo"}, {"x": 2, "y": "bar"}]"#,
            "b" => r#"[{"x": 3, "y": "foo"}]"#
        })
        .expect_final(r#"[{"y": "bar", "cnt": 1}, {"y": "foo", "cnt": 2}]"#)
        .call()
        .await?;
    for p in &r.partials {
        assert!(p.contains_key("y") && p.contains_key("cnt"));
    }
    Ok(())
}

#[tokio::test]
async fn partial_stream_summarize_full() -> Result<()> {
    let r = check_partial_stream()
        .query(
            r#"
            test.a
            | union (test.b)
            | summarize max_x=max(x),
                        min_x=min(x),
                        sum_x=sum(x),
                        avg_x=avg(x),
                        dcount_z=dcount(z),
                        cnt=count(),
                        cif=countif(z == 2)
              by y
            "#,
        )
        .input(btreemap! {
            "a" => r#"[{"x": 3, "y": 3, "z": 2}, {"x": 5, "y": 6, "z": 1}]"#,
            "b" => r#"[{"x": 1, "y": 3, "z": 2}, {"x": 9, "y": 6, "z": 3}]"#
        })
        .expect_final(
            r#"[
                {"max_x": 3, "min_x": 1, "sum_x": 4.0, "avg_x": 2.0, "dcount_z": 1, "cnt": 2, "cif": 2, "y": 3},
                {"max_x": 9, "min_x": 5, "sum_x": 14.0, "avg_x": 7.0, "dcount_z": 2, "cnt": 2, "cif": 0, "y": 6}
            ]"#,
        )
        .call()
        .await?;
    for p in &r.partials {
        let y = p.get("y").unwrap().as_i64().unwrap();
        assert!(y == 3 || y == 6);
        assert!(p.get("cnt").unwrap().as_i64().unwrap() <= 2);
    }
    Ok(())
}

#[tokio::test]
async fn partial_stream_topn() -> Result<()> {
    let r = check_partial_stream()
        .query("test.a | union (test.b) | top 2 by x desc")
        .input(btreemap! {
            "a" => r#"[{"x": 5}, {"x": 3}, {"x": 1}]"#,
            "b" => r#"[{"x": 10}, {"x": 2}]"#
        })
        .expect_final(r#"[{"x": 10}, {"x": 5}]"#)
        .call()
        .await?;
    assert!(r.partials.len() <= 2);
    for p in &r.partials {
        assert!(p.contains_key("x"));
    }
    Ok(())
}

#[tokio::test]
async fn partial_stream_filter() -> Result<()> {
    let r = check_partial_stream()
        .query("test.a | union (test.b) | count | where Count > 0")
        .input(btreemap! { "a" => r#"[{"x": 1}]"#, "b" => r#"[{"x": 2}]"# })
        .expect_final(r#"[{"Count": 2}]"#)
        .call()
        .await?;
    for p in &r.partials {
        assert!(p.get("Count").unwrap().as_i64().unwrap() > 0);
    }
    Ok(())
}

#[tokio::test]
async fn partial_stream_project() -> Result<()> {
    let r = check_partial_stream()
        .query("test.a | union (test.b) | count | project c=Count")
        .input(btreemap! { "a" => r#"[{"x": 1}]"#, "b" => r#"[{"x": 2}]"# })
        .expect_final(r#"[{"c": 2}]"#)
        .call()
        .await?;
    let final_c = r.finals[0].get("c").unwrap().as_i64().unwrap();
    for p in &r.partials {
        assert!(!p.contains_key("Count"));
        assert!(p.get("c").unwrap().as_i64().unwrap() <= final_c);
    }
    Ok(())
}

#[tokio::test]
async fn partial_stream_extend() -> Result<()> {
    let r = check_partial_stream()
        .query("test.a | union (test.b) | summarize s=sum(x) | extend d=s*2")
        .input(btreemap! { "a" => r#"[{"x": 1}]"#, "b" => r#"[{"x": 2}]"# })
        .expect_final(r#"[{"s": 3, "d": 6}]"#)
        .call()
        .await?;
    let final_d = r.finals[0].get("d").unwrap().as_i64().unwrap();
    for p in &r.partials {
        assert!(p.get("d").unwrap().as_i64().unwrap() <= final_d);
    }
    Ok(())
}

#[tokio::test]
async fn partial_stream_limit() -> Result<()> {
    let r = check_partial_stream()
        .query("test.a | union (test.b) | top 3 by x desc | take 2")
        .input(btreemap! {
            "a" => r#"[{"x": 5}, {"x": 3}, {"x": 1}]"#,
            "b" => r#"[{"x": 10}]"#
        })
        .expect_final(r#"[{"x": 10}, {"x": 5}]"#)
        .call()
        .await?;
    assert!(r.partials.len() <= 2);
    Ok(())
}

#[tokio::test]
async fn partial_stream_multi_union() -> Result<()> {
    let r = check_partial_stream()
        .query("test.a | union (test.b) | union (test.c) | count")
        .input(btreemap! {
            "a" => r#"[{"x": 1}]"#,
            "b" => r#"[{"x": 2}]"#,
            "c" => r#"[{"x": 3}]"#
        })
        .expect_final(r#"[{"Count": 3}]"#)
        .call()
        .await?;
    let final_count = r.finals[0].get("Count").unwrap().as_i64().unwrap();
    for p in &r.partials {
        assert!(p.get("Count").unwrap().as_i64().unwrap() <= final_count);
    }
    Ok(())
}

fn find_sort_error(err: &color_eyre::Report) -> Option<&SortError> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<SortError>())
}

#[tokio::test]
async fn sort_memory_limit_exceeded() {
    let limits = WorkflowLimits {
        sort_memory_limit: bytesize::ByteSize::b(100),
    };
    let input = r#"[{"x": 1, "data": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, {"x": 2, "data": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]"#;
    let err = check_with_limits(r#"test.c | sort by x"#, input, "[]", limits)
        .await
        .unwrap_err();

    let sort_err = find_sort_error(&err).expect("expected SortError in error chain");
    assert!(matches!(sort_err, SortError::MemoryLimitExceeded { .. }));
}

#[tokio::test]
async fn sort_memory_limit_under_limit_succeeds() -> Result<()> {
    let limits = WorkflowLimits {
        sort_memory_limit: bytesize::ByteSize::mb(1),
    };
    check_with_limits(
        r#"test.c | sort by x"#,
        r#"[{"x": 2}, {"x": 1}]"#,
        r#"[{"x": 1}, {"x": 2}]"#,
        limits,
    )
    .await
}

#[tokio::test]
async fn sort_memory_limit_empty_input() -> Result<()> {
    let limits = WorkflowLimits {
        sort_memory_limit: bytesize::ByteSize::b(1),
    };
    check_with_limits(r#"test.c | sort by x"#, r#"[]"#, "[]", limits).await
}

#[tokio::test]
async fn sort_memory_limit_filter_reduces_usage() -> Result<()> {
    let limits = WorkflowLimits {
        sort_memory_limit: bytesize::ByteSize::b(300),
    };
    let input = r#"[{"x": 1, "data": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, {"x": 2, "data": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}, {"x": 3, "data": "cccccccccccccccccccccccccccccccccccc"}]"#;

    let err = check_with_limits(r#"test.c | sort by x"#, input, "[]", limits.clone())
        .await
        .unwrap_err();
    assert!(find_sort_error(&err).is_some());

    check_with_limits(
        r#"test.c | where x == 1 | sort by x"#,
        input,
        r#"[{"x": 1, "data": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]"#,
        limits,
    )
    .await
}
