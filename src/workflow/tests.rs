use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_stream::try_stream;
use axum::async_trait;
use collection_macros::btreemap;
use color_eyre::{
    eyre::{Context, ContextCompat},
    Result,
};
use ctor::ctor;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::{
    connector::{Connector, QueryHandle, QueryResponse, Split},
    http_server::to_workflow_steps,
    log::Log,
    optimizations::Optimizer,
    workflow::{sortable_value::SortableValue, Workflow},
};

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct TestHandle {}

#[derive(Debug, Default)]
struct TestConnector {
    collections: BTreeMap<String, Vec<Log>>,
}

impl TestConnector {
    fn insert(&mut self, collection: String, logs: Vec<Log>) {
        self.collections.insert(collection, logs);
    }
}

#[async_trait]
impl Connector for TestConnector {
    fn does_collection_exist(&self, collection: &str) -> bool {
        self.collections.contains_key(collection)
    }

    fn get_handle(&self) -> Box<dyn QueryHandle> {
        Box::new(TestHandle {})
    }

    async fn get_splits(&self) -> Vec<Arc<dyn Split>> {
        vec![Arc::new(TestSplit {}) as Arc<dyn Split>]
    }

    async fn query(
        &self,
        collection: &str,
        _split: &dyn Split,
        _handle: &dyn QueryHandle,
    ) -> Result<QueryResponse> {
        let logs = self
            .collections
            .get(collection)
            .context("collection to exist")?
            .clone();
        Ok(QueryResponse::Logs(Box::pin(try_stream! {
            for log in logs {
                yield log;
            }
        })))
    }

    async fn close(self) {}
}

#[ctor]
fn init() {
    color_eyre::install().unwrap();
    tracing_subscriber::fmt::init();
}

#[tokio::test]
async fn cancel() -> Result<()> {
    let workflow = Workflow::new(vec![]);

    let (cancel_tx, cancel_rx) = watch::channel(());
    cancel_tx.send(())?;

    let mut logs_stream = workflow.execute(cancel_rx).context("workflow execute")?;
    assert!(matches!(logs_stream.try_next().await, Ok(None)));

    Ok(())
}

async fn check_multi_connectors(
    query: &str,
    input: BTreeMap<&str, BTreeMap<&str, &str>>,
    expected: &str,
) -> Result<()> {
    let expected_logs: Vec<Log> =
        serde_json::from_str(expected).context("parse expected output logs from json")?;

    let mut connectors = BTreeMap::new();
    for (connector_name, collections) in input {
        for (collection, raw_logs) in collections {
            let input_logs: Vec<Log> =
                serde_json::from_str(raw_logs).context("parse input logs from json")?;
            connectors
                .entry(connector_name.to_string())
                .or_insert_with(TestConnector::default)
                .insert(collection.to_string(), input_logs);
        }
    }

    let steps = to_workflow_steps(
        &connectors
            .into_iter()
            .map(|(name, connector)| (name, Arc::new(connector) as Arc<dyn Connector>))
            .collect(),
        serde_json::from_str(query).context("parse query steps from json")?,
    )
    .await
    .expect("workflow steps to compile");

    let optimizer = Optimizer::default();

    let optimizations_workflow = Workflow::new(optimizer.optimize(steps.clone()).await);
    let no_optimizations_workflow = Workflow::new(steps);

    let (_cancel_tx, cancel_rx) = watch::channel(());

    let mut logs_stream = no_optimizations_workflow
        .execute(cancel_rx)
        .context("non optimized workflow execute")?;

    let mut logs = Vec::new();

    let mut expected_logs_iter = expected_logs.into_iter();
    while let Some(log) = logs_stream.try_next().await? {
        let test_log = expected_logs_iter
            .next()
            .expect("expected there to be more logs to test");
        assert_eq!(
            test_log,
            log,
            "logs[{}] not equal (left is expected, right is what we received)",
            logs.len()
        );

        logs.push(SortableValue(serde_json::Value::Object(log)));
    }

    let left: Vec<_> = expected_logs_iter.collect();
    assert!(
        left.is_empty(),
        "expected no more logs to test, collected: {}, left: {:?}",
        logs.len(),
        left
    );

    let (_cancel_tx, cancel_rx) = watch::channel(());
    let mut logs_stream = optimizations_workflow
        .execute(cancel_rx)
        .context("optimized workflow execute")?;

    let mut optimized_logs = Vec::with_capacity(logs.len());
    while let Some(log) = logs_stream.try_next().await? {
        optimized_logs.push(SortableValue(serde_json::Value::Object(log)));
    }

    logs.sort();
    optimized_logs.sort();

    assert_eq!(
        logs, optimized_logs,
        "results of workflow should equal results of optimized workflow query, after sorting"
    );

    Ok(())
}

async fn check_multi_collection(
    query: &str,
    input: BTreeMap<&str, &str>,
    expected: &str,
) -> Result<()> {
    check_multi_connectors(query, btreemap! {"test" => input}, expected).await
}

/// Creates a test connector named 'test' and a collection named 'c' which will include logs
/// given by |input|. Tests that the logs returned by running |query| are equal to |expected|.
async fn check(query: &str, input: &str, expected: &str) -> Result<()> {
    check_multi_collection(query, btreemap! {"c" => input}, expected).await
}

#[tokio::test]
async fn scan() -> Result<()> {
    let logs = r#"[{"hello": "world"}]"#;
    check(r#"[{"scan": ["test", "c"]}]"#, logs, logs).await
}

#[tokio::test]
async fn filter_eq() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"eq": [{"id": "world"}, {"lit": 2}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}]"#,
        r#"[{"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_float() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"eq": [{"id": "world"}, {"lit": 2.5}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1.5}, {"world": 2.5}]"#,
        r#"[{"world": 2.5}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_string() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"eq": [{"id": "world"}, {"lit": "200"}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": "200"}]"#,
        r#"[{"world": "200"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_bool() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"eq": [{"id": "world"}, {"lit": false}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 5}, {"world": true}, {"world": false}]"#,
        r#"[{"world": false}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_null() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"eq": [{"id": "world"}, {"lit": null}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": null}]"#,
        r#"[{"world": null}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_fields() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"eq": [{"id": "world"}, {"id": "world2"}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1, "world2": 1}, {"world": "33", "world2": 33}]"#,
        r#"[{"world": 1, "world2": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_eq_not_fields() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"eq": [{"id": "w"}, {"not": {"id": "w2"}}]}}
        ]"#,
        r#"[{"hello": "world"}, {"w": true, "w2": 0}, {"w": false, "w2": "a"}, {"w": true, "w2": 22.6}]"#,
        r#"[{"w": true, "w2": 0}, {"w": false, "w2": "a"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_not_eq_fields() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"not": {"eq": [{"id": "w"}, {"id": "w2"}]}}}
        ]"#,
        r#"[{"hello": "world"}, {"w": 100, "w2": 0}, {"w": "abc", "w2": "a"}, {"w": 100.3, "w2": 100.3}]"#,
        r#"[{"hello": "world"}, {"w": 100, "w2": 0}, {"w": "abc", "w2": "a"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_ne() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"ne": [{"id": "world"}, {"lit": 2}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}]"#,
        r#"[{"world": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_gt() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"gt": [{"id": "world"}, {"lit": 1}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 1}]"#,
        r#"[{"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_lt() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"lt": [{"id": "world"}, {"lit": 3}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 3}]"#,
        r#"[{"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_gte() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"gte": [{"id": "world"}, {"lit": 2}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}, {"world": 3}]"#,
        r#"[{"world": 2}, {"world": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_lte() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"lte": [{"id": "world"}, {"lit": 3}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 3}, {"world": 4}]"#,
        r#"[{"world": 2}, {"world": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_and() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"and": [
                {"eq": [{"id": "world"}, {"lit": 3}]},
                {"eq": [{"id": "hello"}, {"lit": "world"}]}
            ]}}
        ]"#,
        r#"[{"hello": "world", "world": 3}, {"hello": "woold", "world": 3}, {"hello": "world", "world": 2}]"#,
        r#"[{"hello": "world", "world": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_or() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"or": [
                {"eq": [{"id": "world"}, {"lit": 3}]},
                {"eq": [{"id": "hello"}, {"lit": "world"}]}
            ]}}
        ]"#,
        r#"[{"hello": "world", "world": 3}, {"hello": "woold", "world": 3}, {"hello": "world", "world": 2}, {"hello": "woold", "world": 4}]"#,
        r#"[{"hello": "world", "world": 3}, {"hello": "woold", "world": 3}, {"hello": "world", "world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_contains() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"contains": [{"id": "hello"}, {"lit": "wor"}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "aaawora"}, {"hello": "woold"}]"#,
        r#"[{"hello": "world"}, {"hello": "aaawora"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_starts_with() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"starts_with": [{"id": "hello"}, {"lit": "wor"}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "aaawora"}, {"hello": "woold"}]"#,
        r#"[{"hello": "world"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_starts_with_on_object() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"starts_with": [{"id": "hello.there"}, {"lit": "wor"}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": {"there": "woold"}}, {"hello": {"there": "world"}}]"#,
        r#"[{"hello": {"there": "world"}}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_ends_with() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"ends_with": [{"id": "hello"}, {"lit": "ora"}]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "aaawora"}, {"hello": "woold"}]"#,
        r#"[{"hello": "aaawora"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_exists() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"exists": "hello"}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "woold"}]"#,
        r#"[{"hello": "world"}, {"hello": "woold"}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_exists_on_object() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"exists": "hello.there"}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": {"there": "abc"}}, {"hello": {"world": "def"}}]"#,
        r#"[{"hello": {"there": "abc"}}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_exists_null() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"exists": "hello"}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": null}]"#,
        r#"[{"hello": "world"}, {"hello": null}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_not_exists() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"not": {"exists": "hello"}}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "woold"}]"#,
        r#"[{"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn project_add() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {
                "project": [
                    {"to": "world", "from": {"id": "world"}},
                    {
                        "to": "test",
                        "from": {
                            "+": [
                                {"cast": ["float", {"id": "world"}]},
                                {"lit": 2}
                            ]
                        }
                    }
                ]
            }
        ]"#,
        r#"[{"world": 2}, {"world": 1}, {"hello": "world"}]"#,
        r#"[{"world": 2, "test": 4.0}, {"world": 1, "test": 3.0}, {}]"#,
    )
    .await
}

#[tokio::test]
async fn sort_asc_then_desc() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"sort": [{"by": "world"}, {"by": "test", "order": "desc"}]}
        ]"#,
        r#"[{"world": 3, "test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"world": 2, "test": 6}, {"world": 2, "test": 3}, {"world": 3, "test": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn sort_nulls_order() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"sort": [{"by": "world"}, {"by": "test", "nulls": "first"}]}
        ]"#,
        r#"[{"world": 4, "test": 1}, {}, {"world": 3, "test": 1}, {"world": null, "test": 1}, {"world": 4, "test": null}]"#,
        r#"[{"world": 3, "test": 1}, {"world": 4, "test": null}, {"world": 4, "test": 1}, {}, {"world": null, "test": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn limit() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"limit": 2}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}, {"world": 3}]"#,
        r#"[{"hello": "world"}, {"world": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn topn() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"top": [[{"by": "world"}, {"by": "test", "order": "desc"}], 2]}
        ]"#,
        r#"[{"world": 3, "test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"world": 2, "test": 6}, {"world": 2, "test": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn sort_limit() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"sort": [{"by": "world"}, {"by": "test", "order": "desc"}]},
            {"limit": 2}
        ]"#,
        r#"[{"world": 3, "test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"world": 2, "test": 6}, {"world": 2, "test": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn sort_limit_count() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"sort": [{"by": "world"}, {"by": "test", "order": "desc"}]},
            {"limit": 2},
            "count"
        ]"#,
        r#"[{"world": 3, "test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"count": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn summarize() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {
                "summarize": {
                    "aggs": {
                        "max_x": {"max": "x"},
                        "min_x": {"min": "x"},
                        "sum_x": {"sum": "x"},
                        "c": "count"
                    },
                    "by": ["y"]
                }
            }
        ]"#,
        r#"[{"x": 3, "y": 3}, {"x": 5, "y": 6}, {"x": 1, "y": 3}, {"x": 9, "y": 6}]"#,
        r#"[{"max_x": 3, "min_x": 1, "sum_x": 4.0, "c": 2, "y": 3}, {"max_x": 9, "min_x": 5, "sum_x": 14.0, "c": 2, "y": 6}]"#,
    )
    .await
}

#[tokio::test]
async fn join_inner() -> Result<()> {
    check_multi_collection(
        r#"[
            {"scan": ["test", "left"]},
            {
                "join": [
                    {"on": ["id", "id"]},
                    [{"scan": ["test", "right"]}]
                ]
            }
        ]"#,
        btreemap!{
            "left"  => r#"[{"id": 1, "value": "one"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]"#,
            "right" => r#"[{"id": 1, "value": "ONE"}, {"id": 2, "value": "TWO"}, {"id": 4, "value": "FOUR"}]"#,
        },
        r#"[
            {"id": 1, "value_left": "one", "value_right": "ONE"},
            {"id": 2, "value_left": "two", "value_right": "TWO"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn join_outer() -> Result<()> {
    check_multi_collection(
        r#"[
            {"scan": ["test", "left"]},
            {
                "join": [
                    {"on": ["id", "id"], "type": "outer"},
                    [{"scan": ["test", "right"]}]
                ]
            }
        ]"#,
        btreemap!{
            "left"  => r#"[{"id": 1, "value": "one"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]"#,
            "right" => r#"[{"id": 1, "value": "ONE"}, {"id": 2, "value": "TWO"}, {"id": 4, "value": "FOUR"}]"#,
        },
        r#"[
            {"id": 1, "value_left": "one", "value_right": "ONE"},
            {"id": 2, "value_left": "two", "value_right": "TWO"},
            {"id": 3, "value": "three"},
            {"id": 4, "value": "FOUR"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn join_left() -> Result<()> {
    check_multi_collection(
        r#"[
            {"scan": ["test", "left"]},
            {
                "join": [
                    {"on": ["id", "id"], "type": "left"},
                    [{"scan": ["test", "right"]}]
                ]
            }
        ]"#,
        btreemap!{
            "left"  => r#"[{"id": 1, "value": "one"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]"#,
            "right" => r#"[{"id": 1, "value": "ONE"}, {"id": 2, "value": "TWO"}, {"id": 4, "value": "FOUR"}]"#,
        },
        r#"[
            {"id": 1, "value": "one"},
            {"id": 2, "value": "two"},
            {"id": 3, "value": "three"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn join_right() -> Result<()> {
    check_multi_collection(
        r#"[
            {"scan": ["test", "left"]},
            {
                "join": [
                    {"on": ["id", "id"], "type": "right"},
                    [{"scan": ["test", "right"]}]
                ]
            }
        ]"#,
        btreemap!{
            "left"  => r#"[{"id": 1, "value": "one"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]"#,
            "right" => r#"[{"id": 1, "value": "ONE"}, {"id": 2, "value": "TWO"}, {"id": 4, "value": "FOUR"}]"#,
        },
        r#"[
            {"id": 1, "value": "ONE"},
            {"id": 2, "value": "TWO"},
            {"id": 4, "value": "FOUR"}
        ]"#,
    )
    .await
}

#[tokio::test]
async fn count() -> Result<()> {
    check(
        r#"[{"scan": ["test", "c"]}, "count"]"#,
        r#"[{"world": 3}, {"test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"count": 4}]"#,
    )
    .await
}

#[tokio::test]
async fn count_on_count() -> Result<()> {
    check(
        r#"[{"scan": ["test", "c"]}, "count", "count"]"#,
        r#"[{"world": 3}, {"test": 1}, {"world": 2, "test": 3}, {"world": 2, "test": 6}]"#,
        r#"[{"count": 1}]"#,
    )
    .await
}
