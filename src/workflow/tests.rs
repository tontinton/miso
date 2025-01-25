use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_stream::try_stream;
use axum::async_trait;
use color_eyre::{
    eyre::{Context, ContextCompat},
    Result,
};
use ctor::ctor;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use vrl::btreemap;

use crate::{
    connector::{Connector, QueryHandle, QueryResponse, Split},
    http_server::to_workflow_steps,
    log::Log,
    workflow::Workflow,
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

#[derive(Debug)]
struct TestConnector {
    collections: BTreeMap<String, Vec<Log>>,
}

impl Default for TestConnector {
    fn default() -> Self {
        Self {
            collections: BTreeMap::new(),
        }
    }
}

impl TestConnector {
    fn insert(&mut self, collection: String, logs: Vec<Log>) {
        self.collections.insert(collection, logs);
    }
}

#[async_trait]
impl Connector for TestConnector {
    async fn does_collection_exist(&self, collection: &str) -> bool {
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
    let workflow = Workflow::new(steps);

    let (_cancel_tx, cancel_rx) = watch::channel(());

    let mut logs_stream = workflow.execute(cancel_rx).context("workflow execute")?;

    let mut i = 0;
    let mut expected_logs_iter = expected_logs.into_iter();
    while let Some(log) = logs_stream.try_next().await? {
        let test_log = expected_logs_iter
            .next()
            .expect("expected there to be more logs to test");
        assert_eq!(
            test_log, log,
            "logs[{i}] not equal (left is expected, right is what we received)"
        );

        i += 1;
    }

    assert!(
        expected_logs_iter.next().is_none(),
        "expected no more logs to test"
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
            {"filter": {"eq": ["world", "2"]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}]"#,
        r#"[{"hello": "world"}, {"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_ne() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"ne": ["world", "2"]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}]"#,
        r#"[{"hello": "world"}, {"world": 1}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_gt() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"gt": ["world", "1"]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 1}]"#,
        r#"[{"hello": "world"}, {"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_lt() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"lt": ["world", "3"]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 3}]"#,
        r#"[{"hello": "world"}, {"world": 2}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_gte() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"gte": ["world", "2"]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 1}, {"world": 2}, {"world": 3}]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_lte() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"lte": ["world", "3"]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 3}, {"world": 4}]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"world": 3}]"#,
    )
    .await
}

#[tokio::test]
async fn filter_and() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {"filter": {"and": [{"eq": ["world", "3"]}, {"eq": ["hello", "\"world\""]}]}}
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
            {"filter": {"or": [{"eq": ["world", "3"]}, {"eq": ["hello", "\"world\""]}]}}
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
            {"filter": {"contains": ["hello", "wor"]}}
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
            {"filter": {"starts_with": ["hello", "wor"]}}
        ]"#,
        r#"[{"hello": "world"}, {"world": 2}, {"hello": "aaawora"}, {"hello": "woold"}]"#,
        r#"[{"hello": "world"}]"#,
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
                    {"to": "world", "from": {"field": "world"}},
                    {
                        "to": "test",
                        "from": {
                            "+": [
                                {"cast": ["float", {"field": "world"}]},
                                {"value": "2"}
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
async fn summarize() -> Result<()> {
    check(
        r#"[
            {"scan": ["test", "c"]},
            {
                "summarize": {
                    "aggs": {
                        "max_x": {"max": "x"},
                        "min_x": {"min": "x"},
                        "c": "count"
                    },
                    "by": ["y"]
                }
            }
        ]"#,
        r#"[{"x": 3, "y": 3}, {"x": 5, "y": 6}, {"x": 1, "y": 3}, {"x": 9, "y": 6}]"#,
        r#"[{"max_x": 3, "min_x": 1, "c": 2, "y": 3}, {"max_x": 9, "min_x": 5, "c": 2, "y": 6}]"#,
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
