use std::{any::Any, collections::BTreeMap, sync::Arc};

use async_stream::try_stream;
use axum::async_trait;
use color_eyre::{
    eyre::{Context, ContextCompat},
    Result,
};
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

#[tokio::test]
async fn simple_scan() -> Result<()> {
    let logs = vec![btreemap! {"hello" => "world"}];

    let mut connector = TestConnector::default();
    connector.insert("test".to_string(), logs.clone());
    let connectors = btreemap! {
        "c".to_string() => Arc::new(connector) as Arc<dyn Connector>,
    };

    let steps = to_workflow_steps(
        &connectors,
        serde_json::from_str(r#"[{"scan": ["c", "test"]}]"#).expect("parse query steps from json"),
    )
    .await
    .expect("workflow steps to compile");
    let workflow = Workflow::new(steps);

    let (_, cancel_rx) = watch::channel(());

    let mut logs_stream = workflow.execute(cancel_rx).context("workflow execute")?;

    let mut logs_iter = logs.into_iter();
    while let Some(log) = logs_stream.try_next().await? {
        let test_log = logs_iter.next().expect("to there be more logs to test");
        assert_eq!(test_log, log);
    }
    assert!(logs_iter.next().is_none(), "expected no more logs to test");

    Ok(())
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
