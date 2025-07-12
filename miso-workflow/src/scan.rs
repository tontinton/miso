use std::{future::Future, sync::Arc, time::Duration};

use color_eyre::{Result, eyre::Context};
use flume::Receiver;
use futures_util::{StreamExt, stream::once};
use miso_common::watch::Watch;
use miso_connectors::{
    Connector, ConnectorState, QueryHandle, QueryResponse, Split,
    stats::{ConnectorStats, FieldStats},
};
use miso_workflow_types::{
    filter::FilterAst,
    log::{LogItem, LogTryStream},
};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use super::{AsyncTask, count::count_to_log};

const DYNAMIC_FILTER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct Scan {
    pub connector_name: String,
    pub collection: String,

    pub connector: Arc<dyn Connector>,
    pub handle: Arc<dyn QueryHandle>,
    pub split: Option<Arc<dyn Split>>,
    pub stats: Arc<Mutex<ConnectorStats>>,

    pub dynamic_filter_tx: Option<Watch<FilterAst>>,
    pub dynamic_filter_rx: Option<Watch<FilterAst>>,
}

impl PartialEq for Scan {
    fn eq(&self, other: &Self) -> bool {
        // Only checking the name for now.
        self.connector_name == other.connector_name && self.collection == other.collection
    }
}

impl Scan {
    pub async fn from_connector_state(
        connector_state: Arc<ConnectorState>,
        connector_name: String,
        collection: String,
    ) -> Self {
        let connector = connector_state.connector.clone();
        let handle = connector.get_handle().into();
        let stats = connector_state.stats.clone();
        Self {
            connector_name,
            collection,
            connector,
            handle,
            split: None,
            stats,
            dynamic_filter_tx: None,
            dynamic_filter_rx: None,
        }
    }

    pub fn get_field_stats(&self, field: &str) -> Option<FieldStats> {
        self.stats
            .lock()
            .get(&self.collection)
            .and_then(|x| x.get(field))
            .cloned()
    }
}

fn count_to_stream(count: u64) -> LogTryStream {
    Box::pin(once(async move { Ok(count_to_log(count)) }))
}

async fn apply_dynamic_filter(
    connector: &dyn Connector,
    handle: &dyn QueryHandle,
    dynamic_filter_rx: Watch<FilterAst>,
) -> Option<Arc<dyn QueryHandle>> {
    info!("Waiting for dynamic filter");

    let Some(ast) = dynamic_filter_rx.wait_for(DYNAMIC_FILTER_TIMEOUT).await else {
        info!("Timeout waiting on dynamic filter");
        return None;
    };

    info!("Got dynamic filter");
    let dynamic_filtered_handle = connector.apply_filter(&ast, handle)?;

    info!("Applied dynamic filter");
    Some(dynamic_filtered_handle.into())
}

async fn scan_stream(scan: Scan) -> Result<LogTryStream> {
    let Scan {
        collection,
        connector,
        mut handle,
        split,
        dynamic_filter_rx,
        ..
    } = scan;

    if let Some(filter_rx) = dynamic_filter_rx {
        if let Some(dynamic_filtered_handle) =
            apply_dynamic_filter(connector.as_ref(), handle.as_ref(), filter_rx).await
        {
            handle = dynamic_filtered_handle;
        }
    }

    let response = connector
        .query(&collection, handle.as_ref(), split.as_deref())
        .await?;
    let stream = match response {
        QueryResponse::Logs(logs) => Box::pin(logs.map(Into::into)),
        QueryResponse::Count(count) => count_to_stream(count),
    };

    Ok(stream)
}

pub async fn cancel_or<F, T>(cancel: &CancellationToken, fut: F) -> Option<T>
where
    F: Future<Output = T> + Send,
{
    tokio::select! {
        _ = cancel.cancelled() => {
            None
        }
        result = fut => {
            Some(result)
        }
    }
}

pub fn scan_rx(scan: Scan, cancel: CancellationToken) -> (Receiver<LogItem>, AsyncTask) {
    let (tx, rx) = flume::bounded(1);
    let task = tokio::spawn(async move {
        let mut stream = scan_stream(scan).await.context("create scan stream")?;
        while let Some(Some(item)) = cancel_or(&cancel, stream.next()).await {
            if let Err(e) = tx.send_async(item.into()).await {
                debug!("Closing scan task: {e:?}");
                break;
            }
        }
        Ok(())
    });
    (rx, task)
}
