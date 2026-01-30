//! Reads logs from a data source via a connector. Supports dynamic filters from joins.

use std::{future::Future, sync::Arc, time::Duration};

use color_eyre::{Result, eyre::Context};
use flume::Receiver;
use futures_util::{StreamExt, stream::once};
use hashbrown::HashMap;
use miso_common::{
    metrics::{METRICS, STEP_SCAN},
    watch::Watch,
};
use miso_connectors::{
    Connector, ConnectorState, QueryHandle, QueryResponse, Split,
    stats::{ConnectorStats, FieldStats},
};
use miso_workflow_types::{
    expr::Expr,
    log::{Log, LogItem, LogItemTryStream},
};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, info, info_span};

use super::{AsyncTask, CHANNEL_CAPACITY, count::count_to_log};

const DYNAMIC_FILTER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug)]
pub struct Scan {
    pub connector_name: String,
    pub collection: String,
    pub static_fields: HashMap<String, String>,

    pub connector: Arc<dyn Connector>,
    pub handle: Arc<dyn QueryHandle>,
    pub split: Option<Arc<dyn Split>>,
    pub stats: Arc<Mutex<ConnectorStats>>,

    pub dynamic_filter_tx: Option<Watch<Expr>>,
    pub dynamic_filter_rx: Option<Watch<Expr>>,
}

impl PartialEq for Scan {
    fn eq(&self, other: &Self) -> bool {
        // Only checking the name for now.
        self.connector_name == other.connector_name && self.collection == other.collection
    }
}

impl Scan {
    pub fn new(
        connector_state: Arc<ConnectorState>,
        connector_name: String,
        collection: String,
        static_fields: HashMap<String, String>,
    ) -> Result<Self> {
        let connector = connector_state.connector.clone();
        let handle = connector
            .get_handle(&collection)
            .context("get connector handle")?
            .into();
        let stats = connector_state.stats.clone();
        Ok(Self {
            connector_name,
            collection,
            static_fields,
            connector,
            handle,
            split: None,
            stats,
            dynamic_filter_tx: None,
            dynamic_filter_rx: None,
        })
    }

    pub fn get_field_stats(&self, field: &str) -> Option<FieldStats> {
        self.stats
            .lock()
            .get(&self.collection)
            .and_then(|x| x.get(field))
            .cloned()
    }
}

fn count_to_stream(count: u64) -> LogItemTryStream {
    Box::pin(once(async move { Ok(LogItem::Log(count_to_log(count))) }))
}

#[inline]
fn apply_static_fields(mut log: Log, static_fields: &HashMap<String, String>) -> Log {
    for (to, from) in static_fields {
        if let Some(value) = log.remove(from) {
            log.insert(to.clone(), value);
        }
    }
    log
}

async fn apply_dynamic_filter(
    connector: &dyn Connector,
    handle: &dyn QueryHandle,
    dynamic_filter_rx: Watch<Expr>,
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

async fn scan_stream(scan: Scan) -> Result<LogItemTryStream> {
    let Scan {
        collection,
        connector,
        static_fields,
        mut handle,
        split,
        dynamic_filter_rx,
        ..
    } = scan;

    if let Some(filter_rx) = dynamic_filter_rx
        && let Some(dynamic_filtered_handle) =
            apply_dynamic_filter(connector.as_ref(), handle.as_ref(), filter_rx).await
    {
        handle = dynamic_filtered_handle;
    }

    let response = connector
        .query(&collection, handle.as_ref(), split.as_deref())
        .await?;
    let stream: LogItemTryStream =
        match response {
            QueryResponse::Logs(logs) if static_fields.is_empty() => {
                Box::pin(logs.map(|res| res.map(LogItem::Log)))
            }
            QueryResponse::Logs(logs) => Box::pin(logs.map(move |res| {
                res.map(|log| LogItem::Log(apply_static_fields(log, &static_fields)))
            })),
            QueryResponse::PartialLogs(items) if static_fields.is_empty() => items,
            QueryResponse::PartialLogs(items) => Box::pin(items.map(move |res| {
                res.map(|item| match item {
                    LogItem::Log(log) => LogItem::Log(apply_static_fields(log, &static_fields)),
                    LogItem::PartialStreamLog(log, id) => {
                        LogItem::PartialStreamLog(apply_static_fields(log, &static_fields), id)
                    }
                    other => other,
                })
            })),
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
    let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);
    let span = info_span!("scan", connector = %scan.connector_name, collection = %scan.collection);
    let task = tokio::spawn(
        async move {
            let mut rows_processed = 0u64;

            let mut stream = scan_stream(scan).await.context("create scan stream")?;
            while let Some(Some(item)) = cancel_or(&cancel, stream.next()).await {
                let log_item = match item {
                    Ok(log_item) => {
                        if matches!(log_item, LogItem::Log(_) | LogItem::PartialStreamLog(..)) {
                            rows_processed += 1;
                        }
                        log_item
                    }
                    Err(e) => LogItem::Err(e),
                };
                if let Err(e) = tx.send_async(log_item).await {
                    debug!("Closing scan task: {e:?}");
                    break;
                }
            }

            METRICS
                .workflow_step_rows
                .with_label_values(&[STEP_SCAN])
                .inc_by(rows_processed);

            Ok(())
        }
        .instrument(span),
    );
    (rx, task)
}
