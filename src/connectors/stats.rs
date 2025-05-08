use std::{collections::BTreeMap, sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::{spawn, sync::watch, task::JoinHandle};
use tracing::{error, instrument};

use crate::run_at_interval::run_at_interval;

use super::Connector;

pub type ConnectorStats = BTreeMap<String, CollectionStats>;
pub type SharedConnectorStats = Arc<Mutex<BTreeMap<String, CollectionStats>>>;
pub type CollectionStats = BTreeMap<String, FieldStats>;

#[derive(Debug, Default, Clone)]
pub struct FieldStats {
    pub distinct_count: Option<u32>,
}

async fn refetch_stats_at_interval(
    interval: Duration,
    connector: Arc<dyn Connector>,
    stats: SharedConnectorStats,
    shutdown_rx: watch::Receiver<()>,
) {
    run_at_interval(
        async || {
            if let Some(fetched) = connector.fetch_stats().await {
                let mut guard = stats.lock();
                *guard = fetched;
            };
        },
        interval,
        shutdown_rx,
        "Stats collector",
    )
    .await;
}

#[derive(Debug)]
pub struct IntervalStatsCollector {
    _task: JoinHandle<()>,
    _shutdown_tx: watch::Sender<()>,
}

impl IntervalStatsCollector {
    pub fn new(
        interval: Duration,
        connector: Arc<dyn Connector>,
        stats: Arc<Mutex<ConnectorStats>>,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = spawn(refetch_stats_at_interval(
            interval,
            connector,
            stats,
            shutdown_rx,
        ));

        Self {
            _task: task,
            _shutdown_tx: shutdown_tx,
        }
    }

    #[instrument(skip_all, name = "Interval stats collector - close")]
    async fn close(self) {
        if let Err(e) = self._shutdown_tx.send(()) {
            error!(
                "Failed to send shutdown to stats collector interval task: {}",
                e
            );
        }
        if let Err(e) = self._task.await {
            error!("Failed to join interval stats task: {}", e);
        }
    }
}
