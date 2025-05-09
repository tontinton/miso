use std::{collections::BTreeMap, sync::Weak, time::Duration};

use parking_lot::Mutex;
use tracing::instrument;

use crate::{run_at_interval::run_at_interval, shutdown_future::ShutdownFuture};

use super::Connector;

pub type ConnectorStats = BTreeMap<String, CollectionStats>;
pub type CollectionStats = BTreeMap<String, FieldStats>;

#[derive(Debug, Default, Clone)]
pub struct FieldStats {
    pub distinct_count: Option<u32>,
}

async fn refetch_stats_at_interval(
    interval: Duration,
    weak_connector: Weak<dyn Connector>,
    weak_stats: Weak<Mutex<ConnectorStats>>,
) {
    run_at_interval(
        async || {
            let Some(connector) = weak_connector.upgrade() else {
                return false;
            };
            let Some(stats) = weak_stats.upgrade() else {
                return false;
            };

            if let Some(fetched) = connector.fetch_stats().await {
                let mut guard = stats.lock();
                *guard = fetched;
            };

            true
        },
        interval,
    )
    .await;
}

#[derive(Debug)]
pub struct IntervalStatsCollector {
    collector_task: ShutdownFuture,
}

impl IntervalStatsCollector {
    pub fn new(
        interval: Duration,
        connector: Weak<dyn Connector>,
        stats: Weak<Mutex<ConnectorStats>>,
    ) -> Self {
        let collector_task = ShutdownFuture::new(
            refetch_stats_at_interval(interval, connector, stats),
            "Stats collector",
        );
        Self { collector_task }
    }

    #[instrument(skip_all, name = "Interval stats collector - close")]
    pub async fn close(&self) {
        self.collector_task.shutdown().await;
    }
}
