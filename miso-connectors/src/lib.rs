pub mod quickwit;
pub mod stats;

use std::fmt::{self, Debug};
use std::sync::Arc;
use std::time::Duration;
use std::{any::Any, collections::BTreeMap};

use axum::async_trait;
use color_eyre::eyre::Result;
use miso_workflow_types::{filter::FilterAst, log::LogTryStream, sort::Sort, summarize::Summarize};
use parking_lot::Mutex;
use thiserror::Error;
use tokio::time::sleep;

use stats::{ConnectorStats, IntervalStatsCollector};

const CLOSE_WHEN_LAST_OWNER_INTERVAL: Duration = Duration::from_millis(100);

#[macro_export]
macro_rules! downcast_unwrap {
    ($obj:expr, $target_type:ty) => {{
        match $obj.as_any().downcast_ref::<$target_type>() {
            Some(obj) => obj,
            None => panic!("Failed to downcast to {}", stringify!($target_type)),
        }
    }};
}

#[derive(Debug)]
pub struct ConnectorState {
    pub connector: Arc<dyn Connector>,
    pub stats: Arc<Mutex<ConnectorStats>>,
    stats_collector: Option<IntervalStatsCollector>,
}

impl ConnectorState {
    pub fn new(connector: Arc<dyn Connector>) -> Self {
        Self {
            connector,
            stats: Arc::new(Mutex::new(BTreeMap::new())),
            stats_collector: None,
        }
    }

    pub fn new_with_stats(connector: Arc<dyn Connector>, interval: Duration) -> Self {
        let stats = Arc::new(Mutex::new(BTreeMap::new()));
        let stats_collector = Some(IntervalStatsCollector::new(
            interval,
            Arc::downgrade(&connector),
            Arc::downgrade(&stats),
        ));

        Self {
            connector,
            stats,
            stats_collector,
        }
    }

    pub fn new_with_static_stats(
        connector: Arc<dyn Connector>,
        stats: Option<ConnectorStats>,
    ) -> Self {
        let stats = Arc::new(Mutex::new(stats.unwrap_or_default()));
        Self {
            connector,
            stats,
            stats_collector: None,
        }
    }

    /// Wait for the given Arc to be the last reference, only then close().
    /// This is useful when there are some running queries on the connector and you want to allow them
    /// to finish executing.
    pub async fn close_when_last_owner(self: Arc<Self>) {
        while Arc::strong_count(&self) > 1 {
            sleep(CLOSE_WHEN_LAST_OWNER_INTERVAL).await;
        }

        if let Some(stats_collector) = &self.stats_collector {
            stats_collector.close().await;
        }
        let connector = self.connector.clone();
        drop(self);

        while Arc::strong_count(&connector) > 1 {
            sleep(CLOSE_WHEN_LAST_OWNER_INTERVAL).await;
        }
        connector.close().await;
    }
}

pub enum QueryResponse {
    Logs(LogTryStream),
    Count(u64),
}

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("HTTP failed: {0}")]
    Http(#[source] reqwest::Error),
    #[error("Server responded with status code {0}: {1}")]
    ServerResp(u16, String),
}

#[typetag::serde(tag = "type")]
pub trait Split: Any + Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[typetag::serde(tag = "type")]
pub trait QueryHandle: Any + Debug + fmt::Display + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[async_trait]
#[typetag::serde(tag = "type")]
pub trait Connector: Debug + Send + Sync {
    fn does_collection_exist(&self, collection: &str) -> bool;

    fn get_handle(&self) -> Box<dyn QueryHandle>;

    /// Returns splits to run multiple shorter queries in parallel (which we union).
    /// Empty vec (default implementation) means no need to split the query.
    fn get_splits(&self) -> Vec<Box<dyn Split>> {
        vec![]
    }

    async fn query(
        &self,
        collection: &str,
        handle: &dyn QueryHandle,
        split: Option<&dyn Split>,
    ) -> Result<QueryResponse>;

    /// Returns the handle with the filter AST the connector should predicate pushdown.
    /// None means it can't predicate pushdown the filter AST provided.
    /// Called multiple times, which means that every time you predicate pushdown
    /// an expression you need to query them all with an AND, or the connector's equivalent.
    fn apply_filter(
        &self,
        _ast: &FilterAst,
        _handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        None
    }

    /// Returns the handle with limit to predicate pushdown.
    /// None means it can't predicate pushdown limit.
    fn apply_limit(&self, _max: u32, _handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        None
    }

    /// Returns the handle with sort order and limit to predicate pushdown.
    /// None means it can't predicate pushdown top-n.
    fn apply_topn(
        &self,
        _sorts: &[Sort],
        _max: u32,
        _handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        None
    }

    /// Returns the handle with count predicate pushdown.
    /// None means it can't predicate pushdown count.
    fn apply_count(&self, _handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        None
    }

    /// Returns the handle with summarize predicate pushdown.
    /// None means it can't predicate pushdown count.
    fn apply_summarize(
        &self,
        _config: &Summarize,
        _handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        None
    }

    /// Returns the handle with union predicate pushdown.
    /// None means it can't predicate pushdown union.
    fn apply_union(
        &self,
        _scan_collection: &str,
        _union_collection: &str,
        _handle: &dyn QueryHandle,
        _union_handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        None
    }

    /// Get statistics about collections and their fields.
    /// Miso uses these statistics to optimize queries.
    /// For example: by knowing how many distinct values there are on a field which a query wants to
    /// JOIN ON, we can see ahead of time whether we should execute that query and filter the
    /// results in the other part of the JOIN query. This is called "dynamic filtering".
    async fn fetch_stats(&self) -> Option<ConnectorStats> {
        None
    }

    async fn close(&self);
}
