use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use std::{any::Any, collections::BTreeMap};

use axum::async_trait;
use color_eyre::eyre::Result;
use parking_lot::Mutex;

use crate::connector_stats::IntervalStatsCollector;
use crate::{
    connector_stats::{ConnectorStats, SharedConnectorStats},
    log::LogTryStream,
    workflow::{filter::FilterAst, sort::Sort, summarize::Summarize, Workflow},
};

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
    pub stats: SharedConnectorStats,

    // Need to close() this when the connector is deleted.
    _stats_collector: Option<IntervalStatsCollector>,
}

impl ConnectorState {
    pub fn new(connector: Arc<dyn Connector>) -> Self {
        Self {
            connector,
            stats: Arc::new(Mutex::new(BTreeMap::new())),
            _stats_collector: None,
        }
    }

    pub fn new_with_stats(connector: Arc<dyn Connector>, interval: Option<Duration>) -> Self {
        let stats = Arc::new(Mutex::new(BTreeMap::new()));
        let stats_collector =
            interval.map(|x| IntervalStatsCollector::new(x, connector.clone(), stats.clone()));
        Self {
            connector,
            stats,
            _stats_collector: stats_collector,
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
            _stats_collector: None,
        }
    }
}

pub enum QueryResponse {
    Logs(LogTryStream),
    Count(u64),
}

#[typetag::serde(tag = "type")]
pub trait Split: Any + Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[typetag::serde(tag = "type")]
pub trait QueryHandle: Any + Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[async_trait]
pub trait Connector: Debug + Send + Sync {
    fn does_collection_exist(&self, collection: &str) -> bool;

    fn get_handle(&self) -> Box<dyn QueryHandle>;

    async fn get_splits(&self) -> Vec<Arc<dyn Split>>;

    async fn query(
        &self,
        collection: &str,
        split: &dyn Split,
        handle: &dyn QueryHandle,
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
        _union: &Workflow,
        _handle: &dyn QueryHandle,
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

    async fn close(self);
}
