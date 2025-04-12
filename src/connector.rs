use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use axum::async_trait;
use color_eyre::eyre::Result;

use crate::log::LogTryStream;
use crate::workflow::filter::FilterAst;
use crate::workflow::sort::Sort;
use crate::workflow::summarize::Summarize;
use crate::workflow::Workflow;

#[macro_export]
macro_rules! downcast_unwrap {
    ($obj:expr, $target_type:ty) => {{
        match $obj.as_any().downcast_ref::<$target_type>() {
            Some(obj) => obj,
            None => panic!("Failed to downcast to {}", stringify!($target_type)),
        }
    }};
}

pub enum QueryResponse {
    Logs(LogTryStream),
    Count(i64),
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
    async fn does_collection_exist(&self, collection: &str) -> bool;

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
        _union: &Workflow,
        _handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        None
    }

    async fn close(self);
}
