use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use axum::async_trait;
use color_eyre::eyre::Result;
use futures_core::Stream;
use vrl::value::ObjectMap;

use crate::filter::FilterAst;

#[typetag::serde(tag = "type")]
pub trait Split: Any + Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[typetag::serde(tag = "type")]
pub trait FilterPushdown: Any + Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub type Log = ObjectMap;

#[async_trait]
pub trait Connector: Debug + Send + Sync {
    async fn does_collection_exist(&self, collection: &str) -> bool;
    async fn get_splits(&self) -> Vec<Arc<dyn Split>>;
    fn query(
        &self,
        collection: &str,
        split: &dyn Split,
        pushdown: &Option<&dyn FilterPushdown>,
        limit: Option<u64>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Log>> + Send>>>;

    /// Returns the filter AST the connector should predicate pushdown.
    /// None means it can't predicate pushdown the filter AST provided.
    fn apply_filter(&self, _ast: &FilterAst) -> Option<Arc<dyn FilterPushdown>> {
        None
    }

    async fn close(self);
}
