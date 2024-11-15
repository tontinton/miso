use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use axum::async_trait;
use color_eyre::eyre::Result;

use crate::log::LogTryStream;
use crate::workflow::filter::FilterAst;

#[macro_export]
macro_rules! downcast_unwrap {
    // Reference downcast.
    (ref $obj:expr, $target_type:ty) => {{
        match $obj.as_any().downcast_ref::<$target_type>() {
            Some(obj) => obj,
            None => panic!("Failed to downcast to {}", stringify!($target_type)),
        }
    }};
    // Owned downcast.
    (owned $obj:expr, $target_type:ty) => {{
        match $obj.into_any().downcast::<$target_type>() {
            Ok(obj) => obj,
            Err(_) => panic!("Failed to downcast to {}", stringify!($target_type)),
        }
    }};
    // Default to owned.
    ($handle:expr, $target_type:ty) => {
        downcast_unwrap!(owned $handle, $target_type)
    };
}

#[typetag::serde(tag = "type")]
pub trait Split: Any + Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

#[typetag::serde(tag = "type")]
pub trait QueryHandle: Any + Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

#[async_trait]
pub trait Connector: Debug + Send + Sync {
    async fn does_collection_exist(&self, collection: &str) -> bool;

    fn get_handle(&self) -> Box<dyn QueryHandle>;

    async fn get_splits(&self) -> Vec<Arc<dyn Split>>;

    fn query(
        &self,
        collection: &str,
        split: &dyn Split,
        handle: &dyn QueryHandle,
    ) -> Result<LogTryStream>;

    /// Returns the filter AST the connector should predicate pushdown.
    /// None means it can't predicate pushdown the filter AST provided.
    /// Called multiple times, which means that every time you predicate pushdown
    /// an expression you need to query them all with an AND, or the connector's equivalent.
    fn apply_filter(
        &self,
        _ast: &FilterAst,
        _handle: Box<dyn QueryHandle>,
    ) -> Option<Box<dyn QueryHandle>> {
        None
    }

    /// Returns the handle with limit to predicate pushdown.
    /// None means it can't predicate pushdown limit.
    fn apply_limit(
        &self,
        _max: u64,
        _handle: Box<dyn QueryHandle>,
    ) -> Option<Box<dyn QueryHandle>> {
        None
    }

    async fn close(self);
}
