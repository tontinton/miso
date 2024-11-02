use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::BTreeMap, fmt::Debug};

use axum::async_trait;
use color_eyre::eyre::Result;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::{ast::FilterAst, quickwit_connector::QuickwitSplit};

#[derive(Debug, Serialize, Deserialize)]
pub enum Split {
    Quickwit(QuickwitSplit),
}

#[typetag::serde(tag = "type")]
pub trait FilterPushdown: Any + Debug + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub type Log = BTreeMap<String, serde_json::Value>;

#[async_trait]
pub trait Connector: Debug + Send + Sync {
    async fn does_collection_exist(&self, collection: &str) -> bool;
    fn get_splits(&self) -> Vec<Split>;
    fn query(
        &self,
        collection: &str,
        split: &Split,
        pushdown: &Option<Arc<dyn FilterPushdown>>,
        limit: Option<u64>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Log>> + Send>>>;

    /// Returns the filter AST the connector should predicate pushdown.
    /// None means it can't predicate pushdown the filter AST provided.
    fn apply_filter(&self, _ast: &FilterAst) -> Option<Arc<dyn FilterPushdown>> {
        None
    }

    async fn close(self);
}
