use std::pin::Pin;
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

pub type Log = BTreeMap<String, serde_json::Value>;

#[async_trait]
pub trait Connector: Debug + Send + Sync {
    async fn does_collection_exist(&self, collection: &str) -> bool;
    fn get_splits(&self) -> Vec<Split>;
    fn query(
        &self,
        collection: &str,
        split: &Split,
    ) -> Pin<Box<dyn Stream<Item = Result<Log>> + Send>>;

    /// Returns whether the connector is able to predicate pushdown the entire filter AST.
    fn can_filter(&self, _filter: &FilterAst) -> bool {
        false
    }

    async fn close(self);
}
