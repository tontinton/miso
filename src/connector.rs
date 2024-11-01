use std::fmt::Debug;
use std::pin::Pin;

use axum::async_trait;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::{ast::FilterAst, quickwit_connector::QuickwitSplit};

#[derive(Debug, Serialize, Deserialize)]
pub enum Split {
    Quickwit(QuickwitSplit),
}

pub type Log = String;

#[async_trait]
pub trait Connector: Debug + Send + Sync {
    fn get_splits(&self) -> Vec<Split>;
    fn query(&self, split: &Split) -> Pin<Box<dyn Stream<Item = Log> + Send>>;

    /// Returns whether the connector is able to predicate pushdown the entire filter AST.
    fn can_filter(&self, _filter: &FilterAst) -> bool {
        false
    }

    async fn close(self);
}
