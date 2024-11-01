use std::pin::Pin;

use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::{ast::FilterAst, elasticsearch::ElasticsearchSplit};

#[derive(Debug, Serialize, Deserialize)]
pub enum Split {
    Elasticsearch(ElasticsearchSplit),
}

pub type Log = String;

pub trait Connector: Send + Sync {
    fn get_splits(&self) -> Vec<Split>;
    fn query(&self, split: &Split) -> Pin<Box<dyn Stream<Item = Log> + Send>>;

    /// Returns whether the connector is able to predicate pushdown the entire filter AST.
    fn can_filter(&self, _filter: &FilterAst) -> bool {
        false
    }
}
