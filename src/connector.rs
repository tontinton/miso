use std::pin::Pin;

use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::{ast::FilterItem, elasticsearch::ElasticsearchSplit};

#[derive(Debug, Serialize, Deserialize)]
pub enum Split {
    Elasticsearch(ElasticsearchSplit),
}

pub type Log = String;

pub trait Connector: Send + Sync {
    fn get_splits(&self) -> Vec<Split>;
    fn query(&self, split: &Split) -> Pin<Box<dyn Stream<Item = Log> + Send>>;
    fn apply_filter_and(&self, item: &FilterItem) -> bool;
    fn apply_filter_or(&self, items: &[FilterItem]) -> bool;
}
