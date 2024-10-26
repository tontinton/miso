use std::pin::Pin;

use async_stream::stream;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::{
    ast::FilterItem,
    connector::{Connector, Log, Split},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticsearchSplit {
    pub query: String,
}

pub struct ElasticsearchConnector {}

impl Connector for ElasticsearchConnector {
    fn get_splits(&self) -> Vec<Split> {
        return vec![Split::Elasticsearch(ElasticsearchSplit {
            query: "".to_string(),
        })];
    }

    fn query(&self, _split: &Split) -> Pin<Box<dyn Stream<Item = Log> + Send>> {
        Box::pin(stream! {
            for i in 0..3 {
                yield format!(r#"{{ "test": "{}" }}"#, i);
            }
        })
    }

    fn apply_filter_and(&self, _item: &FilterItem) -> bool {
        false
    }

    fn apply_filter_or(&self, _items: &[FilterItem]) -> bool {
        false
    }
}
