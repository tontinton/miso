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

    fn query(&self, split: &Split) -> Box<dyn Stream<Item = Log>> {
        dbg!(split);
        Box::new(stream! {
            for i in 0..3 {
                yield i.to_string()
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
