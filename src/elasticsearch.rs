use std::pin::Pin;

use async_stream::stream;
use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::{
    ast::FilterAst,
    connector::{Connector, Log, Split},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticsearchSplit {
    pub query: String,
}

pub struct ElasticsearchConnector {}

impl Connector for ElasticsearchConnector {
    fn get_splits(&self) -> Vec<Split> {
        vec![Split::Elasticsearch(ElasticsearchSplit {
            query: "".to_string(),
        })]
    }

    fn query(&self, _split: &Split) -> Pin<Box<dyn Stream<Item = Log> + Send>> {
        Box::pin(stream! {
            for i in 0..3 {
                yield format!(r#"{{ "test": "{}" }}"#, i);
            }
        })
    }

    fn can_filter(&self, _filter: &FilterAst) -> bool {
        false
    }
}
