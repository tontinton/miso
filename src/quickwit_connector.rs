use std::{pin::Pin, sync::Arc, time::Duration};

use async_stream::try_stream;
use axum::async_trait;
use color_eyre::eyre::Result;
use futures_core::Stream;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::{
    select, spawn,
    sync::{watch, RwLock},
    task::JoinHandle,
    time::sleep,
};
use tracing::{debug, error, info, instrument};

use crate::{
    ast::FilterAst,
    connector::{Connector, Log, Split},
    http_client::get_text,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct QuickwitSplit {
    query: String,
}

#[derive(Debug, Deserialize)]
struct IndexResponseConfig {
    index_id: String,
}

#[derive(Debug, Deserialize)]
struct IndexResponse {
    index_config: IndexResponseConfig,
}

#[derive(Debug, Deserialize)]
struct SearchResponseHit {
    #[serde(rename = "_source")]
    source: Log,
}

#[derive(Debug, Deserialize)]
struct SearchResponseHits {
    hits: Vec<SearchResponseHit>,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    hits: SearchResponseHits,
}

fn default_refresh_interval() -> Duration {
    humantime::parse_duration("1m").expect("Invalid duration format")
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuickwitConfig {
    url: String,

    #[serde(
        default = "default_refresh_interval",
        deserialize_with = "deserialize_duration"
    )]
    refresh_interval: Duration,
}

type SharedCollections = Arc<RwLock<Vec<String>>>;

#[derive(Debug)]
pub struct QuickwitConnector {
    config: QuickwitConfig,
    collections: SharedCollections,
    interval_task: JoinHandle<()>,
    shutdown_tx: watch::Sender<()>,
}

#[instrument(name = "GET and parse quickwit search results")]
async fn search(base_url: &str, index: &str) -> Result<Vec<Log>> {
    let url = format!("{}/api/v1/_elastic/{}/_search", base_url, index);
    let text = get_text(&url).await?;
    let data: SearchResponse = serde_json::from_str(&text)?;
    Ok(data.hits.hits.into_iter().map(|x| x.source).collect())
}

#[instrument(name = "GET and parse quickwit indexes")]
async fn get_indexes(base_url: &str) -> Result<Vec<String>> {
    let url = format!("{}/api/v1/indexes", base_url);
    let text = get_text(&url).await?;
    let data: Vec<IndexResponse> = serde_json::from_str(&text)?;
    Ok(data.into_iter().map(|x| x.index_config.index_id).collect())
}

async fn refresh_indexes(url: &str, collections: &SharedCollections) {
    match get_indexes(url).await {
        Ok(indexes) => {
            debug!("Got indexes: {:?}", &indexes);
            let mut guard = collections.write().await;
            *guard = indexes;
        }
        Err(e) => {
            error!("Failed to get quickwit indexes: {}", e);
        }
    }
}

async fn run_interval_task(
    config: QuickwitConfig,
    collections: SharedCollections,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let future = async {
        refresh_indexes(&config.url, &collections).await;
        loop {
            sleep(config.refresh_interval).await;
            refresh_indexes(&config.url, &collections).await;
        }
    };

    select! {
        _ = future => {
            panic!("Interval future done looping?");
        }
        _ = shutdown_rx.changed() => {
            info!("Shutdown signal received. Stopping task.");
        }
    }
}

impl QuickwitConnector {
    pub fn new(config: QuickwitConfig) -> QuickwitConnector {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let collections = Arc::new(RwLock::new(Vec::new()));
        let interval_task = spawn(run_interval_task(
            config.clone(),
            collections.clone(),
            shutdown_rx,
        ));

        Self {
            config,
            collections,
            interval_task,
            shutdown_tx,
        }
    }
}

#[async_trait]
impl Connector for QuickwitConnector {
    async fn does_collection_exist(&self, collection: &str) -> bool {
        self.collections
            .read()
            .await
            .iter()
            .any(|s| s == collection)
    }

    fn get_splits(&self) -> Vec<Split> {
        vec![Split::Quickwit(QuickwitSplit {
            query: "".to_string(),
        })]
    }

    fn query(
        &self,
        collection: &str,
        _split: &Split,
    ) -> Pin<Box<dyn Stream<Item = Result<Log>> + Send>> {
        let url = self.config.url.clone();
        let collection = collection.to_string();
        Box::pin(try_stream! {
            for log in search(&url, &collection).await? {
                yield log;
            }
        })
    }

    fn can_filter(&self, _filter: &FilterAst) -> bool {
        false
    }

    async fn close(self) {
        if let Err(e) = self.shutdown_tx.send(()) {
            error!("Failed to send shutdown to quickwit interval task: {}", e);
            return;
        }
        if let Err(e) = self.interval_task.await {
            error!("Failed to join quickwit interval task: {}", e);
        }
    }
}
