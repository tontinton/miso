use std::{pin::Pin, sync::Arc, time::Duration};

use async_stream::stream;
use axum::async_trait;
use color_eyre::eyre::{bail, Context, Result};
use futures_core::Stream;
use reqwest::Client;
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
};

#[derive(Debug, Serialize, Deserialize)]
pub struct QuickwitSplit {
    query: String,
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

#[derive(Debug, Deserialize)]
struct IndexResponseConfig {
    index_id: String,
}

#[derive(Debug, Deserialize)]
struct IndexResponse {
    index_config: IndexResponseConfig,
}

#[instrument(name = "GET and parse quickwit indexes", skip_all)]
async fn get_indexes(client: &Client, url: &str) -> Result<Vec<String>> {
    let response = client
        .get(url)
        .send()
        .await
        .context("GET http request failed")?;
    if !response.status().is_success() {
        bail!("GET {} failed with status: {}", url, response.status());
    }
    let text = response.text().await.context("text from response")?;
    let data: Vec<IndexResponse> = serde_json::from_str(&text)?;
    Ok(data.into_iter().map(|x| x.index_config.index_id).collect())
}

async fn run_interval_task(
    config: QuickwitConfig,
    collections: SharedCollections,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let client = Client::new();
    let indexes_url = format!("{}/api/v1/indexes", config.url);

    loop {
        select! {
            _ = sleep(config.refresh_interval) => {
                match get_indexes(&client, &indexes_url).await {
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
            _ = shutdown_rx.changed() => {
                info!("Shutdown signal received. Stopping interval task.");
                break;
            }
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
    fn get_splits(&self) -> Vec<Split> {
        vec![Split::Quickwit(QuickwitSplit {
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
