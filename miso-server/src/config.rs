use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::{Context, Result, bail};
use miso_common::humantime_utils::deserialize_duration;
use miso_connectors::{Connector, ConnectorState};
use serde::Deserialize;

use crate::VIEWS_CONNECTOR_NAME;

const DEFAULT_STATS_FETCH_INTERVAL: Duration = Duration::from_hours(3);

pub type ConnectorsMap = BTreeMap<String, Arc<ConnectorState>>;

fn default_stats_fetch_interval() -> Duration {
    DEFAULT_STATS_FETCH_INTERVAL
}

#[derive(Deserialize)]
pub struct ConnectorConfig {
    /// The interval to fetch statistics (e.g. distinct count of each field), and cache in memory.
    #[serde(
        default = "default_stats_fetch_interval",
        deserialize_with = "deserialize_duration"
    )]
    pub stats_fetch_interval: Duration,

    /// The connector config to set.
    #[serde(flatten)]
    pub connector: Box<dyn Connector>,
}

#[derive(Deserialize)]
struct Config {
    connectors: BTreeMap<String, ConnectorConfig>,
    query_status_collection: Option<String>,
}

/// Configuration for writing query status records.
#[derive(Debug, Clone)]
pub struct QueryStatusConfig {
    pub connector_name: String,
    pub collection_name: String,
}

impl QueryStatusConfig {
    /// Parse a "connector.collection" string into a QueriesStatusConfig.
    pub fn parse(value: &str) -> Result<Self> {
        let (connector_name, collection_name) = value.split_once('.').ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "query_status_collection must be in format 'connector.collection', got: {value}"
            )
        })?;

        if connector_name.is_empty() || collection_name.is_empty() {
            bail!(
                "query_status_collection must have non-empty connector and collection names, got: {value}"
            );
        }

        Ok(Self {
            connector_name: connector_name.to_string(),
            collection_name: collection_name.to_string(),
        })
    }
}

pub fn load_config<P: AsRef<Path>>(path: P) -> Result<(ConnectorsMap, Option<QueryStatusConfig>)> {
    let path = path.as_ref();

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config: {}", path.display()))?;

    let config: Config = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse config: {}", path.display()))?;

    let mut connectors = BTreeMap::new();

    for (name, cfg) in config.connectors {
        if name == VIEWS_CONNECTOR_NAME {
            bail!("connector name '{}' is reserved", VIEWS_CONNECTOR_NAME);
        }

        connectors.insert(
            name,
            Arc::new(ConnectorState::new_with_stats(
                cfg.connector.into(),
                cfg.stats_fetch_interval,
            )),
        );
    }

    let query_status_config = match config.query_status_collection {
        Some(value) => Some(QueryStatusConfig::parse(&value)?),
        None => None,
    };

    Ok((connectors, query_status_config))
}
