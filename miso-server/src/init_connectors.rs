use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use color_eyre::eyre::{Context, Result, bail};
use miso_common::humantime_utils::deserialize_duration;
use miso_connectors::{Connector, ConnectorState};
use serde::Deserialize;

use crate::VIEWS_CONNECTOR_NAME;
use crate::http_server::{ConnectorsMap, DEFAULT_STATS_FETCH_INTERVAL};

fn default_stats_fetch_interval() -> Duration {
    DEFAULT_STATS_FETCH_INTERVAL
}

#[derive(Deserialize)]
struct ConnectorConfig {
    #[serde(
        default = "default_stats_fetch_interval",
        deserialize_with = "deserialize_duration"
    )]
    stats_fetch_interval: Duration,

    #[serde(flatten)]
    connector: Box<dyn Connector>,
}

#[derive(Deserialize)]
struct InitConnectorsConfig {
    connectors: BTreeMap<String, ConnectorConfig>,
}

pub fn load_connectors_from_file<P: AsRef<Path>>(path: P) -> Result<ConnectorsMap> {
    let path = path.as_ref();

    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read connectors config: {}", path.display()))?;

    let config: InitConnectorsConfig = serde_json::from_str(&content)
        .with_context(|| format!("failed to parse connectors config: {}", path.display()))?;

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

    Ok(connectors)
}
