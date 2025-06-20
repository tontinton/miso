use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::watch;

use crate::connectors::{
    stats::{ConnectorStats, FieldStats},
    Connector, ConnectorState, QueryHandle, Split,
};

use super::filter::FilterAst;

#[derive(Clone, Debug)]
pub struct Scan {
    pub connector_name: String,
    pub collection: String,

    pub connector: Arc<dyn Connector>,
    pub handle: Arc<dyn QueryHandle>,
    pub split: Option<Arc<dyn Split>>,
    pub stats: Arc<Mutex<ConnectorStats>>,

    pub dynamic_filter_tx: Option<watch::Sender<Option<FilterAst>>>,
    pub dynamic_filter_rx: Option<watch::Receiver<Option<FilterAst>>>,
}

impl PartialEq for Scan {
    fn eq(&self, other: &Self) -> bool {
        // Only checking the name for now.
        self.connector_name == other.connector_name && self.collection == other.collection
    }
}

impl Scan {
    pub async fn from_connector_state(
        connector_state: Arc<ConnectorState>,
        connector_name: String,
        collection: String,
    ) -> Self {
        let connector = connector_state.connector.clone();
        let handle = connector.get_handle().into();
        let stats = connector_state.stats.clone();
        Self {
            connector_name,
            collection,
            connector,
            handle,
            split: None,
            stats,
            dynamic_filter_tx: None,
            dynamic_filter_rx: None,
        }
    }

    pub fn get_field_stats(&self, field: &str) -> Option<FieldStats> {
        self.stats
            .lock()
            .get(&self.collection)
            .and_then(|x| x.get(field))
            .cloned()
    }
}
