use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_stream::try_stream;
use axum::async_trait;
use color_eyre::eyre::Result;
use hashbrown::HashMap;
use miso_workflow_types::log::Log;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::{Collection, Connector, QueryHandle, QueryResponse, Sink};

/// A simple in-memory sink that stores logs in a Vec.
#[derive(Debug)]
pub struct MemorySink {
    logs: Arc<RwLock<HashMap<String, Vec<Log>>>>,
    collection: String,
}

impl MemorySink {
    pub fn new(logs: Arc<RwLock<HashMap<String, Vec<Log>>>>, collection: String) -> Self {
        {
            let mut guard = logs.write();
            guard.entry(collection.clone()).or_default();
        }
        Self { logs, collection }
    }
}

#[async_trait]
impl Sink for MemorySink {
    async fn write(&self, log: Log) {
        self.logs
            .write()
            .get_mut(&self.collection)
            .expect("inserted in new")
            .push(log);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MemoryHandle;

impl fmt::Display for MemoryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryHandle")
    }
}

#[typetag::serde]
impl QueryHandle for MemoryHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A memory connector that supports both sink (writing) and query (reading) operations.
/// Logs are stored in memory, organized by collection name.
#[derive(Debug, Serialize, Deserialize)]
pub struct MemoryConnector {
    #[serde(skip)]
    collections: Arc<RwLock<HashMap<String, Vec<Log>>>>,
}

impl MemoryConnector {
    pub fn new() -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_logs(&self, collection: &str) -> Vec<Log> {
        self.collections
            .read()
            .get(collection)
            .cloned()
            .unwrap_or_default()
    }
}

impl Default for MemoryConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
#[typetag::serde]
impl Connector for MemoryConnector {
    fn get_collection(&self, collection: &str) -> Option<Collection> {
        if self.collections.read().contains_key(collection) {
            Some(Collection::default())
        } else {
            None
        }
    }

    fn get_handle(&self, _collection: &str) -> Result<Box<dyn QueryHandle>> {
        Ok(Box::new(MemoryHandle))
    }

    async fn query(
        &self,
        collection: &str,
        _handle: &dyn QueryHandle,
        _split: Option<&dyn super::Split>,
    ) -> Result<QueryResponse> {
        let logs = self.get_logs(collection);
        let stream = try_stream! {
            for log in logs {
                yield log;
            }
        };
        Ok(QueryResponse::Logs(Box::pin(stream)))
    }

    fn create_sink(&self, collection: &str) -> Option<Box<dyn Sink>> {
        self.collections
            .write()
            .entry(collection.to_string())
            .or_default();
        Some(Box::new(MemorySink::new(
            self.collections.clone(),
            collection.to_string(),
        )))
    }

    async fn close(&self) {
        // Nothing to close
    }
}
