use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_stream::try_stream;
use async_trait::async_trait;
use color_eyre::eyre::Result;
use hashbrown::HashMap;
use miso_workflow_types::log::Log;
use miso_workflow_types::value::Value;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use super::{
    Collection, Connector, QueryHandle, QueryResponse, Sink, SinkUpsertError, UpdatableSink,
};

#[derive(Debug, Clone)]
enum CollectionStorage {
    Vec(Vec<Log>),
    Indexed {
        primary_key: String,
        logs: BTreeMap<Value, Log>,
    },
}

impl CollectionStorage {
    fn to_vec(&self) -> Vec<Log> {
        match self {
            CollectionStorage::Vec(v) => v.clone(),
            CollectionStorage::Indexed { logs, .. } => logs.values().cloned().collect(),
        }
    }
}

type Storage = HashMap<String, CollectionStorage>;

#[derive(Debug)]
pub struct MemorySink {
    storage: Arc<RwLock<Storage>>,
    collection: String,
}

impl MemorySink {
    fn new_vec(storage: Arc<RwLock<Storage>>, collection: String) -> Self {
        storage
            .write()
            .entry(collection.clone())
            .or_insert_with(|| CollectionStorage::Vec(Vec::new()));
        Self {
            storage,
            collection,
        }
    }

    fn new_indexed(storage: Arc<RwLock<Storage>>, collection: String, primary_key: String) -> Self {
        storage
            .write()
            .entry(collection.clone())
            .or_insert_with(|| CollectionStorage::Indexed {
                primary_key,
                logs: BTreeMap::new(),
            });
        Self {
            storage,
            collection,
        }
    }
}

#[async_trait]
impl Sink for MemorySink {
    async fn write(&self, log: Log) {
        let mut guard = self.storage.write();
        match guard.get_mut(&self.collection).expect("inserted in new") {
            CollectionStorage::Vec(v) => v.push(log),
            CollectionStorage::Indexed { primary_key, logs } => {
                let key = log.get(primary_key).cloned().unwrap_or(Value::Null);
                logs.insert(key, log);
            }
        }
    }
}

#[async_trait]
impl UpdatableSink for MemorySink {
    async fn upsert(&self, log: Log) -> Result<(), SinkUpsertError> {
        let mut guard = self.storage.write();
        match guard.get_mut(&self.collection).expect("inserted in new") {
            CollectionStorage::Indexed { primary_key, logs } => {
                let key_value = log
                    .get(primary_key)
                    .ok_or_else(|| SinkUpsertError::PrimaryKeyNotFound(primary_key.clone()))?
                    .clone();
                logs.insert(key_value, log);
            }
            CollectionStorage::Vec(_) => unreachable!("updatable sink uses indexed storage"),
        };

        Ok(())
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
    storage: Arc<RwLock<Storage>>,
}

impl MemoryConnector {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn get_logs(&self, collection: &str) -> Vec<Log> {
        self.storage
            .read()
            .get(collection)
            .map(|s| s.to_vec())
            .unwrap_or_default()
    }
}

impl Default for MemoryConnector {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
#[typetag::serde(name = "memory")]
impl Connector for MemoryConnector {
    fn get_collection(&self, collection: &str) -> Option<Collection> {
        if self.storage.read().contains_key(collection) {
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
        Some(Box::new(MemorySink::new_vec(
            self.storage.clone(),
            collection.to_string(),
        )))
    }

    fn create_updatable_sink(
        &self,
        collection: &str,
        primary_key: &str,
    ) -> Option<Box<dyn UpdatableSink>> {
        Some(Box::new(MemorySink::new_indexed(
            self.storage.clone(),
            collection.to_string(),
            primary_key.to_string(),
        )))
    }

    async fn close(&self) {
        // Nothing to close
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(fields: &[(&str, Value)]) -> Log {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[tokio::test]
    async fn sink_write_appends_to_vec() {
        let connector = MemoryConnector::new();
        let sink = connector.create_sink("test").unwrap();

        sink.write(log(&[("a", Value::Int(1))])).await;
        sink.write(log(&[("a", Value::Int(2))])).await;

        let logs = connector.get_logs("test");
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0].get("a"), Some(&Value::Int(1)));
        assert_eq!(logs[1].get("a"), Some(&Value::Int(2)));
    }

    #[tokio::test]
    async fn updatable_sink_upsert_replaces_by_key() {
        let connector = MemoryConnector::new();
        let sink = connector.create_updatable_sink("test", "id").unwrap();

        sink.upsert(log(&[
            ("id", Value::String("k1".into())),
            ("v", Value::Int(1)),
        ]))
        .await
        .unwrap();
        sink.upsert(log(&[
            ("id", Value::String("k1".into())),
            ("v", Value::Int(2)),
        ]))
        .await
        .unwrap();

        let logs = connector.get_logs("test");
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].get("v"), Some(&Value::Int(2)));
    }

    #[tokio::test]
    async fn upsert_returns_error_when_primary_key_missing() {
        let connector = MemoryConnector::new();
        let sink = connector.create_updatable_sink("test", "id").unwrap();

        let result = sink.upsert(log(&[("other", Value::Int(1))])).await;

        assert!(matches!(result, Err(SinkUpsertError::PrimaryKeyNotFound(key)) if key == "id"));
    }

    #[test]
    fn get_collection_reflects_sink_creation() {
        let connector = MemoryConnector::new();
        assert!(connector.get_collection("test").is_none());
        let _sink = connector.create_sink("test");
        assert!(connector.get_collection("test").is_some());
    }

    #[tokio::test]
    async fn collections_are_isolated() {
        let connector = MemoryConnector::new();
        let s1 = connector.create_sink("a").unwrap();
        let s2 = connector.create_sink("b").unwrap();

        s1.write(log(&[("x", Value::Int(1))])).await;
        s2.write(log(&[("x", Value::Int(2))])).await;

        assert_eq!(connector.get_logs("a")[0].get("x"), Some(&Value::Int(1)));
        assert_eq!(connector.get_logs("b")[0].get("x"), Some(&Value::Int(2)));
    }
}
