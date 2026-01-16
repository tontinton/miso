use std::sync::Arc;

use miso_connectors::UpdatableSink;
use miso_workflow_types::{log::Log, value::Value};
use time::{Duration, OffsetDateTime};
use tracing::error;
use uuid::Uuid;

pub(crate) const QUERY_ID_FIELD: &str = "id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryStatus {
    Planning,
    Running,
    Success,
    InternalError(String),
    ConnectorError(String),
    Cancelled,
}

impl QueryStatus {
    fn as_str(&self) -> &'static str {
        match self {
            QueryStatus::Planning => "planning",
            QueryStatus::Running => "running",
            QueryStatus::Success => "success",
            QueryStatus::InternalError(_) => "internal_error",
            QueryStatus::ConnectorError(_) => "connector_error",
            QueryStatus::Cancelled => "cancelled",
        }
    }

    fn error_message(&self) -> Option<&str> {
        match self {
            QueryStatus::InternalError(msg) | QueryStatus::ConnectorError(msg) => Some(msg),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct QueryStatusWriter {
    sink: Arc<dyn UpdatableSink>,
}

impl QueryStatusWriter {
    pub fn new(sink: Box<dyn UpdatableSink>) -> Self {
        Self { sink: sink.into() }
    }

    pub async fn start(&self, query_id: Uuid, query: String) -> QueryStatusHandle {
        let handle = QueryStatusHandle {
            sink: self.sink.clone(),
            query_id,
            start_time: OffsetDateTime::now_utc(),
            query,
        };
        handle.write(QueryStatus::Planning, None).await;
        handle
    }
}

#[derive(Clone)]
pub struct QueryStatusHandle {
    sink: Arc<dyn UpdatableSink>,
    query_id: Uuid,
    start_time: OffsetDateTime,
    query: String,
}

impl QueryStatusHandle {
    pub async fn update(&self, status: QueryStatus) {
        self.write(status, None).await;
    }

    pub async fn finish(self, status: QueryStatus) {
        let end_time = OffsetDateTime::now_utc();
        self.write(status, Some(end_time)).await;
    }

    async fn write(&self, status: QueryStatus, end_time: Option<OffsetDateTime>) {
        let update_time = OffsetDateTime::now_utc();
        let run_time = end_time.map(|end| end - self.start_time);

        let log = Log::from([
            (QUERY_ID_FIELD.to_string(), self.query_id.to_string().into()),
            ("status".to_string(), status.as_str().into()),
            (
                "error".to_string(),
                status.error_message().map_or(Value::Null, |msg| msg.into()),
            ),
            ("start_time".to_string(), self.start_time.into()),
            (
                "end_time".to_string(),
                end_time.map_or(Value::Null, |t: OffsetDateTime| t.into()),
            ),
            (
                "run_time".to_string(),
                run_time.map_or(Value::Null, |d: Duration| d.into()),
            ),
            ("update_time".to_string(), update_time.into()),
            ("query".to_string(), self.query.as_str().into()),
        ]);

        if let Err(e) = self.sink.upsert(log).await {
            error!("Failed to upsert query status: {e}");
        }
        self.sink.flush().await;
    }
}
