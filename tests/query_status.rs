mod common;

use std::sync::Arc;

use color_eyre::Result;
use ctor::ctor;
use miso_connectors::{memory::MemoryConnector, Connector};
use miso_server::query_status::{
    QueryStatus, QueryStatusWriter, END_TIME_FIELD, ERROR_FIELD, QUERY_FIELD, QUERY_ID_FIELD,
    RUN_TIME_FIELD, RUN_TIME_SECONDS_FIELD, START_TIME_FIELD, STATUS_FIELD,
};

use common::init_test_tracing;
use uuid::Uuid;

#[ctor]
fn init() {
    init_test_tracing();
}

#[tokio::test]
async fn query_status_with_memory_connector() -> Result<()> {
    let connector = Arc::new(MemoryConnector::new());
    let sink = connector
        .create_updatable_sink("query_status", QUERY_ID_FIELD)
        .unwrap();
    let writer = QueryStatusWriter::new(sink);

    let handle = writer
        .start(Uuid::now_v7(), "test.data | count".into())
        .await;

    let record = &connector.get_logs("query_status")[0];
    assert_eq!(
        record.get(STATUS_FIELD).and_then(|v| v.as_str()),
        Some("planning")
    );
    assert_eq!(
        record.get(QUERY_FIELD).and_then(|v| v.as_str()),
        Some("test.data | count")
    );
    assert!(record.contains_key(START_TIME_FIELD));
    assert!(record.get(END_TIME_FIELD).unwrap().is_null());

    handle.update(QueryStatus::Running).await;
    let record = &connector.get_logs("query_status")[0];
    assert_eq!(
        record.get(STATUS_FIELD).and_then(|v| v.as_str()),
        Some("running")
    );

    handle.finish_with_now(QueryStatus::Success).await;
    let records = connector.get_logs("query_status");
    let record = &records[0];
    assert_eq!(
        record.get(STATUS_FIELD).and_then(|v| v.as_str()),
        Some("success")
    );
    assert!(!record.get(END_TIME_FIELD).unwrap().is_null());
    assert!(!record.get(RUN_TIME_FIELD).unwrap().is_null());
    assert!(!record.get(RUN_TIME_SECONDS_FIELD).unwrap().is_null());
    assert!(record.get(ERROR_FIELD).unwrap().is_null());
    assert_eq!(
        records.len(),
        1,
        "upsert should not create duplicate records"
    );

    Ok(())
}
