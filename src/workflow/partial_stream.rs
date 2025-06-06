use serde_json::{json, Map, Value};

use crate::log::Log;

use super::MISO_METADATA_FIELD_NAME;

const PARTIAL_STREAM_ID_FIELD_NAME: &str = "id";
const PARTIAL_STREAM_DONE_FIELD_NAME: &str = "done";

pub fn add_partial_stream_id(mut log: Log, id: usize) -> Log {
    log.entry(MISO_METADATA_FIELD_NAME)
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .unwrap()
        .insert(PARTIAL_STREAM_ID_FIELD_NAME.to_string(), Value::from(id));
    log
}

pub fn build_partial_stream_id_done_log(id: usize) -> Log {
    let mut log = Map::with_capacity(1);
    log.insert(
        MISO_METADATA_FIELD_NAME.to_string(),
        json!({
            PARTIAL_STREAM_ID_FIELD_NAME: id,
            PARTIAL_STREAM_DONE_FIELD_NAME: true,
        }),
    );
    log
}

pub fn get_partial_id(log: &Log) -> Option<(usize, bool)> {
    let metadata = log.get(MISO_METADATA_FIELD_NAME)?;
    let obj = metadata.as_object()?;

    let id = obj
        .get(PARTIAL_STREAM_ID_FIELD_NAME)
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)?;

    let done = obj
        .get(PARTIAL_STREAM_DONE_FIELD_NAME)
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    Some((id, done))
}
