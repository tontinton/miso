use std::{future::Future, sync::Arc};

use futures_util::{future::join_all, pin_mut};
use serde_json::{json, Map, Value};
use tokio::{spawn, sync::Notify, time::sleep};

use crate::log::Log;

use super::{PartialStream, MISO_METADATA_FIELD_NAME};

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

pub async fn run_with_partial_stream<T, StreamFut, PartialFn, PartialStreamFut>(
    partial_stream: PartialStream,
    stream_done_notify: Arc<Notify>,
    full_stream_fut: StreamFut,
    mut create_partial_task: PartialFn,
) -> T
where
    StreamFut: Future<Output = T>,
    PartialFn: FnMut(usize) -> PartialStreamFut,
    PartialStreamFut: Future<Output = ()> + Send + 'static,
{
    pin_mut!(full_stream_fut);

    let debounce = partial_stream.debounce;

    let mut partial_sender_tasks = Vec::new();
    let mut partial_send_id = 0;

    let result = loop {
        tokio::select! {
            result = &mut full_stream_fut => break result,
            () = stream_done_notify.notified() => {},
        }

        let sleep_fut = sleep(debounce);

        let task = create_partial_task(partial_send_id);
        partial_sender_tasks.push(spawn(task));
        partial_send_id += 1;

        tokio::select! {
            result = &mut full_stream_fut => break result,
            () = sleep_fut => {}
        }
    };

    join_all(partial_sender_tasks).await;

    result
}
