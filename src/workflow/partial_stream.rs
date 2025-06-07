use std::sync::Arc;

use async_stream::stream;
use axum::async_trait;
use color_eyre::{eyre::Context, Result};
use futures_util::{
    future::{join, join_all},
    pin_mut,
    stream::select_all,
};
use serde_json::{json, Map, Value};
use tokio::{
    spawn,
    sync::{mpsc, Notify},
    time::sleep,
};
use tracing::debug;

use crate::{
    log::{Log, LogStream},
    workflow::logs_iter_to_tx,
};

use super::{PartialStream, MISO_METADATA_FIELD_NAME};

const PARTIAL_STREAM_ID_FIELD_NAME: &str = "id";
const PARTIAL_STREAM_DONE_FIELD_NAME: &str = "done";

#[async_trait]
pub trait PartialStreamExecutor {
    type Output;

    async fn execute(&self, input_stream: LogStream) -> Result<Self::Output>;
    fn get_partial(&self) -> Self::Output;
}

fn add_partial_stream_id(mut log: Log, id: usize) -> Log {
    log.entry(MISO_METADATA_FIELD_NAME)
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .unwrap()
        .insert(PARTIAL_STREAM_ID_FIELD_NAME.to_string(), Value::from(id));
    log
}

fn build_partial_stream_id_done_log(id: usize) -> Log {
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

fn rx_stream_notify(mut rx: mpsc::Receiver<Log>, notify: Arc<Notify>) -> LogStream {
    Box::pin(stream! {
        while let Some(log) = rx.recv().await {
            yield log
        }
        notify.notify_one();
    })
}

fn rx_union_stream_notify(rxs: Vec<mpsc::Receiver<Log>>, notify: Arc<Notify>) -> LogStream {
    let streams: Vec<LogStream> = rxs
        .into_iter()
        .map(|rx| rx_stream_notify(rx, notify.clone()))
        .collect();
    Box::pin(select_all(streams))
}

async fn partial_logs_iter_to_tx<I>(logs: I, id: usize, tx: mpsc::Sender<Log>, tag: &str)
where
    I: IntoIterator<Item = Log>,
{
    for log in logs {
        if let Err(e) = tx.send(add_partial_stream_id(log, id)).await {
            debug!("Closing {} step: {:?}", tag, e);
            break;
        }
    }

    let done_log = build_partial_stream_id_done_log(id);
    if let Err(e) = tx.send(done_log).await {
        debug!("Closing {} step: {:?}", tag, e);
    }
}

pub async fn execute_partial_stream<T, LogsFn>(
    executor: Box<dyn PartialStreamExecutor<Output = T> + Send>,
    config: PartialStream,
    rxs: Vec<mpsc::Receiver<Log>>,
    tx: mpsc::Sender<Log>,
    mut output_to_logs_fn: LogsFn,
    tag: &'static str,
) -> Result<()>
where
    LogsFn: FnMut(T) -> Vec<Log>,
{
    let stream_done_notify = Arc::new(Notify::new());

    let partial_stream_fut =
        executor.execute(rx_union_stream_notify(rxs, stream_done_notify.clone()));
    pin_mut!(partial_stream_fut);

    let debounce = config.debounce;

    let mut partial_sender_tasks = Vec::new();
    let mut partial_send_id = 0;

    let result = loop {
        tokio::select! {
            result = &mut partial_stream_fut => break result,
            () = stream_done_notify.notified() => {},
        }

        let sleep_fut = sleep(debounce);

        let logs = output_to_logs_fn(executor.get_partial());
        partial_sender_tasks.push(spawn(partial_logs_iter_to_tx(
            logs,
            partial_send_id,
            tx.clone(),
            tag,
        )));
        partial_send_id += 1;

        tokio::select! {
            result = &mut partial_stream_fut => break result,
            () = sleep_fut => {}
        }
    };

    let logs = output_to_logs_fn(result.with_context(|| format!("full stream output of {tag}"))?);
    join(
        logs_iter_to_tx(logs, tx, tag),
        join_all(partial_sender_tasks),
    )
    .await;

    Ok(())
}
