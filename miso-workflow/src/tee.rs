use std::sync::Arc;

use flume::Sender;
use miso_common::metrics::{METRICS, STEP_TEE};
use miso_connectors::Sink;
use miso_workflow_types::log::{Log, LogItem, LogIter};
use tracing::Instrument;

use crate::log_utils::PartialStreamItem;
use crate::{AsyncTask, CHANNEL_CAPACITY};

use super::try_next_with_partial_stream;

/// Tee writes logs to a sink while forwarding them downstream.
#[derive(Clone, Debug)]
pub struct Tee {
    pub sink: Arc<dyn Sink>,
}

impl PartialEq for Tee {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Tee {
    pub fn new(sink: Arc<dyn Sink>) -> Self {
        Self { sink }
    }
}

pub struct TeeIter {
    input: LogIter,
    tx: Sender<Log>,
    rows_processed: u64,
}

impl TeeIter {
    pub fn new(input: LogIter, tx: Sender<Log>) -> Self {
        Self {
            input,
            tx,
            rows_processed: 0,
        }
    }
}

impl Drop for TeeIter {
    fn drop(&mut self) {
        METRICS
            .workflow_step_rows
            .with_label_values(&[STEP_TEE])
            .inc_by(self.rows_processed);
    }
}

impl Iterator for TeeIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        let item = try_next_with_partial_stream!(self.input)?;
        match item {
            PartialStreamItem::Log(log) => {
                self.rows_processed += 1;
                let _ = self.tx.send(log.clone());
                Some(LogItem::Log(log))
            }
            PartialStreamItem::PartialStreamLog(log, id) => {
                self.rows_processed += 1;
                Some(LogItem::PartialStreamLog(log, id))
            }
            PartialStreamItem::PartialStreamDone(id) => Some(LogItem::PartialStreamDone(id)),
        }
    }
}

pub fn tee_iter(input: LogIter, tee: Tee) -> (TeeIter, AsyncTask) {
    let (tx, rx) = flume::bounded::<Log>(CHANNEL_CAPACITY);

    let task = tokio::spawn(
        async move {
            while let Ok(log) = rx.recv_async().await {
                tee.sink.write(log).await;
            }
            tee.sink.flush().await;
            Ok(())
        }
        .in_current_span(),
    );

    (TeeIter::new(input, tx), task)
}
