use std::sync::Arc;

use flume::Sender;
use miso_common::metrics::{METRICS, STEP_TEE};
use miso_connectors::Sink;
use miso_workflow_types::log::{Log, LogItem, LogIter};
use tracing::{Instrument, info_span};

use crate::log_iter_creator::{IterCreator, fn_creator};
use crate::log_utils::PartialStreamItem;
use crate::partial_stream::add_partial_stream_id;
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
    tx: Sender<(Log, Option<usize>)>,
    rows_processed: u64,
}

impl TeeIter {
    pub fn new(input: LogIter, tx: Sender<(Log, Option<usize>)>) -> Self {
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
                let _ = self.tx.send((log.clone(), None));
                Some(LogItem::Log(log))
            }
            PartialStreamItem::PartialStreamLog(log, id) => {
                self.rows_processed += 1;
                let _ = self.tx.send((log.clone(), Some(id)));
                Some(LogItem::PartialStreamLog(log, id))
            }
            PartialStreamItem::PartialStreamDone(id) => Some(LogItem::PartialStreamDone(id)),
        }
    }
}

pub fn tee_creator(input: IterCreator, tee: Tee) -> (IterCreator, AsyncTask) {
    let (tx, rx) = flume::bounded::<(Log, Option<usize>)>(CHANNEL_CAPACITY);

    let task = tokio::spawn(
        async move {
            while let Ok((mut log, partial_stream_id)) = rx.recv_async().await {
                if let Some(id) = partial_stream_id {
                    log = add_partial_stream_id(log, id);
                }
                tee.sink.write(log).await;
            }
            tee.sink.flush().await;
            Ok(())
        }
        .instrument(info_span!("tee")),
    );

    (
        fn_creator(move || Box::new(TeeIter::new(input.create(), tx))),
        task,
    )
}
