//! Writes logs to a sink and consumes them.

use std::sync::Arc;

use flume::Sender;
use miso_common::metrics::{METRICS, STEP_WRITE};
use miso_connectors::Sink;
use miso_workflow_types::log::{Log, LogItem, LogIter, PartialStreamKey};
use tracing::{Instrument, info_span};

use crate::log_iter_creator::{IterCreator, fn_creator};
use crate::log_utils::PartialStreamItem;
use crate::partial_stream::add_partial_stream_id;
use crate::{AsyncTask, CHANNEL_CAPACITY};

use super::try_next_with_partial_stream;

#[derive(Clone, Debug)]
pub struct Write {
    pub sink: Arc<dyn Sink>,
}

impl PartialEq for Write {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl Write {
    pub fn new(sink: Arc<dyn Sink>) -> Self {
        Self { sink }
    }
}

pub struct WriteIter {
    input: LogIter,
    tx: Sender<(Log, Option<PartialStreamKey>)>,
    rows_processed: u64,
}

impl WriteIter {
    pub fn new(input: LogIter, tx: Sender<(Log, Option<PartialStreamKey>)>) -> Self {
        Self {
            input,
            tx,
            rows_processed: 0,
        }
    }
}

impl Drop for WriteIter {
    fn drop(&mut self) {
        METRICS
            .workflow_step_rows
            .with_label_values(&[STEP_WRITE])
            .inc_by(self.rows_processed);
    }
}

impl Iterator for WriteIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = try_next_with_partial_stream!(self.input)?;
            match item {
                PartialStreamItem::Log(log) => {
                    self.rows_processed += 1;
                    let _ = self.tx.send((log, None));
                }
                PartialStreamItem::PartialStreamLog(log, key) => {
                    self.rows_processed += 1;
                    let _ = self.tx.send((log, Some(key)));
                }
                PartialStreamItem::PartialStreamDone(_) | PartialStreamItem::SourceDone(_) => {}
            }
        }
    }
}

pub fn write_creator(input: IterCreator, write: Write) -> (IterCreator, AsyncTask) {
    let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);

    let task = tokio::spawn(
        async move {
            while let Ok((mut log, partial_stream_key)) = rx.recv_async().await {
                if let Some(key) = partial_stream_key {
                    log = add_partial_stream_id(log, key);
                }
                write.sink.write(log).await;
            }
            write.sink.flush().await;
            Ok(())
        }
        .instrument(info_span!("write")),
    );

    (
        fn_creator(move || {
            for _ in WriteIter::new(input.create(), tx) {}
            Box::new(std::iter::empty())
        }),
        task,
    )
}
