use hashbrown::HashMap;
use miso_common::metrics::{METRICS, STEP_LIMIT};
use miso_workflow_types::log::{LogItem, LogIter, PartialStreamKey};

use crate::{log_utils::PartialStreamItem, try_next_with_partial_stream};

pub struct LimitIter {
    input: LogIter,
    limit: u64,
    streamed: u64,
    partial_limits: HashMap<PartialStreamKey, u64>,
    rows_processed: u64,
}

impl LimitIter {
    pub fn new(input: LogIter, limit: u64) -> Self {
        Self {
            input,
            limit,
            streamed: 0,
            partial_limits: HashMap::new(),
            rows_processed: 0,
        }
    }
}

impl Drop for LimitIter {
    fn drop(&mut self) {
        METRICS
            .workflow_step_rows
            .with_label_values(&[STEP_LIMIT])
            .inc_by(self.rows_processed);
    }
}

impl Iterator for LimitIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.streamed == self.limit {
            return None;
        }

        while let Some(item) = try_next_with_partial_stream!(self.input) {
            let log_item_to_stream = match item {
                PartialStreamItem::Log(log) => {
                    self.rows_processed += 1;
                    self.streamed += 1;
                    Some(LogItem::Log(log))
                }
                PartialStreamItem::PartialStreamLog(log, key) => {
                    self.rows_processed += 1;
                    let streamed = self.partial_limits.entry(key).or_insert(0);
                    if *streamed == self.limit {
                        None
                    } else {
                        *streamed += 1;
                        Some(LogItem::PartialStreamLog(log, key))
                    }
                }
                PartialStreamItem::PartialStreamDone(key) => {
                    self.partial_limits.remove(&key);
                    Some(LogItem::PartialStreamDone(key))
                }
                PartialStreamItem::SourceDone(id) => Some(LogItem::SourceDone(id)),
            };

            if let Some(log_item) = log_item_to_stream {
                return Some(log_item);
            }
        }

        None
    }
}
