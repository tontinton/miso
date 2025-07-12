use hashbrown::HashMap;
use miso_workflow_types::log::{LogItem, LogIter};

use crate::{log_utils::PartialStreamItem, try_next_with_partial_stream};

pub struct LimitIter {
    input: LogIter,
    limit: u32,
    streamed: u32,
    partial_limits: HashMap<usize, u32>,
}

impl LimitIter {
    pub fn new(input: LogIter, limit: u32) -> Self {
        Self {
            input,
            limit,
            streamed: 0,
            partial_limits: HashMap::new(),
        }
    }
}

impl Iterator for LimitIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.streamed == self.limit {
            return None;
        }

        while let Some(item) = try_next_with_partial_stream!(self.input) {
            let log_to_stream = match item {
                PartialStreamItem::Log(log) => {
                    self.streamed += 1;
                    Some(log)
                }
                PartialStreamItem::PartialStreamLog(log, id) => {
                    let streamed = self.partial_limits.entry(id).or_insert(0);
                    if *streamed == self.limit {
                        None
                    } else {
                        *streamed += 1;
                        Some(log)
                    }
                }
                PartialStreamItem::PartialStreamDone(id) => {
                    self.partial_limits.remove(&id);
                    None
                }
            };

            if let Some(log) = log_to_stream {
                return Some(LogItem::Log(log));
            }
        }

        None
    }
}
