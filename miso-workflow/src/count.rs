use std::iter;

use hashbrown::HashMap;
use miso_common::metrics::{METRICS, STEP_COUNT};
use miso_workflow_types::{
    log::{Log, LogItem, LogIter},
    value::Value,
};

use super::{
    log_utils::PartialStreamItem, partial_stream::PartialLogIter, try_next_with_partial_stream,
};

const COUNT_LOG_FIELD_NAME: &str = "Count";

pub fn count_to_log(count: u64) -> Log {
    let mut log = Log::new();
    log.insert(COUNT_LOG_FIELD_NAME.into(), Value::from(count));
    log
}

fn count_to_log_item(count: u64) -> LogItem {
    LogItem::Log(count_to_log(count))
}

fn log_to_count(mut log: Log) -> Option<u64> {
    if let Some(value) = log.remove(COUNT_LOG_FIELD_NAME)
        && let Some(count) = value.as_u64()
    {
        return Some(count);
    }
    None
}

enum CountMode {
    Simple,
    Mux,
}

impl CountMode {
    fn update_count(&self, count: &mut u64, log: Log) {
        match self {
            CountMode::Simple => *count += 1,
            CountMode::Mux => {
                if let Some(c) = log_to_count(log) {
                    *count += c;
                }
            }
        }
    }
}

pub struct CountIter {
    input: LogIter,
    count: u64,
    partial_counts: HashMap<usize, u64>,
    mode: CountMode,
    next: Option<LogItem>,
    done: bool,
    rows_processed: u64,
}

impl CountIter {
    fn new(input: LogIter, mode: CountMode) -> Self {
        Self {
            input,
            count: 0,
            partial_counts: HashMap::new(),
            mode,
            next: None,
            done: false,
            rows_processed: 0,
        }
    }

    pub fn new_simple(input: LogIter) -> Self {
        Self::new(input, CountMode::Simple)
    }

    pub fn new_mux(input: LogIter) -> Self {
        Self::new(input, CountMode::Mux)
    }
}

impl Drop for CountIter {
    fn drop(&mut self) {
        METRICS
            .workflow_step_rows
            .with_label_values(&[STEP_COUNT])
            .inc_by(self.rows_processed);
    }
}

impl Iterator for CountIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        if let Some(log) = self.next.take() {
            return Some(log);
        }

        while let Some(item) = try_next_with_partial_stream!(self.input) {
            match item {
                PartialStreamItem::Log(log) => {
                    self.rows_processed += 1;
                    self.mode.update_count(&mut self.count, log);
                }
                PartialStreamItem::PartialStreamLog(log, id) => {
                    self.rows_processed += 1;
                    let count = self.partial_counts.entry(id).or_insert(0);
                    self.mode.update_count(count, log);
                }
                PartialStreamItem::PartialStreamDone(id) => {
                    if let Some(count) = self.partial_counts.remove(&id) {
                        self.next = Some(LogItem::PartialStreamDone(id));
                        return Some(LogItem::PartialStreamLog(count_to_log(count), id));
                    }
                }
            };
        }

        self.done = true;
        Some(count_to_log_item(self.count))
    }
}

impl PartialLogIter for CountIter {
    fn get_partial(&self) -> LogIter {
        Box::new(iter::once(count_to_log_item(self.count)))
    }
}
