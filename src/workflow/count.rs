use std::iter;

use serde_json::Value;

use crate::{
    log::{Log, LogItem, LogIter},
    try_next,
};

use super::partial_stream::PartialLogIter;

pub const COUNT_LOG_FIELD_NAME: &str = "count";

pub fn count_to_log(count: u64) -> Log {
    let mut log = Log::new();
    log.insert(COUNT_LOG_FIELD_NAME.into(), Value::from(count));
    log
}

fn log_to_count(mut log: Log) -> Option<u64> {
    if let Some(value) = log.remove(COUNT_LOG_FIELD_NAME) {
        if let Some(count) = value.as_u64() {
            return Some(count);
        }
    }
    None
}

pub struct CountIter {
    input: LogIter,
    count: u64,
    done: bool,
}

impl CountIter {
    pub fn new(input: LogIter) -> Self {
        Self {
            input,
            count: 0,
            done: false,
        }
    }
}

impl Iterator for CountIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        while try_next!(self.input).is_some() {
            self.count += 1;
        }
        self.done = true;
        Some(LogItem::Log(count_to_log(self.count)))
    }
}

pub struct MuxCountIter {
    input: LogIter,
    count: u64,
    done: bool,
}

impl MuxCountIter {
    pub fn new(input: LogIter) -> Self {
        Self {
            input,
            count: 0,
            done: false,
        }
    }
}

impl Iterator for MuxCountIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        while let Some(log) = try_next!(self.input) {
            if let Some(count) = log_to_count(log) {
                self.count += count;
            }
        }
        self.done = true;
        Some(LogItem::Log(count_to_log(self.count)))
    }
}

impl PartialLogIter for MuxCountIter {
    fn get_partial(&self) -> LogIter {
        Box::new(iter::once(LogItem::Log(count_to_log(self.count))))
    }
}
