//! Counts logs and emits a single result. Mux mode sums existing count fields instead.

use std::iter;

use miso_common::metrics::{METRICS, STEP_COUNT};
use miso_workflow_types::{
    log::{Log, LogItem, LogIter},
    value::Value,
};

use super::{
    log_utils::PartialStreamItem, partial_stream::PartialLogIter,
    partial_stream_tracker::PartialStreamTracker, try_next_with_partial_stream,
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
    tracker: PartialStreamTracker<u64>,
    mode: CountMode,
    pending: Vec<LogItem>,
    done: bool,
    rows_processed: u64,
}

impl CountIter {
    fn new(input: LogIter, mode: CountMode) -> Self {
        Self {
            input,
            tracker: PartialStreamTracker::new(0),
            mode,
            pending: Vec::new(),
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

        if let Some(item) = self.pending.pop() {
            return Some(item);
        }

        while let Some(item) = try_next_with_partial_stream!(self.input) {
            match item {
                PartialStreamItem::Log(log) => {
                    self.rows_processed += 1;
                    self.tracker
                        .update_final(|count| self.mode.update_count(count, log));
                }
                PartialStreamItem::PartialStreamLog(log, key) => {
                    self.rows_processed += 1;
                    let count = self.tracker.get_or_create_state(key, || 0);
                    self.mode.update_count(count, log);
                }
                PartialStreamItem::PartialStreamDone(key) => {
                    if let Some((count, out_key)) = self.tracker.mark_done(key) {
                        self.pending.push(LogItem::PartialStreamDone(out_key));
                        return Some(LogItem::PartialStreamLog(count_to_log(count), out_key));
                    }
                }
                PartialStreamItem::SourceDone(source_id) => {
                    for (count, out_key) in self.tracker.finish_source(source_id) {
                        self.pending.push(LogItem::PartialStreamDone(out_key));
                        self.pending
                            .push(LogItem::PartialStreamLog(count_to_log(count), out_key));
                    }
                    self.pending.push(LogItem::SourceDone(source_id));
                    if let Some(item) = self.pending.pop() {
                        return Some(item);
                    }
                }
            };
        }

        self.done = true;
        Some(count_to_log_item(*self.tracker.final_state()))
    }
}

impl PartialLogIter for CountIter {
    fn get_partial(&self) -> LogIter {
        Box::new(iter::once(count_to_log_item(*self.tracker.final_state())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{log, pdone, plog_empty as plog};

    fn collect_items(input: Vec<LogItem>) -> Vec<LogItem> {
        CountIter::new_simple(Box::new(input.into_iter())).collect()
    }

    fn extract_counts(items: &[LogItem]) -> Vec<u64> {
        items
            .iter()
            .filter_map(|i| match i {
                LogItem::Log(l) | LogItem::PartialStreamLog(l, _) => l.get("Count")?.as_u64(),
                _ => None,
            })
            .collect()
    }

    fn count_source_done(items: &[LogItem]) -> usize {
        items
            .iter()
            .filter(|i| matches!(i, LogItem::SourceDone(_)))
            .count()
    }

    #[test]
    fn source_done_passthrough() {
        let items = collect_items(vec![log(), LogItem::SourceDone(1), log()]);
        assert_eq!(count_source_done(&items), 1);
        assert_eq!(extract_counts(&items), vec![2]);
    }

    #[test]
    fn multiple_source_done_all_passthrough() {
        let items = collect_items(vec![
            log(),
            LogItem::SourceDone(1),
            log(),
            LogItem::SourceDone(2),
            log(),
        ]);
        assert_eq!(count_source_done(&items), 2);
        assert_eq!(extract_counts(&items), vec![3]);
    }

    #[test]
    fn partial_stream_aggregation_two_sources() {
        let items = collect_items(vec![
            plog(0, 1),
            plog(0, 1),
            plog(0, 2),
            pdone(0, 1),
            pdone(0, 2),
        ]);

        let partial_counts = extract_counts(&items);
        assert_eq!(partial_counts, vec![3, 0]);
    }

    #[test]
    fn partial_stream_source_done_drops_contributed() {
        let items = collect_items(vec![
            plog(0, 1),
            plog(0, 2),
            pdone(0, 1),
            LogItem::SourceDone(2),
        ]);

        assert_eq!(count_source_done(&items), 1);
        assert_eq!(extract_counts(&items), vec![0]);
    }

    #[test]
    fn mixed_logs_and_partial_streams() {
        let items = collect_items(vec![log(), plog(0, 1), log(), pdone(0, 1), log()]);
        let counts = extract_counts(&items);
        assert_eq!(counts, vec![3, 3]); // partial merges with final
    }

    #[test]
    fn partial_stream_merges_with_regular_logs() {
        let items = collect_items(vec![
            plog(0, 1),
            log(),
            log(),
            log(),
            plog(0, 1),
            LogItem::SourceDone(2),
            pdone(0, 1),
        ]);

        let counts = extract_counts(&items);
        assert!(counts.contains(&5)); // 2 partial + 3 regular
        assert_eq!(counts.last(), Some(&3));
    }
}
