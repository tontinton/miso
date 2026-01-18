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
    count: u64,
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
            count: 0,
            tracker: PartialStreamTracker::new(),
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
                    self.mode.update_count(&mut self.count, log);
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
                    self.tracker.source_completed(source_id);
                    for (count, out_key) in self.tracker.drain_completed() {
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
        Some(count_to_log_item(self.count))
    }
}

impl PartialLogIter for CountIter {
    fn get_partial(&self) -> LogIter {
        Box::new(iter::once(count_to_log_item(self.count)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miso_workflow_types::log::PartialStreamKey;

    fn log() -> LogItem {
        LogItem::Log(Log::new())
    }

    fn plog(partial_stream_id: usize, source_id: usize) -> LogItem {
        LogItem::PartialStreamLog(
            Log::new(),
            PartialStreamKey {
                partial_stream_id,
                source_id,
            },
        )
    }

    fn pdone(partial_stream_id: usize, source_id: usize) -> LogItem {
        LogItem::PartialStreamDone(PartialStreamKey {
            partial_stream_id,
            source_id,
        })
    }

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
    fn partial_stream_source_done_drains() {
        let items = collect_items(vec![
            plog(0, 1),
            plog(0, 2),
            pdone(0, 1),
            LogItem::SourceDone(2),
        ]);

        assert_eq!(count_source_done(&items), 1);
        assert_eq!(extract_counts(&items), vec![2, 0]);
    }

    #[test]
    fn mixed_logs_and_partial_streams() {
        let items = collect_items(vec![log(), plog(0, 1), log(), pdone(0, 1), log()]);

        let counts = extract_counts(&items);
        assert_eq!(counts.last(), Some(&3));
        assert!(counts.contains(&1));
    }
}
