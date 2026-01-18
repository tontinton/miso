use std::{cmp::Ordering, collections::BinaryHeap, iter, rc::Rc};

use miso_common::metrics::{METRICS, STEP_TOPN};
use miso_workflow_types::{
    log::{Log, LogItem, LogIter, PartialStreamKey},
    sort::Sort,
};
use scoped_thread_local::scoped_thread_local;

use super::{
    log_utils::PartialStreamItem,
    partial_stream::PartialLogIter,
    partial_stream_tracker::PartialStreamTracker,
    sort::{SortConfig, cmp_logs},
    try_next, try_next_with_partial_stream,
};

struct SortConfigTLS<'a>(&'a SortConfig);

scoped_thread_local!(static SORT_CONFIG: for<'a> SortConfigTLS<'a>);

/// A wrapper to be able to use Log in a BinaryHeap by reading the comparison configuration from a
/// task local variable.
#[derive(Debug, Clone)]
struct SortableLog(Log);

impl Ord for SortableLog {
    fn cmp(&self, other: &Self) -> Ordering {
        SORT_CONFIG.with(|tls| cmp_logs(&self.0, &other.0, tls.0))
    }
}

impl PartialOrd for SortableLog {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for SortableLog {}

impl PartialEq for SortableLog {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

#[derive(Clone)]
struct TopNState {
    limit: usize,
    heap: BinaryHeap<SortableLog>,
    config: Rc<SortConfig>,
}

impl TopNState {
    fn new(limit: usize, config: Rc<SortConfig>) -> Self {
        Self {
            limit,
            heap: BinaryHeap::new(),
            config,
        }
    }

    fn push(&mut self, log: Log) {
        let sortable = SortableLog(log);

        SORT_CONFIG.set(&mut SortConfigTLS(&self.config), || {
            if self.heap.len() < self.limit {
                self.heap.push(sortable);
            } else {
                let bottom_of_top = self.heap.peek().unwrap();
                if sortable.cmp(bottom_of_top) == Ordering::Less {
                    self.heap.pop();
                    self.heap.push(sortable);
                }
            }
        });
    }

    fn into_sorted_vec(self) -> Vec<SortableLog> {
        SORT_CONFIG.set(&mut SortConfigTLS(&self.config), || {
            self.heap.into_sorted_vec()
        })
    }
}

pub struct TopNIter {
    input: LogIter,
    config: Rc<SortConfig>,
    limit: usize,
    state: Option<TopNState>,
    tracker: PartialStreamTracker<TopNState>,
    logs: LogIter,
    pending_batches: Vec<(Vec<SortableLog>, PartialStreamKey)>,
    rows_processed: u64,
}

impl TopNIter {
    pub fn new(input: LogIter, sorts: Vec<Sort>, limit: usize) -> Self {
        let config = Rc::new(SortConfig::new(sorts));

        Self {
            input,
            config: config.clone(),
            limit,
            state: Some(TopNState::new(limit, config)),
            tracker: PartialStreamTracker::new(),
            logs: Box::new(iter::empty()),
            pending_batches: Vec::new(),
            rows_processed: 0,
        }
    }

    fn set_next_batch(&mut self, logs: Vec<SortableLog>) -> Option<LogItem> {
        self.logs = Box::new(logs.into_iter().map(|x| LogItem::Log(x.0)));
        self.logs.next()
    }

    fn set_next_partial_stream_batch(
        &mut self,
        logs: Vec<SortableLog>,
        key: PartialStreamKey,
    ) -> Option<LogItem> {
        self.logs = Box::new(
            logs.into_iter()
                .map(move |x| LogItem::PartialStreamLog(x.0, key))
                .chain(iter::once(LogItem::PartialStreamDone(key))),
        );
        self.logs.next()
    }

    fn drain_pending_batch(&mut self) -> Option<LogItem> {
        if let Some((logs, key)) = self.pending_batches.pop() {
            return self.set_next_partial_stream_batch(logs, key);
        }
        None
    }
}

impl Drop for TopNIter {
    fn drop(&mut self) {
        METRICS
            .workflow_step_rows
            .with_label_values(&[STEP_TOPN])
            .inc_by(self.rows_processed);
    }
}

impl Iterator for TopNIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(log) = self.logs.next() {
            return Some(log);
        }
        if let Some(item) = self.drain_pending_batch() {
            return Some(item);
        }
        self.state.as_ref()?;

        while let Some(item) = try_next_with_partial_stream!(self.input) {
            match item {
                PartialStreamItem::Log(log) => {
                    self.rows_processed += 1;
                    self.state.as_mut().unwrap().push(log);
                }
                PartialStreamItem::PartialStreamLog(log, key) => {
                    self.rows_processed += 1;
                    let limit = self.limit;
                    let config = self.config.clone();
                    self.tracker
                        .get_or_create_state(key, || TopNState::new(limit, config))
                        .push(log);
                }
                PartialStreamItem::PartialStreamDone(key) => {
                    if let Some((pstate, out_key)) = self.tracker.mark_done(key) {
                        return self
                            .set_next_partial_stream_batch(pstate.into_sorted_vec(), out_key);
                    }
                }
                PartialStreamItem::SourceDone(source_id) => {
                    self.tracker.source_completed(source_id);
                    for (pstate, out_key) in self.tracker.drain_completed() {
                        self.pending_batches
                            .push((pstate.into_sorted_vec(), out_key));
                    }
                    if let Some(item) = self.drain_pending_batch() {
                        return Some(item);
                    }
                }
            };
        }

        let logs = self.state.take().unwrap().into_sorted_vec();
        self.set_next_batch(logs)
    }
}

pub struct PartialTopNIter {
    input: LogIter,
    // Assuming there cannot be a partial stream into a partial top-n stream, so only need to track
    // the top-n state of one stream (no passthrough).
    state: Option<TopNState>,
    logs: LogIter,
    rows_processed: u64,
}

impl PartialTopNIter {
    pub fn new(input: LogIter, sorts: Vec<Sort>, limit: u64) -> Self {
        Self {
            input,
            state: Some(TopNState::new(
                limit as usize,
                Rc::new(SortConfig::new(sorts)),
            )),
            logs: Box::new(iter::empty()),
            rows_processed: 0,
        }
    }

    fn set_next_batch(&mut self, logs: Vec<SortableLog>) -> Option<LogItem> {
        self.logs = Box::new(logs.into_iter().map(|x| LogItem::Log(x.0)));
        self.logs.next()
    }
}

impl Drop for PartialTopNIter {
    fn drop(&mut self) {
        METRICS
            .workflow_step_rows
            .with_label_values(&[STEP_TOPN])
            .inc_by(self.rows_processed);
    }
}

impl PartialLogIter for PartialTopNIter {
    fn get_partial(&self) -> LogIter {
        Box::new(
            self.state
                .clone()
                .unwrap()
                .into_sorted_vec()
                .into_iter()
                .map(|x| LogItem::Log(x.0)),
        )
    }
}

impl Iterator for PartialTopNIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(log) = self.logs.next() {
            return Some(log);
        }

        let state = self.state.as_mut()?;
        while let Some(log) = try_next!(self.input) {
            self.rows_processed += 1;
            state.push(log);
        }

        let logs = self.state.take().unwrap().into_sorted_vec();
        self.set_next_batch(logs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miso_workflow_types::{
        field::Field,
        sort::{NullsOrder, SortOrder},
        value::Value,
    };
    use std::str::FromStr;

    fn plog(val: i64, partial_stream_id: usize, source_id: usize) -> LogItem {
        let mut l = Log::new();
        l.insert("x".into(), Value::from(val));
        LogItem::PartialStreamLog(
            l,
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

    fn top2_desc() -> (Vec<Sort>, usize) {
        (
            vec![Sort {
                by: Field::from_str("x").unwrap(),
                order: SortOrder::Desc,
                nulls: NullsOrder::default(),
            }],
            2,
        )
    }

    fn collect_topn(input: Vec<LogItem>) -> Vec<LogItem> {
        let (sorts, limit) = top2_desc();
        TopNIter::new(Box::new(input.into_iter()), sorts, limit).collect()
    }

    fn extract_partial_vals(items: &[LogItem]) -> Vec<i64> {
        items
            .iter()
            .filter_map(|i| match i {
                LogItem::PartialStreamLog(l, _) => l.get("x")?.as_i64(),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn partial_stream_two_sources_aggregation() {
        let items = collect_topn(vec![
            plog(5, 0, 1),
            plog(3, 0, 1),
            plog(10, 0, 2),
            pdone(0, 1),
            pdone(0, 2),
        ]);

        let vals = extract_partial_vals(&items);
        assert!(vals.contains(&10));
        assert!(vals.contains(&5));
        assert!(!vals.contains(&3));
    }

    #[test]
    fn partial_stream_respects_limit() {
        let items = collect_topn(vec![
            plog(1, 0, 1),
            plog(2, 0, 1),
            plog(3, 0, 1),
            pdone(0, 1),
        ]);

        let vals = extract_partial_vals(&items);
        assert_eq!(vals.len(), 2);
        assert!(vals.contains(&3));
        assert!(vals.contains(&2));
    }

    #[test]
    fn partial_stream_source_done_drains_pending() {
        let items = collect_topn(vec![
            plog(5, 0, 1),
            plog(3, 0, 2),
            pdone(0, 1),
            LogItem::SourceDone(2),
        ]);

        let vals = extract_partial_vals(&items);
        assert_eq!(vals.len(), 2);
    }
}
