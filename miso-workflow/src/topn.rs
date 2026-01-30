//! Keeps the top N logs using a heap. More memory efficient than sort + limit for small N.

use std::{cmp::Ordering, collections::BinaryHeap, iter, rc::Rc};

use miso_common::metrics::{METRICS, STEP_TOPN};
use miso_workflow_types::{
    field::Field,
    log::{Log, LogItem, LogIter, PartialStreamKey},
    sort::{NullsOrder, Sort, SortOrder},
    value::Value,
};
use scoped_thread_local::scoped_thread_local;

use super::{
    interpreter::get_field_value,
    log_utils::PartialStreamItem,
    partial_stream::PartialLogIter,
    partial_stream_tracker::{Mergeable, PartialStreamTracker},
    try_next, try_next_with_partial_stream,
};

#[derive(Debug)]
struct SortComparator {
    by: Vec<Field>,
    sort_orders: Vec<SortOrder>,
    nulls_orders: Vec<NullsOrder>,
}

impl SortComparator {
    fn new(sorts: Vec<Sort>) -> Self {
        let mut by = Vec::with_capacity(sorts.len());
        let mut sort_orders = Vec::with_capacity(sorts.len());
        let mut nulls_orders = Vec::with_capacity(sorts.len());

        for sort in sorts {
            by.push(sort.by);
            sort_orders.push(sort.order);
            nulls_orders.push(sort.nulls);
        }

        Self {
            by,
            sort_orders,
            nulls_orders,
        }
    }
}

fn cmp_logs(a: &Log, b: &Log, config: &SortComparator) -> Ordering {
    for ((key, sort_order), nulls_order) in config
        .by
        .iter()
        .zip(&config.sort_orders)
        .zip(&config.nulls_orders)
    {
        let a_val = get_field_value(a, key).unwrap_or(&Value::Null);
        let b_val = get_field_value(b, key).unwrap_or(&Value::Null);
        let mut any_null = true;
        let ordering = match (a_val, b_val, nulls_order) {
            (Value::Null, Value::Null, _) => Ordering::Equal,
            (Value::Null, _, NullsOrder::First) => Ordering::Less,
            (_, Value::Null, NullsOrder::First) => Ordering::Greater,
            (Value::Null, _, NullsOrder::Last) => Ordering::Greater,
            (_, Value::Null, NullsOrder::Last) => Ordering::Less,
            _ => {
                any_null = false;
                a_val.cmp(b_val)
            }
        };

        if ordering == Ordering::Equal {
            continue;
        }

        if any_null {
            return ordering;
        }

        return if *sort_order == SortOrder::Asc {
            ordering
        } else {
            ordering.reverse()
        };
    }

    Ordering::Equal
}

struct SortComparatorTLS<'a>(&'a SortComparator);

scoped_thread_local!(static SORT_COMPARATOR: for<'a> SortComparatorTLS<'a>);

/// A wrapper to be able to use Log in a BinaryHeap by reading the comparison configuration from a
/// task local variable.
#[derive(Debug, Clone)]
struct SortableLog(Log);

impl Ord for SortableLog {
    fn cmp(&self, other: &Self) -> Ordering {
        SORT_COMPARATOR.with(|tls| cmp_logs(&self.0, &other.0, tls.0))
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
    comparator: Rc<SortComparator>,
}

impl TopNState {
    fn new(limit: usize, comparator: Rc<SortComparator>) -> Self {
        Self {
            limit,
            heap: BinaryHeap::new(),
            comparator,
        }
    }

    fn push_sortable(&mut self, sortable: SortableLog) {
        SORT_COMPARATOR.set(&mut SortComparatorTLS(&self.comparator), || {
            if self.heap.len() < self.limit {
                self.heap.push(sortable);
            } else if let Some(bottom) = self.heap.peek()
                && sortable.cmp(bottom) == Ordering::Less
            {
                self.heap.pop();
                self.heap.push(sortable);
            }
        });
    }

    fn push(&mut self, log: Log) {
        self.push_sortable(SortableLog(log));
    }

    fn into_sorted_vec(self) -> Vec<SortableLog> {
        SORT_COMPARATOR.set(&mut SortComparatorTLS(&self.comparator), || {
            self.heap.into_sorted_vec()
        })
    }
}

impl Mergeable for TopNState {
    fn merge(&mut self, other: &Self) {
        for item in &other.heap {
            self.push_sortable(item.clone());
        }
    }
}

pub struct TopNIter {
    input: LogIter,
    comparator: Rc<SortComparator>,
    limit: usize,
    tracker: PartialStreamTracker<TopNState>,
    logs: LogIter,
    pending_batches: Vec<(Vec<SortableLog>, PartialStreamKey)>,
    rows_processed: u64,
    done: bool,
}

impl TopNIter {
    pub fn new(input: LogIter, sorts: Vec<Sort>, limit: usize) -> Self {
        let comparator = Rc::new(SortComparator::new(sorts));

        Self {
            input,
            comparator: comparator.clone(),
            limit,
            tracker: PartialStreamTracker::new(TopNState::new(limit, comparator)),
            logs: Box::new(iter::empty()),
            pending_batches: Vec::new(),
            rows_processed: 0,
            done: false,
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
        if self.done {
            return None;
        }

        while let Some(item) = try_next_with_partial_stream!(self.input) {
            match item {
                PartialStreamItem::Log(log) => {
                    self.rows_processed += 1;
                    self.tracker.update_final(|state| state.push(log));
                }
                PartialStreamItem::PartialStreamLog(log, key) => {
                    self.rows_processed += 1;
                    self.tracker
                        .get_or_create_state(key, || {
                            TopNState::new(self.limit, self.comparator.clone())
                        })
                        .push(log);
                }
                PartialStreamItem::PartialStreamDone(key) => {
                    if let Some((pstate, out_key)) = self.tracker.mark_done(key) {
                        return self
                            .set_next_partial_stream_batch(pstate.into_sorted_vec(), out_key);
                    }
                }
                PartialStreamItem::SourceDone(source_id) => {
                    for (pstate, out_key) in self.tracker.finish_source(source_id) {
                        self.pending_batches
                            .push((pstate.into_sorted_vec(), out_key));
                    }
                    if let Some(item) = self.drain_pending_batch() {
                        return Some(item);
                    }
                }
            };
        }

        self.done = true;
        let logs = std::mem::replace(
            &mut self.tracker,
            PartialStreamTracker::new(TopNState::new(self.limit, self.comparator.clone())),
        )
        .into_final_state()
        .into_sorted_vec();
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
                Rc::new(SortComparator::new(sorts)),
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
    use crate::test_utils::{pdone, plog_val as plog};
    use std::str::FromStr;

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
    fn partial_stream_source_done_drops_contributed() {
        let items = collect_topn(vec![
            plog(5, 0, 1),
            plog(3, 0, 2),
            pdone(0, 1),
            LogItem::SourceDone(2),
        ]);

        let vals = extract_partial_vals(&items);
        assert_eq!(vals.len(), 0);
    }
}
