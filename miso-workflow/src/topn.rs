use std::{cmp::Ordering, collections::BinaryHeap, iter, rc::Rc};

use hashbrown::HashMap;
use miso_workflow_types::{
    log::{Log, LogItem, LogIter},
    sort::Sort,
};
use scoped_thread_local::scoped_thread_local;

use super::{
    log_utils::PartialStreamItem,
    partial_stream::PartialLogIter,
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
        SORT_CONFIG
            .with(|tls| cmp_logs(&self.0, &other.0, tls.0))
            // On wrong type, provide the opposite to get the log out of the heap.
            .unwrap_or(Ordering::Greater)
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
    partial_states: HashMap<usize, TopNState>,
    logs: LogIter,
}

impl TopNIter {
    pub fn new(input: LogIter, sorts: Vec<Sort>, limit: usize) -> Self {
        let config = Rc::new(SortConfig::new(sorts));

        Self {
            input,
            config: config.clone(),
            limit,
            state: Some(TopNState::new(limit, config)),
            partial_states: HashMap::new(),
            logs: Box::new(iter::empty()),
        }
    }

    fn set_next_batch(&mut self, logs: Vec<SortableLog>) -> Option<LogItem> {
        self.logs = Box::new(logs.into_iter().map(|x| LogItem::Log(x.0)));
        self.logs.next()
    }

    fn set_next_partial_stream_batch(
        &mut self,
        logs: Vec<SortableLog>,
        id: usize,
    ) -> Option<LogItem> {
        self.logs = Box::new(
            logs.into_iter()
                .map(move |x| LogItem::PartialStreamLog(x.0, id))
                .chain(iter::once(LogItem::PartialStreamDone(id))),
        );
        self.logs.next()
    }
}

impl Iterator for TopNIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(log) = self.logs.next() {
            return Some(log);
        }
        let state = self.state.as_mut()?;

        while let Some(item) = try_next_with_partial_stream!(self.input) {
            match item {
                PartialStreamItem::Log(log) => {
                    state.push(log);
                }
                PartialStreamItem::PartialStreamLog(log, id) => {
                    self.partial_states
                        .entry(id)
                        .or_insert_with(|| TopNState::new(self.limit, self.config.clone()))
                        .push(log);
                }
                PartialStreamItem::PartialStreamDone(id) => {
                    if let Some(state) = self.partial_states.remove(&id) {
                        return self.set_next_partial_stream_batch(state.into_sorted_vec(), id);
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
}

impl PartialTopNIter {
    pub fn new(input: LogIter, sorts: Vec<Sort>, limit: u32) -> Self {
        Self {
            input,
            state: Some(TopNState::new(
                limit as usize,
                Rc::new(SortConfig::new(sorts)),
            )),
            logs: Box::new(iter::empty()),
        }
    }

    fn set_next_batch(&mut self, logs: Vec<SortableLog>) -> Option<LogItem> {
        self.logs = Box::new(logs.into_iter().map(|x| LogItem::Log(x.0)));
        self.logs.next()
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
            state.push(log);
        }

        let logs = self.state.take().unwrap().into_sorted_vec();
        self.set_next_batch(logs)
    }
}
