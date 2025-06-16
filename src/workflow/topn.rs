use std::{cmp::Ordering, collections::BinaryHeap};

use async_stream::stream;
use axum::async_trait;
use color_eyre::Result;
use futures_util::StreamExt;
use hashbrown::HashMap;
use parking_lot::Mutex;
use tokio::task_local;

use crate::log::{Log, LogStream};

use super::{
    partial_stream::{get_partial_id, PartialStreamExecutor},
    sort::{cmp_logs, Sort, SortConfig},
};

task_local! {
    pub static SORT_CONFIG: SortConfig;
}

/// A wrapper to be able to use Log in a BinaryHeap by reading the comparison configuration from a
/// task local variable.
#[derive(Debug, Clone)]
struct SortableLog(Log);

impl Ord for SortableLog {
    fn cmp(&self, other: &Self) -> Ordering {
        SORT_CONFIG
            .with(|config| cmp_logs(&self.0, &other.0, config))
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

#[derive(Clone, Default)]
struct TopNState {
    limit: usize,
    heap: BinaryHeap<SortableLog>,
}

impl TopNState {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::new(),
        }
    }

    fn push(&mut self, log: Log) {
        let sortable = SortableLog(log);

        if self.heap.len() < self.limit {
            self.heap.push(sortable);
        } else {
            let bottom_of_top = self.heap.peek().unwrap();
            if sortable.cmp(bottom_of_top) == Ordering::Less {
                self.heap.pop();
                self.heap.push(sortable);
            }
        }
    }

    fn into_sorted_iter(self) -> impl Iterator<Item = Log> {
        self.heap.into_sorted_vec().into_iter().map(|x| x.0)
    }
}

/// The caller must scope the returned config via SORT_CONFIG.scope(), this is an optimization to
/// not need to store (a pointer to) the sort config per item in the binary heap.
pub async fn topn_stream(
    sorts: Vec<Sort>,
    limit: u32,
    mut input_stream: LogStream,
) -> (LogStream, SortConfig) {
    let stream = Box::pin(stream! {
        let mut state = TopNState::new(limit as usize);
        let mut partial_states: HashMap<usize, TopNState> = HashMap::new();

        while let Some(log) = input_stream.next().await {
            match get_partial_id(&log) {
                None => {
                    state.push(log);
                }
                Some((id, false)) => {
                    partial_states
                        .entry(id)
                        .or_insert_with(|| TopNState::new(limit as usize))
                        .push(log);
                }
                Some((id, true)) => {
                    if let Some(state) = partial_states.remove(&id) {
                        for log in state.into_sorted_iter() {
                            yield log;
                        }
                        yield log;
                    }
                }
            }
        }

        for log in state.into_sorted_iter() {
            yield log;
        }
    });

    (stream, SortConfig::new(sorts))
}

pub struct PartialTopNExecutor {
    // Assuming there cannot be a partial stream into a partial top-n stream, so only need to track
    // the top-n state of one stream (no passthrough).
    state: Mutex<TopNState>,
}

impl PartialTopNExecutor {
    /// Same as topn_stream, the caller must scope the returned config via SORT_CONFIG.scope().
    pub fn new(sorts: Vec<Sort>, limit: u32) -> (Self, SortConfig) {
        let state = Mutex::new(TopNState::new(limit as usize));
        (Self { state }, SortConfig::new(sorts))
    }
}

#[async_trait]
impl PartialStreamExecutor for PartialTopNExecutor {
    type Output = Vec<Log>;

    async fn execute(&self, mut input_stream: LogStream) -> Result<Self::Output> {
        while let Some(log) = input_stream.next().await {
            self.state.lock().push(log);
        }
        let state = std::mem::take(&mut *self.state.lock());
        return Ok(state.into_sorted_iter().collect());
    }

    fn get_partial(&self) -> Self::Output {
        let state = self.state.lock().clone();
        state.into_sorted_iter().collect()
    }
}
