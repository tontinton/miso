use std::{cmp::Ordering, collections::BinaryHeap};

use async_stream::try_stream;
use futures_util::StreamExt;
use hashbrown::HashMap;
use tokio::task_local;

use crate::log::{Log, LogStream, LogTryStream};

use super::{
    partial_stream::get_partial_id,
    sort::{cmp_logs, Sort, SortConfig},
};

task_local! {
    pub static SORT_CONFIG: SortConfig;
}

/// A wrapper to be able to use Log in a BinaryHeap by reading the comparison configuration from a
/// task local variable.
#[derive(Debug)]
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
) -> (LogTryStream, SortConfig) {
    let stream = Box::pin(try_stream! {
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
