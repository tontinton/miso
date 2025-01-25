use std::{cmp::Ordering, collections::BinaryHeap};

use futures_util::StreamExt;

use color_eyre::Result;
use tracing::info;

use crate::log::{Log, LogStream};

use super::sort::{cmp_logs, Sort, SortConfig};

#[derive(Debug)]
struct Sortable<'a> {
    log: Log,
    config: &'a SortConfig,
}

impl Ord for Sortable<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        if let Some(ordering) = cmp_logs(&self.log, &other.log, self.config) {
            ordering
        } else {
            // On wrong type, provide the opposite to get the log out of the heap.
            Ordering::Greater
        }
    }
}

impl PartialOrd for Sortable<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Sortable<'_> {}

impl PartialEq for Sortable<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

pub async fn topn_stream(
    sorts: Vec<Sort>,
    limit: u32,
    mut input_stream: LogStream,
) -> Result<Vec<Log>> {
    info!(
        "Collecting top {} sorted by {}",
        limit,
        sorts
            .iter()
            .map(|sort| sort.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let config = SortConfig::new(&sorts);

    let mut heap = BinaryHeap::new();

    while let Some(log) = input_stream.next().await {
        let sortable = Sortable {
            log,
            config: &config,
        };

        if heap.len() < limit as usize {
            heap.push(sortable);
        } else {
            let bottom_of_top = heap.peek().unwrap();
            if sortable.cmp(bottom_of_top) == Ordering::Less {
                heap.pop();
                heap.push(sortable);
            }
        }
    }

    Ok(heap.into_sorted_vec().into_iter().map(|x| x.log).collect())
}
