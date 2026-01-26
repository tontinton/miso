use std::{cmp::Ordering, num::NonZero, thread::available_parallelism};

use bytesize::ByteSize;
use color_eyre::eyre::{Context, Result};
use flume::Receiver;
use miso_common::metrics::{METRICS, STEP_SORT};
use miso_workflow_types::{
    field::Field,
    log::{Log, LogItem},
    sort::{NullsOrder, Sort, SortOrder},
    value::Value,
};
use rayon::{
    ThreadPool, ThreadPoolBuilder,
    iter::{IntoParallelIterator, ParallelIterator},
    slice::ParallelSliceMut,
};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    CHANNEL_CAPACITY,
    cancel_iter::CancelIter,
    interpreter::get_field_value,
    log_iter_creator::IterCreator,
    memory_size::MemorySize,
    spawn_thread::{ThreadRx, spawn},
    type_tracker::TypeTracker,
};

const SORT_THREAD_TAG: &str = "sort";
const PARALLEL_CHUNK_SIZE: usize = 25_000;

#[derive(Debug, Error)]
pub enum SortError {
    #[error(
        "Sort operation exceeded memory limit: collected {collected} of logs but limit is {limit}. Consider adding filters to reduce the dataset size before sorting."
    )]
    MemoryLimitExceeded {
        collected: ByteSize,
        limit: ByteSize,
    },
}

#[derive(Debug)]
pub struct SortComparator {
    by: Vec<Field>,
    sort_orders: Vec<SortOrder>,
    nulls_orders: Vec<NullsOrder>,
}

impl SortComparator {
    pub fn new(sorts: Vec<Sort>) -> Self {
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

pub fn cmp_logs(a: &Log, b: &Log, config: &SortComparator) -> Ordering {
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

fn collect_logs(
    by: &[Field],
    input: impl Iterator<Item = LogItem>,
    memory_limit: u64,
) -> Result<Vec<Log>> {
    let mut type_tracker = TypeTracker::new(by.len());

    let mut logs = Vec::new();
    let mut total_memory: u64 = 0;

    for log in input {
        let log = match log {
            LogItem::Log(log) => log,
            LogItem::Err(e) => return Err(e),
            LogItem::SourceDone(..)
            | LogItem::PartialStreamLog(..)
            | LogItem::PartialStreamDone(..) => {
                continue;
            }
        };

        for (i, key) in by.iter().enumerate() {
            if let Some(value) = get_field_value(&log, key) {
                type_tracker.check(i, value, key)?;
            }
        }

        let log_size = log.estimate_memory_size() as u64;
        total_memory += log_size;

        if total_memory > memory_limit {
            return Err(SortError::MemoryLimitExceeded {
                collected: ByteSize::b(total_memory),
                limit: ByteSize(memory_limit),
            }
            .into());
        }

        logs.push(log);
    }

    Ok(logs)
}

pub fn sort_num_threads() -> usize {
    let num_cores = available_parallelism().map(NonZero::get).unwrap_or(1);
    (num_cores / 4).max(1)
}

fn sort_thread_pool() -> Result<ThreadPool> {
    let num_threads = sort_num_threads();

    ThreadPoolBuilder::new()
        .thread_name(|i| format!("parallel-sort-{i}"))
        .num_threads(num_threads)
        .spawn_handler(move |thread| {
            std::thread::spawn(move || {
                let metric = METRICS.alive_threads.with_label_values(&[SORT_THREAD_TAG]);
                metric.inc();
                let _guard = scopeguard::guard(metric, |metric| {
                    metric.dec();
                });

                thread.run()
            });
            Ok(())
        })
        .build()
        .context("create sort thread pool")
}

pub fn parallel_sort(
    mut logs: Vec<Log>,
    config: &SortComparator,
    cancel: Option<&CancellationToken>,
) -> Result<Vec<Log>> {
    let pool = sort_thread_pool()?;
    let num_threads = pool.current_num_threads();
    let chunk_size = (logs.len() / num_threads).max(PARALLEL_CHUNK_SIZE);

    pool.install(|| {
        // Phase 1: Sort chunks in parallel
        let cancelled = logs.par_chunks_mut(chunk_size).any(|chunk| {
            if cancel.is_some_and(|c| c.is_cancelled()) {
                return true;
            }
            chunk.sort_unstable_by(|a, b| cmp_logs(a, b, config));
            false
        });

        if cancelled || cancel.is_some_and(|c| c.is_cancelled()) {
            return Ok(Vec::new());
        }

        // Phase 2: Parallel merge rounds
        let num_chunks = logs.len().div_ceil(chunk_size);
        if num_chunks <= 1 {
            return Ok(logs);
        }

        parallel_merge_rounds(logs, chunk_size, num_chunks, config, cancel)
    })
}

fn parallel_merge_rounds(
    logs: Vec<Log>,
    initial_chunk_size: usize,
    num_chunks: usize,
    config: &SortComparator,
    cancel: Option<&CancellationToken>,
) -> Result<Vec<Log>> {
    let mut current = logs;
    let mut chunk_size = initial_chunk_size;
    let mut chunks_remaining = num_chunks;

    while chunks_remaining > 1 {
        // Check cancel between merge rounds
        if cancel.is_some_and(|c| c.is_cancelled()) {
            return Ok(Vec::new());
        }

        // Merge adjacent pairs in parallel
        let pair_size = chunk_size * 2;
        let pairs: Vec<_> = current.chunks(pair_size).collect();

        let merged: Vec<Log> = pairs
            .into_par_iter()
            .flat_map(|pair| {
                if pair.len() <= chunk_size {
                    // Odd chunk, no merge needed
                    pair.to_vec()
                } else {
                    // Merge two sorted halves
                    let (left, right) = pair.split_at(chunk_size.min(pair.len()));
                    merge_sorted(left, right, config)
                }
            })
            .collect();

        current = merged;
        chunk_size = pair_size;
        chunks_remaining = chunks_remaining.div_ceil(2);
    }

    Ok(current)
}

fn merge_sorted(left: &[Log], right: &[Log], config: &SortComparator) -> Vec<Log> {
    let mut result = Vec::with_capacity(left.len() + right.len());
    let mut i = 0;
    let mut j = 0;

    while i < left.len() && j < right.len() {
        if cmp_logs(&left[i], &right[j], config).is_le() {
            result.push(left[i].clone());
            i += 1;
        } else {
            result.push(right[j].clone());
            j += 1;
        }
    }

    result.extend(left[i..].iter().cloned());
    result.extend(right[j..].iter().cloned());
    result
}

/// Parallel quicksort using rayon::join for benchmark comparison.
/// This is an in-place sort that uses parallel partitioning.
pub fn parallel_quicksort(
    logs: &mut [Log],
    config: &SortComparator,
    cancel: Option<&CancellationToken>,
) -> bool {
    let pool = match sort_thread_pool() {
        Ok(pool) => pool,
        Err(_) => return false,
    };

    pool.install(|| parallel_quicksort_inner(logs, config, cancel))
}

const QUICKSORT_SEQUENTIAL_THRESHOLD: usize = 10_000;

fn parallel_quicksort_inner(
    logs: &mut [Log],
    config: &SortComparator,
    cancel: Option<&CancellationToken>,
) -> bool {
    if cancel.is_some_and(|c| c.is_cancelled()) {
        return true;
    }

    if logs.len() <= QUICKSORT_SEQUENTIAL_THRESHOLD {
        logs.sort_unstable_by(|a, b| cmp_logs(a, b, config));
        return false;
    }

    let pivot_idx = partition(logs, config);

    let (left, right) = logs.split_at_mut(pivot_idx);
    let right = &mut right[1..]; // Skip the pivot

    let (left_cancelled, right_cancelled) = rayon::join(
        || parallel_quicksort_inner(left, config, cancel),
        || parallel_quicksort_inner(right, config, cancel),
    );

    left_cancelled || right_cancelled
}

fn partition(logs: &mut [Log], config: &SortComparator) -> usize {
    let len = logs.len();
    if len == 0 {
        return 0;
    }

    // Median-of-three pivot selection
    let mid = len / 2;
    let last = len - 1;

    // Sort first, mid, last to find median
    if cmp_logs(&logs[0], &logs[mid], config).is_gt() {
        logs.swap(0, mid);
    }
    if cmp_logs(&logs[mid], &logs[last], config).is_gt() {
        logs.swap(mid, last);
    }
    if cmp_logs(&logs[0], &logs[mid], config).is_gt() {
        logs.swap(0, mid);
    }

    // Move pivot to end
    logs.swap(mid, last);

    let mut i = 0;
    for j in 0..last {
        if cmp_logs(&logs[j], &logs[last], config).is_lt() {
            logs.swap(i, j);
            i += 1;
        }
    }
    logs.swap(i, last);
    i
}

pub fn sort_rx(
    creator: IterCreator,
    sorts: Vec<Sort>,
    memory_limit: u64,
    cancel: CancellationToken,
) -> (Receiver<LogItem>, ThreadRx) {
    let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);

    let thread = spawn(
        move || {
            let comparator = SortComparator::new(sorts);
            let mut logs = collect_logs(
                &comparator.by,
                CancelIter::new(creator.create(), cancel.clone()),
                memory_limit,
            )?;
            let rows_processed = logs.len() as u64;

            let sorted = if logs.len() < PARALLEL_CHUNK_SIZE {
                logs.sort_unstable_by(|a, b| cmp_logs(a, b, &comparator));
                logs
            } else {
                parallel_sort(logs, &comparator, Some(&cancel))?
            };

            let iter = CancelIter::new(sorted.into_iter(), cancel);
            for log in iter {
                if let Err(e) = tx.send(LogItem::Log(log)) {
                    debug!("Closing sort step: {e:?}");
                    break;
                }
            }

            METRICS
                .workflow_step_rows
                .with_label_values(&[STEP_SORT])
                .inc_by(rows_processed);

            Ok(())
        },
        SORT_THREAD_TAG,
    );

    (rx, thread)
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use collection_macros::btreemap;
    use miso_common::rand::pseudo_random;
    use miso_workflow_types::field_unwrap;

    #[test]
    fn chunked_kmerge_matches_par_sort() {
        let comparator = SortComparator {
            by: vec![field_unwrap!("x")],
            sort_orders: vec![SortOrder::Asc],
            nulls_orders: vec![NullsOrder::Last],
        };
        let logs: Vec<_> = (0..PARALLEL_CHUNK_SIZE * 2)
            .map(|x| {
                let random = pseudo_random(x);
                btreemap! { "x".to_string() => Value::Int(random as i64) }
            })
            .collect();

        let mut expected = logs.clone();
        expected.par_sort_unstable_by(|a, b| cmp_logs(a, b, &comparator));

        let actual = parallel_sort(logs, &comparator, None).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn parallel_quicksort_matches_par_sort() {
        let comparator = SortComparator {
            by: vec![field_unwrap!("x")],
            sort_orders: vec![SortOrder::Asc],
            nulls_orders: vec![NullsOrder::Last],
        };
        let logs: Vec<_> = (0..PARALLEL_CHUNK_SIZE * 2)
            .map(|x| {
                let random = pseudo_random(x);
                btreemap! { "x".to_string() => Value::Int(random as i64) }
            })
            .collect();

        let mut expected = logs.clone();
        expected.par_sort_unstable_by(|a, b| cmp_logs(a, b, &comparator));

        let mut actual = logs;
        parallel_quicksort(&mut actual, &comparator, None);

        assert_eq!(actual, expected);
    }
}
