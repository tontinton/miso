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
use rayon::{ThreadPool, ThreadPoolBuilder, slice::ParallelSliceMut};
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

/// If the sorting set is smaller than this, just sort immediately without building a thread pool
/// to sort in parallel.
const PARALLEL_SORT_THRESHOLD: usize = 5000;
const SORT_THREAD_TAG: &str = "sort";

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
    pub(super) fn new(sorts: Vec<Sort>) -> Self {
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

fn sort_thread_pool() -> Result<ThreadPool> {
    let num_cores = available_parallelism().map(NonZero::get).unwrap_or(1);
    let num_threads = (num_cores / 4).max(1);

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

pub fn sort_rx(
    creator: IterCreator,
    sorts: Vec<Sort>,
    memory_limit: u64,
    cancel: CancellationToken,
) -> (Receiver<LogItem>, ThreadRx) {
    let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);

    let thread = spawn(
        move || {
            let config = SortComparator::new(sorts);
            let mut logs = collect_logs(
                &config.by,
                CancelIter::new(creator.create(), cancel.clone()),
                memory_limit,
            )?;
            let rows_processed = logs.len() as u64;

            let sorted = if logs.len() < PARALLEL_SORT_THRESHOLD {
                logs.sort_unstable_by(|a, b| cmp_logs(a, b, &config));
                logs
            } else {
                sort_thread_pool()?.install(move || {
                    // How to cancel this operation?
                    // One idea is to use par_chunks_mut() and sort_unstable_by() on each chunk,
                    // then use itertools::kmerge(). This will split the operations a little and we
                    // can check for cancel between operations. What I don't like about this
                    // solution is: it seems like par_sort_unstable_by() is optimized and will be
                    // faster than a manual naive implementation.
                    logs.par_sort_unstable_by(|a, b| cmp_logs(a, b, &config));
                    logs
                })
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
