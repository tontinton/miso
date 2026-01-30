//! Collects all logs, sorts them using Arrow's columnar format for speed. Memory limited.

use std::{num::NonZero, sync::Arc, thread::available_parallelism};

use arrow::{
    array::{
        ArrayRef, BooleanArray, DurationNanosecondArray, Float64Array, Int64Array, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    buffer::NullBuffer,
    compute::{SortColumn, SortOptions, lexsort_to_indices},
};
use bytesize::ByteSize;
use color_eyre::eyre::{Context, Result, eyre};
use flume::Receiver;
use miso_common::metrics::{METRICS, STEP_SORT};
use miso_workflow_types::{
    field::Field,
    log::{Log, LogItem},
    sort::{NullsOrder, Sort, SortOrder},
    value::{Value, ValueKind},
};
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};
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
/// to build the columnar arrow buffer in parallel.
const PARALLEL_SORT_COLUMN_BUILDING_THRESHOLD: usize = 32768;
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

fn value_sort_key(v: &Value) -> String {
    match v {
        Value::Null => "0".into(),
        Value::Bool(b) => format!("1{}", *b as u8),
        Value::Int(i) => format!("2{:020}", (*i as u64) ^ (1 << 63)),
        Value::UInt(u) => format!("2{:020}", u),
        Value::Float(f) => {
            let bits = f.to_bits();
            let sortable = if *f >= 0.0 { bits ^ (1 << 63) } else { !bits };
            format!("2{:020}", sortable)
        }
        Value::Timestamp(ts) => {
            format!("3{:020}", (ts.unix_timestamp_nanos() as u128) ^ (1 << 127))
        }
        Value::Timespan(d) => format!("4{:020}", (d.whole_nanoseconds() as u128) ^ (1 << 127)),
        Value::String(s) => format!("5{s}"),
        Value::Array(arr) => format!("6{}", array_sort_key(arr)),
        Value::Object(_) => format!("7{}", v),
    }
}

fn array_sort_key(arr: &[Value]) -> String {
    arr.iter()
        .map(value_sort_key)
        .collect::<Vec<_>>()
        .join("\x1F")
}

fn map_logs<'a, T, F>(logs: &'a [Log], pool: Option<&ThreadPool>, f: F) -> Vec<T>
where
    T: Send,
    F: Fn(&'a Log) -> T + Sync,
{
    match pool {
        Some(pool) => pool.install(|| logs.par_iter().map(&f).collect()),
        None => logs.iter().map(f).collect(),
    }
}

fn build_column_array(
    logs: &[Log],
    field: &Field,
    column_type: ValueKind,
    pool: Option<&ThreadPool>,
) -> ArrayRef {
    match column_type {
        ValueKind::Null => Arc::new(StringArray::from(vec![None::<&str>; logs.len()])),

        ValueKind::Bool => {
            let values = map_logs(logs, pool, |log| match get_field_value(log, field) {
                Some(Value::Bool(b)) => Some(*b),
                _ => None,
            });
            Arc::new(BooleanArray::from(values))
        }

        ValueKind::Int => {
            let values = map_logs(logs, pool, |log| match get_field_value(log, field) {
                Some(Value::Int(i)) => Some(*i),
                _ => None,
            });
            Arc::new(Int64Array::from(values))
        }

        ValueKind::UInt => {
            let values = map_logs(logs, pool, |log| match get_field_value(log, field) {
                Some(Value::UInt(u)) => Some(*u),
                _ => None,
            });
            Arc::new(UInt64Array::from(values))
        }

        ValueKind::Float => {
            let values = map_logs(logs, pool, |log| match get_field_value(log, field) {
                Some(Value::Float(f)) => Some(*f),
                _ => None,
            });
            Arc::new(Float64Array::from(values))
        }

        ValueKind::Timestamp => {
            let n = logs.len();
            let mut values = Vec::with_capacity(n);
            let mut nulls = Vec::with_capacity(n);
            for log in logs {
                if let Some(Value::Timestamp(ts)) = get_field_value(log, field) {
                    values.push(ts.unix_timestamp_nanos() as i64);
                    nulls.push(true);
                } else {
                    values.push(0);
                    nulls.push(false);
                }
            }
            Arc::new(TimestampNanosecondArray::new(
                values.into(),
                Some(NullBuffer::from(nulls)),
            ))
        }

        ValueKind::Timespan => {
            let values = map_logs(logs, pool, |log| match get_field_value(log, field) {
                Some(Value::Timespan(d)) => Some(d.whole_nanoseconds() as i64),
                _ => None,
            });
            Arc::new(DurationNanosecondArray::from(values))
        }

        ValueKind::String => {
            let values: Vec<Option<&str>> =
                map_logs(logs, pool, |log| match get_field_value(log, field) {
                    Some(Value::String(s)) => Some(s.as_str()),
                    _ => None,
                });
            Arc::new(StringArray::from(values))
        }

        ValueKind::Array => {
            let values: Vec<Option<String>> =
                map_logs(logs, pool, |log| match get_field_value(log, field) {
                    Some(Value::Array(arr)) => Some(array_sort_key(arr)),
                    _ => None,
                });
            Arc::new(StringArray::from(values))
        }

        ValueKind::Object => {
            let values: Vec<Option<String>> =
                map_logs(logs, pool, |log| match get_field_value(log, field) {
                    Some(v @ Value::Object(_)) => Some(v.to_string()),
                    _ => None,
                });
            Arc::new(StringArray::from(values))
        }
    }
}

fn build_sort_columns(
    logs: &[Log],
    sorts: &[Sort],
    types: &[ValueKind],
    pool: Option<&ThreadPool>,
) -> Vec<SortColumn> {
    sorts
        .iter()
        .zip(types)
        .map(|(sort, col_type)| SortColumn {
            values: build_column_array(logs, &sort.by, *col_type, pool),
            options: Some(SortOptions {
                descending: sort.order == SortOrder::Desc,
                nulls_first: sort.nulls == NullsOrder::First,
            }),
        })
        .collect()
}

pub fn columnar_sort(logs: Vec<Log>, sorts: &[Sort]) -> Result<Vec<Log>> {
    let types: Vec<ValueKind> = sorts
        .iter()
        .map(|s| {
            logs.iter()
                .find_map(|log| get_field_value(log, &s.by))
                .map(ValueKind::from)
                .unwrap_or(ValueKind::Null)
        })
        .collect();
    columnar_sort_with_types(logs, sorts, &types)
}

fn columnar_sort_with_types(
    logs: Vec<Log>,
    sorts: &[Sort],
    types: &[ValueKind],
) -> Result<Vec<Log>> {
    if logs.is_empty() || sorts.is_empty() {
        return Ok(logs);
    }

    let pool = if logs.len() >= PARALLEL_SORT_COLUMN_BUILDING_THRESHOLD {
        sort_thread_pool().ok()
    } else {
        None
    };

    let sort_columns = build_sort_columns(&logs, sorts, types, pool.as_ref());
    let indices =
        lexsort_to_indices(&sort_columns, None).map_err(|e| eyre!("arrow sort failed: {e}"))?;
    Ok(reorder(logs, indices.values()))
}

fn reorder<T>(mut data: Vec<T>, indices: &[u32]) -> Vec<T> {
    let n = data.len();
    if n == 0 {
        return data;
    }

    let mut visited = vec![0u64; n.div_ceil(64)];

    let is_visited = |v: &[u64], i: usize| (v[i / 64] & (1 << (i % 64))) != 0;
    let set_visited = |v: &mut [u64], i: usize| v[i / 64] |= 1 << (i % 64);

    for i in 0..n {
        if is_visited(&visited, i) || indices[i] as usize == i {
            continue;
        }

        let mut j = i;
        while !is_visited(&visited, j) && indices[j] as usize != i {
            let next = indices[j] as usize;
            data.swap(j, next);
            set_visited(&mut visited, j);
            j = next;
        }
        set_visited(&mut visited, j);
    }

    data
}

fn collect_logs(
    by: &[Field],
    input: impl Iterator<Item = LogItem>,
    memory_limit: u64,
) -> Result<(Vec<Log>, Vec<ValueKind>)> {
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

    Ok((logs, type_tracker.into_types()))
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
            let by: Vec<Field> = sorts.iter().map(|s| s.by.clone()).collect();
            let (logs, types) = collect_logs(
                &by,
                CancelIter::new(creator.create(), cancel.clone()),
                memory_limit,
            )?;
            let rows_processed = logs.len() as u64;

            let sorted = columnar_sort_with_types(logs, &sorts, &types)?;

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
