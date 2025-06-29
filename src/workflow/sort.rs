use std::{cmp::Ordering, fmt, num::NonZero, thread::available_parallelism};

use color_eyre::eyre::{bail, Context, Result};
use flume::Receiver;
use rayon::{slice::ParallelSliceMut, ThreadPool, ThreadPoolBuilder};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    cancel_iter::CancelIter,
    log::{Log, LogItem, LogIter},
    metrics::METRICS,
    send_once::SendOnce,
    spawn_thread::{spawn, ThreadRx},
};

use super::serde_json_utils::partial_cmp_values;

/// If the sorting set is smaller than this, just sort immediately without building a thread pool
/// to sort in parallel.
const PARALLEL_SORT_THREASHOLD: usize = 5000;
const SORT_THREAD_TAG: &str = "sort";

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum NullsOrder {
    #[default]
    Last,
    First,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Sort {
    pub by: String,

    #[serde(default)]
    pub order: SortOrder,

    #[serde(default)]
    pub nulls: NullsOrder,
}

#[derive(Debug)]
pub struct SortConfig {
    by: Vec<String>,
    sort_orders: Vec<SortOrder>,
    nulls_orders: Vec<NullsOrder>,
}

impl SortConfig {
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

impl fmt::Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SortOrder::Asc => write!(f, "asc"),
            SortOrder::Desc => write!(f, "desc"),
        }
    }
}

impl fmt::Display for NullsOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NullsOrder::Last => write!(f, "last"),
            NullsOrder::First => write!(f, "first"),
        }
    }
}

impl fmt::Display for Sort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (order: {}, nulls: {})",
            self.by, self.order, self.nulls
        )
    }
}

pub fn cmp_logs(a: &Log, b: &Log, config: &SortConfig) -> Option<Ordering> {
    for ((key, sort_order), nulls_order) in config
        .by
        .iter()
        .zip(&config.sort_orders)
        .zip(&config.nulls_orders)
    {
        let a_val = a.get(key).unwrap_or(&Value::Null);
        let b_val = b.get(key).unwrap_or(&Value::Null);
        let mut any_null = true;
        let ordering = match (a_val, b_val, nulls_order) {
            (Value::Null, Value::Null, _) => Ordering::Equal,
            (Value::Null, _, NullsOrder::First) => Ordering::Less,
            (_, Value::Null, NullsOrder::First) => Ordering::Greater,
            (Value::Null, _, NullsOrder::Last) => Ordering::Greater,
            (_, Value::Null, NullsOrder::Last) => Ordering::Less,
            _ => {
                any_null = false;
                partial_cmp_values(a_val, b_val)?
            }
        };

        if ordering == Ordering::Equal {
            continue;
        }

        if any_null {
            return Some(ordering);
        }

        return Some(if *sort_order == SortOrder::Asc {
            ordering
        } else {
            ordering.reverse()
        });
    }

    Some(Ordering::Equal)
}

fn collect_logs(by: &[String], input: impl Iterator<Item = LogItem>) -> Result<Vec<Log>> {
    let mut tracked_types = vec![None; by.len()];

    let mut logs = Vec::new();
    for log in input {
        let log = match log {
            LogItem::Log(log) => log,
            LogItem::Err(e) => return Err(e),
            LogItem::OneRxDone | LogItem::PartialStreamLog(..) | LogItem::PartialStreamDone(..) => {
                continue;
            }
        };

        for (tracked_type, key) in tracked_types.iter_mut().zip(by) {
            if let Some(value) = log.get(key) {
                if value != &Value::Null {
                    let value_type = std::mem::discriminant(value);
                    if let Some(t) = tracked_type {
                        if *t != value_type {
                            bail!(
                                "cannot sort over differing types (key '{}'): {:?} != {:?}",
                                key,
                                *t,
                                value_type
                            );
                        }
                    } else {
                        *tracked_type = Some(value_type);
                    }
                }
            }
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
    input: LogIter,
    sorts: Vec<Sort>,
    cancel: CancellationToken,
) -> (Receiver<LogItem>, ThreadRx) {
    let (tx, rx) = flume::bounded(1);

    let input = SendOnce::new(input);
    let thread = spawn(
        move || {
            let config = SortConfig::new(sorts);
            let mut logs = collect_logs(&config.by, CancelIter::new(input.take(), cancel.clone()))?;

            let sorted = if logs.len() < PARALLEL_SORT_THREASHOLD {
                logs.sort_unstable_by(|a, b| {
                    cmp_logs(a, b, &config).expect("types should have been validated")
                });
                logs
            } else {
                sort_thread_pool()?.install(move || {
                    // How to cancel this operation?
                    // One idea is to use par_chunks_mut() and sort_unstable_by() on each chunk,
                    // then use itertools::kmerge(). This will split the operations a little and we
                    // can check for cancel between operations. What I don't like about this
                    // solution is: it seems like par_sort_unstable_by() is optimized and will be
                    // faster than a manual naive implementation.
                    logs.par_sort_unstable_by(|a, b| {
                        cmp_logs(a, b, &config).expect("types should have been validated")
                    });
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
            Ok(())
        },
        SORT_THREAD_TAG,
    );

    (rx, thread)
}
