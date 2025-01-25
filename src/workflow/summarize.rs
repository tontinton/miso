use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{self, AtomicI64},
        Arc,
    },
};

use atomic_float::AtomicF64;
use color_eyre::{eyre::bail, Result};
use futures_util::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::info;
use vrl::{core::Value, prelude::NotNan, value::KeyString};

use crate::log::{Log, LogStream};

use super::{sortable_value::SortableValue, vrl_utils::partial_cmp_values};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Count,
    Sum(/*field=*/ String),
    Min(/*field=*/ String),
    Max(/*field=*/ String),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Summarize {
    pub aggs: HashMap<String, Aggregation>,
    pub by: Vec<String>,
}

/// An on-going aggregation (including the needed state to compute the next value of the
/// aggregation).
trait Aggregate: Send + Sync {
    fn input(&self, log: &Log);
    fn value(&self) -> Value;
}

#[derive(Default)]
struct Count(AtomicI64);

impl Aggregate for Count {
    fn input(&self, _: &Log) {
        self.0.fetch_add(1, atomic::Ordering::Relaxed);
    }

    fn value(&self) -> Value {
        Value::Integer(self.0.load(atomic::Ordering::Relaxed))
    }
}

struct Sum {
    field: KeyString,
    value: AtomicF64,
}

impl Sum {
    fn new(field: KeyString) -> Self {
        Self {
            field,
            value: AtomicF64::new(0.0),
        }
    }
}

impl Aggregate for Sum {
    fn input(&self, log: &Log) {
        let Some(value) = log.get(&self.field) else {
            return;
        };
        match value {
            Value::Float(v) => {
                self.value.fetch_add(**v, atomic::Ordering::Relaxed);
            }
            Value::Integer(v) => {
                self.value.fetch_add(*v as f64, atomic::Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn value(&self) -> Value {
        Value::Float(
            NotNan::new(self.value.load(atomic::Ordering::Relaxed))
                .expect("aggregated sum float value to not be NaN"),
        )
    }
}

struct MinMax {
    /// The field in the logs to aggregate.
    field: KeyString,

    /// The current min / max value.
    value: Mutex<Option<Value>>,

    /// Greater for Min, Less for Max.
    update_order: Ordering,
}

impl MinMax {
    fn new_min(field: KeyString) -> Self {
        Self {
            field,
            value: Mutex::new(None),
            update_order: Ordering::Greater,
        }
    }

    fn new_max(field: KeyString) -> Self {
        Self {
            field,
            value: Mutex::new(None),
            update_order: Ordering::Less,
        }
    }
}

impl Aggregate for MinMax {
    fn input(&self, log: &Log) {
        let Some(value) = log.get(&self.field) else {
            return;
        };

        let mut guard = self.value.lock();
        let Some(stored_value) = guard.as_ref() else {
            *guard = Some(value.clone());
            return;
        };

        let Some(result) = partial_cmp_values(stored_value, value) else {
            return;
        };

        if result == self.update_order {
            *guard = Some(value.clone());
        }
    }

    fn value(&self) -> Value {
        let guard = self.value.lock();
        guard.clone().unwrap_or(Value::Null)
    }
}

fn create_aggregate(aggregation: Aggregation) -> Arc<dyn Aggregate> {
    match aggregation {
        Aggregation::Count => Arc::new(Count::default()),
        Aggregation::Sum(field) => Arc::new(Sum::new(field.into())),
        Aggregation::Min(field) => Arc::new(MinMax::new_min(field.into())),
        Aggregation::Max(field) => Arc::new(MinMax::new_max(field.into())),
    }
}

async fn summarize_all(
    mut input_stream: LogStream,
    output_fields: Vec<KeyString>,
    aggregations: Vec<Aggregation>,
) -> Result<Vec<Log>> {
    let aggregates: Vec<Arc<dyn Aggregate>> =
        aggregations.into_iter().map(create_aggregate).collect();

    while let Some(log) = input_stream.next().await {
        for aggregate in &aggregates {
            aggregate.input(&log);
        }
    }

    let mut log = Log::new();
    for (output_field, aggregate) in output_fields.clone().into_iter().zip(aggregates) {
        log.insert(output_field, aggregate.value());
    }

    Ok(vec![log])
}

async fn summarize_group_by(
    group_by: Vec<String>,
    mut input_stream: LogStream,
    output_fields: Vec<KeyString>,
    aggregations: Vec<Aggregation>,
) -> Result<Vec<Log>> {
    let by: Vec<KeyString> = group_by.into_iter().map(|x| x.into()).collect();

    // All of HashMap, HashSet, BTreeMap and BtreeSet rely on either the hash or the order of keys
    // be unchanging, so having types with interior mutability is a bad idea.
    // We don't mutate the key, so we ignore the lint error here.
    #[allow(clippy::mutable_key_type)]
    let mut group_aggregates: BTreeMap<Vec<SortableValue>, Vec<Arc<dyn Aggregate>>> =
        BTreeMap::new();

    let mut tracked_types = vec![None; by.len()];

    while let Some(log) = input_stream.next().await {
        let mut group_keys = Vec::with_capacity(by.len());

        for (tracked_type, key) in tracked_types.iter_mut().zip(&by) {
            let value = log.get(key).unwrap_or_else(|| &Value::Null);
            if value == &Value::Null {
                group_keys.push(SortableValue(value.clone()));
                continue;
            }

            let value_type = std::mem::discriminant(value);
            if let Some(t) = tracked_type {
                if *t != value_type {
                    bail!(
                        "cannot summarize over differing types (key '{}'): {:?} != {:?}",
                        key,
                        *t,
                        value_type
                    );
                }
            } else {
                *tracked_type = Some(value_type);
            }

            group_keys.push(SortableValue(value.clone()));
        }

        let entry = group_aggregates
            .entry(group_keys)
            .or_insert_with(|| aggregations.iter().cloned().map(create_aggregate).collect());

        for aggregate in entry {
            aggregate.input(&log);
        }
    }

    let mut logs = Vec::with_capacity(group_aggregates.len());
    for (group_keys, aggregates) in group_aggregates {
        let mut log = Log::new();

        for (key, value) in by.clone().into_iter().zip(group_keys) {
            log.insert(key, value.0);
        }

        for (output_field, aggregate) in output_fields.clone().into_iter().zip(aggregates) {
            log.insert(output_field, aggregate.value());
        }

        logs.push(log);
    }

    Ok(logs)
}

pub async fn summarize_stream(config: Summarize, input_stream: LogStream) -> Result<Vec<Log>> {
    info!("{config:?}");

    let (output_fields, aggregations): (Vec<KeyString>, Vec<Aggregation>) = config
        .aggs
        .into_iter()
        .map(|(field, agg)| (field.into(), agg))
        .unzip();

    if config.by.is_empty() {
        summarize_all(input_stream, output_fields, aggregations).await
    } else {
        summarize_group_by(config.by, input_stream, output_fields, aggregations).await
    }
}
