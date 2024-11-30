use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{self, AtomicI64},
        Arc,
    },
};

use futures_util::StreamExt;

use color_eyre::Result;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::info;
use vrl::{core::Value, value::KeyString};

use crate::log::{Log, LogStream};

use super::vrl_utils::partial_cmp_values;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Count,
    Min(/*field=*/ String),
    Max(/*field=*/ String),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

struct Sortable(Value);

impl Ord for Sortable {
    fn cmp(&self, other: &Self) -> Ordering {
        partial_cmp_values(&self.0, &other.0).unwrap_or(Ordering::Less)
    }
}

impl PartialOrd for Sortable {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Sortable {}

impl PartialEq for Sortable {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

fn create_aggregate(aggregation: &Aggregation) -> Arc<dyn Aggregate> {
    match aggregation {
        Aggregation::Count => Arc::new(Count::default()),
        Aggregation::Min(field) => Arc::new(MinMax::new_min(field.clone().into())),
        Aggregation::Max(field) => Arc::new(MinMax::new_max(field.clone().into())),
    }
}

pub async fn summarize_stream(config: Summarize, mut input_stream: LogStream) -> Result<Vec<Log>> {
    info!("{config:?}");

    let (output_fields, aggregations): (Vec<KeyString>, Vec<_>) = config
        .aggs
        .into_iter()
        .map(|(field, agg)| (field.into(), agg))
        .unzip();

    let by: Vec<KeyString> = config.by.into_iter().map(|x| x.into()).collect();

    // Will be fixed once vrl is removed (value will be immutable).
    #[allow(clippy::mutable_key_type)]
    let mut group_aggregates: BTreeMap<Vec<Sortable>, Vec<Arc<dyn Aggregate>>> = BTreeMap::new();

    // TODO: validate "by" types are all the same.

    while let Some(log) = input_stream.next().await {
        let group_keys: Vec<Sortable> = by
            .iter()
            .map(|x| log.get(x).unwrap_or_else(|| &Value::Null))
            .cloned()
            .map(Sortable)
            .collect();

        let entry = group_aggregates
            .entry(group_keys)
            .or_insert_with(|| aggregations.iter().map(create_aggregate).collect());

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
