use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
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
use serde_json::Value;
use tracing::info;

use crate::log::{Log, LogStream};

use super::{serde_json_utils::partial_cmp_values, sortable_value::SortableValue};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Count,
    Sum(/*field=*/ String),
    Min(/*field=*/ String),
    Max(/*field=*/ String),
}

impl fmt::Display for Aggregation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Aggregation::Count => write!(f, "Count"),
            Aggregation::Sum(x) => write!(f, "Sum({})", x),
            Aggregation::Min(x) => write!(f, "Min({})", x),
            Aggregation::Max(x) => write!(f, "Max({})", x),
        }
    }
}

impl Aggregation {
    #[must_use]
    fn field(&self) -> Option<String> {
        match self {
            Self::Count => None,
            Self::Sum(field) | Self::Min(field) | Self::Max(field) => Some(field.clone()),
        }
    }

    #[must_use]
    pub fn map_into_combine_agg(self, field: String) -> Self {
        match self {
            Self::Count => Self::Sum(field),
            Self::Sum(..) => Self::Sum(field),
            Self::Min(..) => Self::Min(field),
            Self::Max(..) => Self::Max(field),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub enum SummarizeType {
    #[default]
    Single,
    Final,
}

impl fmt::Display for SummarizeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SummarizeType::Single => write!(f, "Single"),
            SummarizeType::Final => write!(f, "Final"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Summarize {
    pub aggs: BTreeMap<String, Aggregation>,
    pub by: Vec<String>,

    #[serde(rename = "type", skip_deserializing, default)]
    pub type_: SummarizeType,
}

impl fmt::Display for Summarize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "type={}, by=[", self.type_)?;
        for (i, by) in self.by.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", by)?;
        }
        write!(f, "], aggs=[")?;
        for (i, (key, agg)) in self.aggs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}={}", key, agg)?;
        }
        write!(f, "]")
    }
}

impl Summarize {
    pub fn convert_to_final(self) -> Self {
        let mut aggs = BTreeMap::new();
        for (field, agg) in self.aggs {
            aggs.insert(field.clone(), agg.map_into_combine_agg(field));
        }
        Self {
            aggs,
            by: self.by,
            type_: SummarizeType::Final,
        }
    }

    pub fn is_final(&self) -> bool {
        matches!(self.type_, SummarizeType::Final)
    }
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
        Value::from(self.0.load(atomic::Ordering::Relaxed))
    }
}

struct Sum {
    field: String,
    value: AtomicF64,
}

impl Sum {
    fn new(field: String) -> Self {
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
        let Value::Number(v) = value else {
            return;
        };

        let Some(x) = v.as_f64() else {
            panic!("'{v}' number is not a i64 or a f64");
        };

        self.value.fetch_add(x, atomic::Ordering::Relaxed);
    }

    fn value(&self) -> Value {
        Value::from(self.value.load(atomic::Ordering::Relaxed))
    }
}

struct MinMax {
    /// The field in the logs to aggregate.
    field: String,

    /// The current min / max value.
    value: Mutex<Option<Value>>,

    /// Greater for Min, Less for Max.
    update_order: Ordering,
}

impl MinMax {
    fn new_min(field: String) -> Self {
        Self {
            field,
            value: Mutex::new(None),
            update_order: Ordering::Greater,
        }
    }

    fn new_max(field: String) -> Self {
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
        Aggregation::Sum(field) => Arc::new(Sum::new(field)),
        Aggregation::Min(field) => Arc::new(MinMax::new_min(field)),
        Aggregation::Max(field) => Arc::new(MinMax::new_max(field)),
    }
}

async fn summarize_all(
    mut input_stream: LogStream,
    output_fields: Vec<String>,
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
    output_fields: Vec<String>,
    aggregations: Vec<Aggregation>,
) -> Result<Vec<Log>> {
    let agg_fields: BTreeSet<String> = aggregations.iter().flat_map(Aggregation::field).collect();
    let get_value_fn = if group_by.iter().any(|x| agg_fields.contains(x)) {
        |log: &mut Log, key: &String| log.get(key).cloned().unwrap_or(Value::Null)
    } else {
        // Optimization: remove item from map instead of cloning when no aggregation references
        // a field that is grouped by.
        |log: &mut Log, key: &String| log.remove(key).unwrap_or(Value::Null)
    };

    // All of HashMap, HashSet, BTreeMap and BtreeSet rely on either the hash or the order of keys
    // be unchanging, so having types with interior mutability is a bad idea.
    // We don't mutate the key, so we ignore the lint error here.
    #[allow(clippy::mutable_key_type)]
    let mut group_aggregates: BTreeMap<Vec<SortableValue>, Vec<Arc<dyn Aggregate>>> =
        BTreeMap::new();

    let mut tracked_types = vec![None; group_by.len()];

    while let Some(mut log) = input_stream.next().await {
        let mut group_keys = Vec::with_capacity(group_by.len());

        for (tracked_type, key) in tracked_types.iter_mut().zip(&group_by) {
            let value = get_value_fn(&mut log, key);
            if value == Value::Null {
                group_keys.push(SortableValue(value));
                continue;
            }

            let value_type = std::mem::discriminant(&value);
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

            group_keys.push(SortableValue(value));
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

        for (key, value) in group_by.clone().into_iter().zip(group_keys) {
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

    let (output_fields, aggregations): (Vec<String>, Vec<Aggregation>) =
        config.aggs.into_iter().unzip();

    if config.by.is_empty() {
        summarize_all(input_stream, output_fields, aggregations).await
    } else {
        summarize_group_by(config.by, input_stream, output_fields, aggregations).await
    }
}
