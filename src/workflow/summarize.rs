use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt, iter,
    mem::Discriminant,
};

use color_eyre::eyre::eyre;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    log::{Log, LogItem, LogIter},
    try_next,
};

use super::{
    partial_stream::PartialLogIter, serde_json_utils::partial_cmp_values,
    sortable_value::SortableValue,
};

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
            Aggregation::Sum(x) => write!(f, "Sum({x})"),
            Aggregation::Min(x) => write!(f, "Min({x})"),
            Aggregation::Max(x) => write!(f, "Max({x})"),
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
    pub fn convert_to_mux(self, field: String) -> Self {
        match self {
            Self::Count => Self::Sum(field),
            Self::Sum(..) => Self::Sum(field),
            Self::Min(..) => Self::Min(field),
            Self::Max(..) => Self::Max(field),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Summarize {
    pub aggs: BTreeMap<String, Aggregation>,
    pub by: Vec<String>,
}

impl fmt::Display for Summarize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "by=[")?;
        for (i, by) in self.by.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{by}")?;
        }
        write!(f, "], aggs=[")?;
        for (i, (key, agg)) in self.aggs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{key}={agg}")?;
        }
        write!(f, "]")
    }
}

impl Summarize {
    pub fn convert_to_mux(self) -> Self {
        let mut aggs = BTreeMap::new();
        for (field, agg) in self.aggs {
            aggs.insert(field.clone(), agg.convert_to_mux(field));
        }
        Self { aggs, by: self.by }
    }
}

/// An on-going aggregation (including the needed state to compute the next value of the
/// aggregation).
trait Aggregate {
    fn input(&mut self, log: &Log);
    fn value(&self) -> Value;
}

#[derive(Default)]
struct Count(u64);

impl Aggregate for Count {
    fn input(&mut self, _: &Log) {
        self.0 += 1;
    }

    fn value(&self) -> Value {
        Value::from(self.0)
    }
}

struct Sum {
    field: String,
    value: f64,
}

impl Sum {
    fn new(field: String) -> Self {
        Self { field, value: 0.0 }
    }
}

impl Aggregate for Sum {
    fn input(&mut self, log: &Log) {
        let Some(value) = log.get(&self.field) else {
            return;
        };
        let Value::Number(v) = value else {
            return;
        };

        let Some(x) = v.as_f64() else {
            panic!("'{v}' number is not a i64 or a f64");
        };

        self.value += x;
    }

    fn value(&self) -> Value {
        Value::from(self.value)
    }
}

struct MinMax {
    /// The field in the logs to aggregate.
    field: String,

    /// The current min / max value.
    value: Option<Value>,

    /// Greater for Min, Less for Max.
    update_order: Ordering,
}

impl MinMax {
    fn new(field: String, update_order: Ordering) -> Self {
        Self {
            field,
            value: None,
            update_order,
        }
    }

    fn new_min(field: String) -> Self {
        Self::new(field, Ordering::Greater)
    }

    fn new_max(field: String) -> Self {
        Self::new(field, Ordering::Less)
    }
}

impl Aggregate for MinMax {
    fn input(&mut self, log: &Log) {
        let Some(value) = log.get(&self.field) else {
            return;
        };

        let Some(stored_value) = &self.value else {
            self.value = Some(value.clone());
            return;
        };

        let Some(result) = partial_cmp_values(stored_value, value) else {
            return;
        };

        if result == self.update_order {
            self.value = Some(value.clone());
        }
    }

    fn value(&self) -> Value {
        self.value.clone().unwrap_or(Value::Null)
    }
}

fn create_aggregate(aggregation: Aggregation) -> Box<dyn Aggregate> {
    match aggregation {
        Aggregation::Count => Box::new(Count::default()),
        Aggregation::Sum(field) => Box::new(Sum::new(field)),
        Aggregation::Min(field) => Box::new(MinMax::new_min(field)),
        Aggregation::Max(field) => Box::new(MinMax::new_max(field)),
    }
}

/// Executes summarize without grouping, only aggregations.
pub struct SummarizeAllIter {
    input: LogIter,
    output_fields: Vec<String>,
    aggregates: Vec<Box<dyn Aggregate>>,
    done: bool,
}

impl SummarizeAllIter {
    fn new(input: LogIter, output_fields: Vec<String>, aggregations: Vec<Aggregation>) -> Self {
        let aggregates: Vec<Box<dyn Aggregate>> =
            aggregations.into_iter().map(create_aggregate).collect();
        Self {
            input,
            output_fields,
            aggregates,
            done: false,
        }
    }
}

impl Iterator for SummarizeAllIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        while let Some(log) = try_next!(self.input) {
            for aggregate in &mut self.aggregates {
                aggregate.input(&log);
            }
        }

        self.done = true;

        // Just 1 item.
        self.get_partial().next()
    }
}

impl PartialLogIter for SummarizeAllIter {
    fn get_partial(&self) -> LogIter {
        let mut log = Log::new();
        for (output_field, aggregate) in self
            .output_fields
            .clone()
            .into_iter()
            .zip(self.aggregates.iter())
        {
            log.insert(output_field, aggregate.value());
        }
        Box::new(iter::once(LogItem::Log(log)))
    }
}

type GroupAggregates = BTreeMap<Vec<SortableValue>, Vec<Box<dyn Aggregate>>>;

/// Executes summarize with some aggregations.
pub struct SummarizeGroupByIter {
    input: LogIter,
    group_by: Vec<String>,
    output_fields: Vec<String>,
    aggregations: Vec<Aggregation>,
    get_value_fn: fn(&mut Log, &String) -> Value,
    tracked_types: Vec<Option<Discriminant<Value>>>,

    // All of HashMap, HashSet, BTreeMap and BtreeSet rely on either the hash or the order of keys
    // be unchanging, so having types with interior mutability is a bad idea.
    // We don't mutate the key, so we ignore the lint error here.
    #[allow(clippy::mutable_key_type)]
    group_aggregates: GroupAggregates,

    output: LogIter,
    done: bool,
}

impl SummarizeGroupByIter {
    fn new(
        input: LogIter,
        group_by: Vec<String>,
        output_fields: Vec<String>,
        aggregations: Vec<Aggregation>,
    ) -> Self {
        let agg_fields: BTreeSet<String> =
            aggregations.iter().flat_map(Aggregation::field).collect();

        let get_value_fn = if group_by.iter().any(|x| agg_fields.contains(x)) {
            |log: &mut Log, key: &String| log.get(key).cloned().unwrap_or(Value::Null)
        } else {
            // Optimization: remove item from map instead of cloning when no aggregation references
            // a field that is grouped by.
            |log: &mut Log, key: &String| log.remove(key).unwrap_or(Value::Null)
        };

        let tracked_types = vec![None; group_by.len()];

        Self {
            input,
            group_by,
            output_fields,
            aggregations,
            get_value_fn,
            tracked_types,
            group_aggregates: GroupAggregates::new(),
            output: Box::new(iter::empty()),
            done: false,
        }
    }
}

impl PartialLogIter for SummarizeGroupByIter {
    fn get_partial(&self) -> LogIter {
        let mut logs = Vec::with_capacity(self.group_aggregates.len());

        for (group_keys, aggregates) in &self.group_aggregates {
            let mut log = Log::new();

            for (key, value) in self.group_by.clone().into_iter().zip(group_keys) {
                log.insert(key, value.0.clone());
            }

            for (output_field, aggregate) in self
                .output_fields
                .clone()
                .into_iter()
                .zip(aggregates.iter())
            {
                log.insert(output_field, aggregate.value());
            }

            logs.push(log);
        }

        Box::new(logs.into_iter().map(LogItem::Log))
    }
}

impl Iterator for SummarizeGroupByIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mut log) = try_next!(self.input) {
            let mut group_keys = Vec::with_capacity(self.group_by.len());

            for (tracked_type, key) in self.tracked_types.iter_mut().zip(&self.group_by) {
                let value = (self.get_value_fn)(&mut log, key);
                if value == Value::Null {
                    group_keys.push(SortableValue(value));
                    continue;
                }

                let value_type = std::mem::discriminant(&value);
                if let Some(t) = tracked_type {
                    if *t != value_type {
                        return Some(LogItem::Err(eyre!(
                            "cannot summarize over differing types (key '{}'): {:?} != {:?}",
                            key,
                            *t,
                            value_type
                        )));
                    }
                } else {
                    *tracked_type = Some(value_type);
                }

                group_keys.push(SortableValue(value));
            }

            let entry = self.group_aggregates.entry(group_keys).or_insert_with(|| {
                self.aggregations
                    .iter()
                    .cloned()
                    .map(create_aggregate)
                    .collect()
            });

            for aggregate in entry {
                aggregate.input(&log);
            }
        }

        if let Some(item) = self.output.next() {
            return Some(item);
        }
        if self.done {
            return None;
        }
        self.done = true;
        self.output = self.get_partial();
        self.output.next()
    }
}

pub fn create_summarize_iter(input: LogIter, config: Summarize) -> Box<dyn PartialLogIter> {
    let (output_fields, aggregations): (Vec<String>, Vec<Aggregation>) =
        config.aggs.into_iter().unzip();

    if config.by.is_empty() {
        Box::new(SummarizeAllIter::new(input, output_fields, aggregations))
    } else {
        Box::new(SummarizeGroupByIter::new(
            input,
            config.by,
            output_fields,
            aggregations,
        ))
    }
}
