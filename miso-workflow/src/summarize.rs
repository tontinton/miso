use std::{borrow::Cow, cmp::Ordering, iter, mem::Discriminant};

use color_eyre::{Result, eyre::eyre};
use hashbrown::{HashMap, HashSet};
use miso_workflow_types::{
    log::{Log, LogItem, LogIter},
    summarize::{Aggregation, GroupAst, Summarize},
};
use serde_json::Value;
use tracing::warn;

use super::{
    interpreter::{Val, ident},
    partial_stream::PartialLogIter,
    serde_json_utils::partial_cmp_values,
    try_next,
};

struct AggGroupInterpreter<'a> {
    log: &'a Log,
}

impl<'a> AggGroupInterpreter<'a> {
    fn eval(&self, ast: &'a GroupAst) -> Result<Val<'a>> {
        Ok(match ast {
            GroupAst::Id(name) => ident(self.log, name)?,
            GroupAst::Bin(name, by) => ident(self.log, name)?.bin(&Val::borrowed(by))?.into(),
        })
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

struct DCount {
    field: String,
    seen: HashSet<Value>,
}

impl DCount {
    fn new(field: String) -> Self {
        Self {
            field,
            seen: HashSet::new(),
        }
    }
}

impl Aggregate for DCount {
    fn input(&mut self, log: &Log) {
        let Some(value) = log.get(&self.field) else {
            return;
        };
        self.seen.insert(value.clone());
    }

    fn value(&self) -> Value {
        Value::from(self.seen.len())
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
        Aggregation::DCount(field) => Box::new(DCount::new(field)),
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

type GroupAggregates = HashMap<Vec<Value>, Vec<Box<dyn Aggregate>>>;

/// Executes summarize with some aggregations.
pub struct SummarizeGroupByIter {
    input: LogIter,
    group_by: Vec<GroupAst>,
    output_fields: Vec<String>,
    aggregations: Vec<Aggregation>,
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
        group_by: Vec<GroupAst>,
        output_fields: Vec<String>,
        aggregations: Vec<Aggregation>,
    ) -> Self {
        let tracked_types = vec![None; group_by.len()];
        Self {
            input,
            group_by,
            output_fields,
            aggregations,
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

            for (ast, value) in self.group_by.iter().zip(group_keys.clone()) {
                log.insert(ast.field().to_string(), value);
            }

            for (output_field, aggregate) in self.output_fields.clone().into_iter().zip(aggregates)
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
        'next_log: while let Some(log) = try_next!(self.input) {
            let interpreter = AggGroupInterpreter { log: &log };
            let mut group_keys = Vec::with_capacity(self.group_by.len());

            for (tracked_type, ast) in self.tracked_types.iter_mut().zip(&self.group_by) {
                let value_cow = match interpreter.eval(ast) {
                    Ok(Val(None)) => Cow::Owned(Value::Null),
                    Ok(v) => v.0.unwrap(),
                    Err(e) => {
                        warn!("Aggregation group by evaluation failed: {e}");
                        Cow::Owned(Value::Null)
                    }
                };

                if value_cow.as_ref() == &Value::Null {
                    continue 'next_log;
                }
                let value = value_cow.into_owned();

                let value_type = std::mem::discriminant(&value);
                if let Some(t) = tracked_type {
                    if *t != value_type {
                        return Some(LogItem::Err(eyre!(
                            "cannot summarize over differing types (key '{}'): {:?} != {:?}",
                            ast,
                            *t,
                            value_type
                        )));
                    }
                } else {
                    *tracked_type = Some(value_type);
                }

                group_keys.push(value);
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
