use std::{borrow::Cow, cmp::Ordering, iter, mem::Discriminant};

use color_eyre::eyre::eyre;
use hashbrown::{HashMap, HashSet};
use miso_workflow_types::{
    expr::Expr,
    field::Field,
    log::{Log, LogItem, LogIter},
    summarize::{Aggregation, MUX_AVG_COUNT_SUFFIX, MUX_AVG_SUM_SUFFIX, Summarize},
    value::Value,
};
use tracing::warn;

use crate::interpreter::{LogInterpreter, Val, get_field_value, insert_field_value};

use super::{partial_stream::PartialLogIter, try_next};

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
    field: Field,
    seen: HashSet<Value>,
}

impl DCount {
    fn new(field: Field) -> Self {
        Self {
            field,
            seen: HashSet::new(),
        }
    }
}

impl Aggregate for DCount {
    fn input(&mut self, log: &Log) {
        let Some(value) = get_field_value(log, &self.field) else {
            return;
        };
        self.seen.insert(value.clone());
    }

    fn value(&self) -> Value {
        Value::from(self.seen.len())
    }
}

struct Sum {
    field: Field,
    value: f64,
}

impl Sum {
    fn new(field: Field) -> Self {
        Self { field, value: 0.0 }
    }
}

impl Aggregate for Sum {
    fn input(&mut self, log: &Log) {
        let Some(value) = get_field_value(log, &self.field) else {
            return;
        };
        let Some(x) = value.as_f64() else {
            return;
        };
        self.value += x;
    }

    fn value(&self) -> Value {
        Value::from(self.value)
    }
}

struct Avg {
    field: Field,
    value: f64,
    count: u64,
}

impl Avg {
    fn new(field: Field) -> Self {
        Self {
            field,
            value: 0.0,
            count: 0,
        }
    }
}

impl Aggregate for Avg {
    fn input(&mut self, log: &Log) {
        let Some(value) = get_field_value(log, &self.field) else {
            return;
        };
        let Some(x) = value.as_f64() else {
            return;
        };
        self.value += x;
        self.count += 1;
    }

    fn value(&self) -> Value {
        Value::from(self.value / self.count as f64)
    }
}

struct MuxAvg {
    sum_field: Field,
    count_field: Field,
    value: f64,
    count: f64,
}

impl MuxAvg {
    fn new(field: Field) -> Self {
        Self {
            sum_field: field.clone().with_suffix(MUX_AVG_SUM_SUFFIX),
            count_field: field.with_suffix(MUX_AVG_COUNT_SUFFIX),
            value: 0.0,
            count: 0.0,
        }
    }
}

impl Aggregate for MuxAvg {
    fn input(&mut self, log: &Log) {
        let Some(sum_value) = get_field_value(log, &self.sum_field) else {
            return;
        };
        let Some(count_value) = get_field_value(log, &self.count_field) else {
            return;
        };
        let (Some(sum), Some(count)) = (sum_value.as_f64(), count_value.as_f64()) else {
            return;
        };

        if count <= 0.0 {
            return;
        }

        let new_count = self.count + count;
        let new_avg = self.value + (sum / count - self.value) * (count / new_count);

        self.value = new_avg;
        self.count = new_count;
    }

    fn value(&self) -> Value {
        Value::from(self.value)
    }
}

struct MinMax {
    /// The field in the logs to aggregate.
    field: Field,

    /// The current min / max value.
    value: Option<Value>,

    /// Greater for Min, Less for Max.
    update_order: Ordering,
}

impl MinMax {
    fn new(field: Field, update_order: Ordering) -> Self {
        Self {
            field,
            value: None,
            update_order,
        }
    }

    fn new_min(field: Field) -> Self {
        Self::new(field, Ordering::Greater)
    }

    fn new_max(field: Field) -> Self {
        Self::new(field, Ordering::Less)
    }
}

impl Aggregate for MinMax {
    fn input(&mut self, log: &Log) {
        let Some(value) = get_field_value(log, &self.field) else {
            return;
        };

        let Some(stored_value) = &self.value else {
            self.value = Some(value.clone());
            return;
        };

        let result = stored_value.cmp(value);
        if result == self.update_order {
            self.value = Some(value.clone());
        }
    }

    fn value(&self) -> Value {
        self.value.clone().unwrap_or(Value::Null)
    }
}

fn create_aggregate(aggregation: Aggregation, is_mux: bool) -> Box<dyn Aggregate> {
    match aggregation {
        Aggregation::Count => Box::new(Count::default()),
        Aggregation::DCount(field) => Box::new(DCount::new(field)),
        Aggregation::Sum(field) => Box::new(Sum::new(field)),
        Aggregation::Avg(field) if is_mux => Box::new(MuxAvg::new(field)),
        Aggregation::Avg(field) => Box::new(Avg::new(field)),
        Aggregation::Min(field) => Box::new(MinMax::new_min(field)),
        Aggregation::Max(field) => Box::new(MinMax::new_max(field)),
    }
}

/// Executes summarize without grouping, only aggregations.
pub struct SummarizeAllIter {
    input: LogIter,
    output_fields: Vec<Field>,
    aggregates: Vec<Box<dyn Aggregate>>,
    done: bool,
}

impl SummarizeAllIter {
    fn new(
        input: LogIter,
        output_fields: Vec<Field>,
        aggregations: Vec<Aggregation>,
        is_mux: bool,
    ) -> Self {
        let aggregates: Vec<Box<dyn Aggregate>> = aggregations
            .into_iter()
            .map(|a| create_aggregate(a, is_mux))
            .collect();
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
        for (output_field, aggregate) in self.output_fields.iter().zip(self.aggregates.iter()) {
            insert_field_value(&mut log, output_field, aggregate.value());
        }
        Box::new(iter::once(LogItem::Log(log)))
    }
}

type GroupAggregates = HashMap<Vec<Value>, Vec<Box<dyn Aggregate>>>;

/// Executes summarize with some aggregations.
pub struct SummarizeGroupByIter {
    input: LogIter,
    group_by: Vec<Expr>,
    output_fields: Vec<Field>,
    aggregations: Vec<Aggregation>,
    is_mux: bool,
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
        group_by: Vec<Expr>,
        output_fields: Vec<Field>,
        aggregations: Vec<Aggregation>,
        is_mux: bool,
    ) -> Self {
        let tracked_types = vec![None; group_by.len()];
        Self {
            input,
            group_by,
            output_fields,
            aggregations,
            is_mux,
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
                let field = match ast {
                    Expr::Field(field) => field,
                    Expr::Bin(lhs, _) => {
                        if let Expr::Field(field) = &**lhs {
                            field
                        } else {
                            continue;
                        }
                    }
                    _ => continue,
                };
                insert_field_value(&mut log, field, value);
            }

            for (output_field, aggregate) in self.output_fields.iter().zip(aggregates) {
                insert_field_value(&mut log, output_field, aggregate.value());
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
            let interpreter = LogInterpreter { log: &log };
            let mut group_keys = Vec::with_capacity(self.group_by.len());

            for (tracked_type, expr) in self.tracked_types.iter_mut().zip(&self.group_by) {
                let value_cow = match interpreter.eval(expr) {
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
                            expr,
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
                    .map(|a| create_aggregate(a, self.is_mux))
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

pub fn create_summarize_iter(
    input: LogIter,
    config: Summarize,
    is_mux: bool,
) -> Box<dyn PartialLogIter> {
    let (output_fields, aggregations): (Vec<Field>, Vec<Aggregation>) =
        config.aggs.into_iter().unzip();

    if config.by.is_empty() {
        Box::new(SummarizeAllIter::new(
            input,
            output_fields,
            aggregations,
            is_mux,
        ))
    } else {
        Box::new(SummarizeGroupByIter::new(
            input,
            config.by,
            output_fields,
            aggregations,
            is_mux,
        ))
    }
}
