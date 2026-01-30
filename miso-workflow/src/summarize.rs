//! Aggregates logs with count, sum, avg, min, max, dcount. Supports grouping by fields.
//!
//! Partial streams are accumulated separately, then merged when emitting. Memory is
//! tracked per-group - when the limit is hit, partial streams are dropped first.

use std::{cmp::Ordering, iter};

use bytesize::ByteSize;
use color_eyre::eyre::Result;
use hashbrown::{HashMap, HashSet};
use miso_common::metrics::{ERROR_EVAL, METRICS, STEP_SUMMARIZE};
use miso_workflow_types::{
    expr::Expr,
    field::Field,
    log::{Log, LogItem, LogIter, PartialStreamKey},
    summarize::{Aggregation, MUX_AVG_COUNT_SUFFIX, MUX_AVG_SUM_SUFFIX, Summarize},
    value::Value,
};
use thiserror::Error;
use tracing::warn;

use crate::interpreter::{LogInterpreter, Val, get_field_value, insert_field_value};
use crate::memory_size::MemorySize;
use crate::type_tracker::TypeTracker;

use super::{
    log_utils::PartialStreamItem,
    partial_stream::PartialLogIter,
    partial_stream_tracker::{MemoryError, Mergeable, PartialStreamTracker},
    try_next_with_partial_stream,
};

macro_rules! same_variant {
    ($other:expr, $pat:pat => $body:block) => {
        if let $pat = $other {
            $body
        } else {
            unreachable!("merge called with mismatched AggregateState")
        }
    };
}

#[derive(Debug, Error)]
pub enum SummarizeError {
    #[error(
        "Summarize operation exceeded memory limit: estimated {estimated} memory usage but limit is {limit}. Consider adding filters to reduce the dataset size or reducing the number of group by fields."
    )]
    MemoryLimitExceeded {
        estimated: ByteSize,
        limit: ByteSize,
    },
}

impl From<MemoryError> for SummarizeError {
    fn from(e: MemoryError) -> Self {
        match e {
            MemoryError::MemoryLimitExceeded { estimated, limit } => {
                Self::MemoryLimitExceeded { estimated, limit }
            }
        }
    }
}

pub fn create_summarize_iter(
    input: LogIter,
    config: Summarize,
    is_mux: bool,
    memory_limit: u64,
) -> Box<dyn PartialLogIter> {
    let (output_fields, aggregations): (Vec<Field>, Vec<Aggregation>) =
        config.aggs.into_iter().unzip();

    Box::new(SummarizeIter::new(
        input,
        config.by,
        output_fields,
        aggregations,
        is_mux,
        memory_limit,
    ))
}

pub struct SummarizeIter {
    input: LogIter,
    group_by: Vec<Expr>,
    output_fields: Vec<Field>,
    aggregations: Vec<Aggregation>,
    is_mux: bool,
    type_tracker: TypeTracker,
    tracker: PartialStreamTracker<SummarizeState>,
    output: LogIter,
    pending: Vec<LogItem>,
    done: bool,
    rows_processed: u64,
}

impl SummarizeIter {
    fn new(
        input: LogIter,
        group_by: Vec<Expr>,
        output_fields: Vec<Field>,
        aggregations: Vec<Aggregation>,
        is_mux: bool,
        memory_limit: u64,
    ) -> Self {
        let initial = if group_by.is_empty() {
            SummarizeState::All(create_aggregates(&aggregations, is_mux))
        } else {
            SummarizeState::GroupBy(HashMap::new())
        };

        let initial_memory = initial.estimate_memory_size() as u64;
        let mut tracker = PartialStreamTracker::with_memory_limit(initial, memory_limit);
        tracker
            .update_final_with_memory(|_| initial_memory)
            .unwrap();

        Self {
            type_tracker: TypeTracker::new(group_by.len()),
            input,
            group_by,
            output_fields,
            aggregations,
            is_mux,
            tracker,
            output: Box::new(iter::empty()),
            pending: Vec::new(),
            done: false,
            rows_processed: 0,
        }
    }

    fn extract_group_keys(&mut self, log: &Log) -> Option<Result<Vec<Value>, LogItem>> {
        let interpreter = LogInterpreter { log };
        let mut keys = Vec::with_capacity(self.group_by.len());

        for (i, expr) in self.group_by.iter().enumerate() {
            let value = match interpreter.eval(expr) {
                Ok(Val(None)) => Value::Null,
                Ok(v) => v.0.unwrap().into_owned(),
                Err(e) => {
                    METRICS
                        .workflow_step_errors
                        .with_label_values(&[STEP_SUMMARIZE, ERROR_EVAL])
                        .inc();
                    warn!("Aggregation group by evaluation failed: {e}");
                    Value::Null
                }
            };

            if value == Value::Null {
                return None;
            }
            if let Err(e) = self.type_tracker.check(i, &value, expr) {
                return Some(Err(LogItem::Err(e)));
            }
            keys.push(value);
        }

        Some(Ok(keys))
    }

    fn process_log(&mut self, log: &Log) -> Option<LogItem> {
        if self.group_by.is_empty() {
            self.tracker
                .update_final_with_memory(|s| s.input_all(log) as u64)
                .err()
                .map(|e| LogItem::Err(SummarizeError::from(e).into()))
        } else {
            let result = self.extract_group_keys(log)?;
            let keys = match result {
                Ok(k) => k,
                Err(e) => return Some(e),
            };
            self.tracker
                .update_final_with_memory(|s| {
                    s.input_grouped(keys, log, &self.aggregations, self.is_mux) as u64
                })
                .err()
                .map(|e| LogItem::Err(SummarizeError::from(e).into()))
        }
    }

    fn process_partial_log(&mut self, log: &Log, key: PartialStreamKey) -> Option<LogItem> {
        if self.group_by.is_empty() {
            self.tracker
                .update_partial_with_memory(
                    key,
                    || SummarizeState::All(create_aggregates(&self.aggregations, self.is_mux)),
                    |s| s.input_all(log) as u64,
                )
                .err()
                .map(|e| LogItem::Err(SummarizeError::from(e).into()))
        } else {
            let result = self.extract_group_keys(log)?;
            let keys = match result {
                Ok(k) => k,
                Err(e) => return Some(e),
            };
            self.tracker
                .update_partial_with_memory(
                    key,
                    || SummarizeState::GroupBy(HashMap::new()),
                    |s| s.input_grouped(keys, log, &self.aggregations, self.is_mux) as u64,
                )
                .err()
                .map(|e| LogItem::Err(SummarizeError::from(e).into()))
        }
    }

    fn state_to_logs(&self, state: &SummarizeState) -> Vec<Log> {
        state.to_logs(&self.group_by, &self.output_fields)
    }

    fn emit_logs(
        &mut self,
        logs: Vec<Log>,
        partial_key: Option<PartialStreamKey>,
    ) -> Option<LogItem> {
        self.output = match partial_key {
            None => Box::new(logs.into_iter().map(LogItem::Log)),
            Some(key) => Box::new(
                logs.into_iter()
                    .map(move |log| LogItem::PartialStreamLog(log, key))
                    .chain(iter::once(LogItem::PartialStreamDone(key))),
            ),
        };
        self.output.next()
    }
}

impl Drop for SummarizeIter {
    fn drop(&mut self) {
        METRICS
            .workflow_step_rows
            .with_label_values(&[STEP_SUMMARIZE])
            .inc_by(self.rows_processed);
    }
}

impl Iterator for SummarizeIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.output.next() {
            return Some(item);
        }
        if let Some(item) = self.pending.pop() {
            return Some(item);
        }
        if self.done {
            return None;
        }

        while let Some(item) = try_next_with_partial_stream!(self.input) {
            match item {
                PartialStreamItem::Log(log) => {
                    self.rows_processed += 1;
                    if let Some(err) = self.process_log(&log) {
                        return Some(err);
                    }
                }
                PartialStreamItem::PartialStreamLog(log, key) => {
                    self.rows_processed += 1;
                    if let Some(err) = self.process_partial_log(&log, key) {
                        return Some(err);
                    }
                }
                PartialStreamItem::PartialStreamDone(key) => {
                    if let Some((state, out_key)) = self.tracker.mark_done(key) {
                        let logs = self.state_to_logs(&state);
                        return self.emit_logs(logs, Some(out_key));
                    }
                }
                PartialStreamItem::SourceDone(source_id) => {
                    for (state, out_key) in self.tracker.finish_source(source_id) {
                        for log in self.state_to_logs(&state) {
                            self.pending.push(LogItem::PartialStreamLog(log, out_key));
                        }
                        self.pending.push(LogItem::PartialStreamDone(out_key));
                    }
                    return Some(LogItem::SourceDone(source_id));
                }
            }
        }

        self.done = true;
        let state = std::mem::replace(
            &mut self.tracker,
            PartialStreamTracker::new(SummarizeState::GroupBy(HashMap::new())),
        )
        .into_final_state();
        let logs = self.state_to_logs(&state);
        self.emit_logs(logs, None)
    }
}

impl PartialLogIter for SummarizeIter {
    fn get_partial(&self) -> LogIter {
        let logs = self.state_to_logs(self.tracker.final_state());
        Box::new(logs.into_iter().map(LogItem::Log))
    }
}

#[derive(Clone)]
enum SummarizeState {
    All(Vec<AggregateState>),
    GroupBy(HashMap<Vec<Value>, Vec<AggregateState>>),
}

impl SummarizeState {
    fn input_all(&mut self, log: &Log) -> usize {
        let SummarizeState::All(aggs) = self else {
            return 0;
        };
        let mut delta = 0;
        for agg in aggs {
            delta += agg.input(log);
        }
        delta
    }

    fn input_grouped(
        &mut self,
        keys: Vec<Value>,
        log: &Log,
        aggregations: &[Aggregation],
        is_mux: bool,
    ) -> usize {
        let SummarizeState::GroupBy(groups) = self else {
            return 0;
        };

        let mut delta = 0;
        let key_existed = groups.contains_key(&keys);

        let entry = groups
            .entry(keys)
            .or_insert_with(|| create_aggregates(aggregations, is_mux));

        if !key_existed {
            delta += entry
                .iter()
                .map(|a| a.estimate_memory_size())
                .sum::<usize>();
            delta += entry.capacity() * std::mem::size_of::<AggregateState>();
            delta += std::mem::size_of::<Vec<Value>>() + std::mem::size_of::<Vec<AggregateState>>();
            delta += std::mem::size_of::<(Vec<Value>, Vec<AggregateState>)>();
            delta += std::mem::size_of::<u64>();
        }

        for agg in entry {
            delta += agg.input(log);
        }
        delta
    }

    fn to_logs(&self, group_by: &[Expr], output_fields: &[Field]) -> Vec<Log> {
        match self {
            SummarizeState::All(aggs) => {
                let mut log = Log::new();
                for (field, agg) in output_fields.iter().zip(aggs) {
                    insert_field_value(&mut log, field, agg.value());
                }
                vec![log]
            }
            SummarizeState::GroupBy(groups) => groups
                .iter()
                .map(|(keys, aggs)| {
                    let mut log = Log::new();
                    for (expr, value) in group_by.iter().zip(keys) {
                        if let Some(field) = expr_to_field(expr) {
                            insert_field_value(&mut log, field, value.clone());
                        }
                    }
                    for (field, agg) in output_fields.iter().zip(aggs) {
                        insert_field_value(&mut log, field, agg.value());
                    }
                    log
                })
                .collect(),
        }
    }
}

impl MemorySize for AggregateState {
    fn estimate_memory_size(&self) -> usize {
        match self {
            AggregateState::Count(_) | AggregateState::Countif { .. } => 8,
            AggregateState::DCount { seen, .. } => {
                std::mem::size_of::<AggregateState>()
                    + std::mem::size_of::<Field>()
                    + seen.capacity() * (std::mem::size_of::<Value>() + std::mem::size_of::<u64>())
                    + seen.iter().map(|v| v.estimate_memory_size()).sum::<usize>()
            }
            AggregateState::Sum { .. } | AggregateState::Avg { .. } => {
                std::mem::size_of::<AggregateState>() + 8 + std::mem::size_of::<Field>()
            }
            AggregateState::MuxAvg { .. } => {
                std::mem::size_of::<AggregateState>() + 16 + std::mem::size_of::<Field>() * 2
            }
            AggregateState::MinMax {
                value_memory_size, ..
            } => {
                std::mem::size_of::<AggregateState>()
                    + std::mem::size_of::<Field>()
                    + std::mem::size_of::<usize>()
                    + *value_memory_size
            }
        }
    }
}

impl MemorySize for SummarizeState {
    fn estimate_memory_size(&self) -> usize {
        match self {
            SummarizeState::All(aggs) => {
                std::mem::size_of::<SummarizeState>()
                    + aggs.capacity() * std::mem::size_of::<AggregateState>()
                    + aggs.iter().map(|a| a.estimate_memory_size()).sum::<usize>()
            }
            SummarizeState::GroupBy(groups) => {
                let groups_size: usize = groups
                    .iter()
                    .map(|(keys, aggs)| {
                        let keys_size = keys.capacity() * std::mem::size_of::<Value>()
                            + keys.iter().map(|v| v.estimate_memory_size()).sum::<usize>();
                        let aggs_size = aggs.capacity() * std::mem::size_of::<AggregateState>()
                            + aggs.iter().map(|a| a.estimate_memory_size()).sum::<usize>();
                        keys_size + aggs_size
                    })
                    .sum();
                std::mem::size_of::<SummarizeState>()
                    + groups_size
                    + groups.capacity()
                        * (std::mem::size_of::<Vec<Value>>()
                            + std::mem::size_of::<Vec<AggregateState>>())
            }
        }
    }
}

impl Mergeable for SummarizeState {
    fn merge(&mut self, other: &Self) {
        match (self, other) {
            (SummarizeState::All(a), SummarizeState::All(b)) => {
                for (agg_a, agg_b) in a.iter_mut().zip(b) {
                    agg_a.merge(agg_b);
                }
            }
            (SummarizeState::GroupBy(a), SummarizeState::GroupBy(b)) => {
                for (key, other_aggs) in b {
                    if let Some(aggs) = a.get_mut(key) {
                        for (agg_a, agg_b) in aggs.iter_mut().zip(other_aggs) {
                            agg_a.merge(agg_b);
                        }
                    } else {
                        a.insert(key.clone(), other_aggs.clone());
                    }
                }
            }
            _ => {
                unreachable!("impossible 2 different SummarizeStates merge");
            }
        }
    }
}

#[derive(Clone)]
enum AggregateState {
    Count(u64),
    Countif {
        expr: Expr,
        value: u64,
    },
    DCount {
        field: Field,
        seen: HashSet<Value>,
    },
    Sum {
        field: Field,
        value: f64,
    },
    Avg {
        field: Field,
        sum: f64,
        count: u64,
    },
    MuxAvg {
        sum_field: Field,
        count_field: Field,
        value: f64,
        count: f64,
    },
    MinMax {
        field: Field,
        value: Option<Value>,
        value_memory_size: usize,
        update_order: Ordering,
    },
}

impl AggregateState {
    fn from_aggregation(agg: &Aggregation, is_mux: bool) -> Self {
        match agg {
            Aggregation::Count => AggregateState::Count(0),
            Aggregation::Countif(expr) => AggregateState::Countif {
                expr: expr.clone(),
                value: 0,
            },
            Aggregation::DCount(field) => AggregateState::DCount {
                field: field.clone(),
                seen: HashSet::new(),
            },
            Aggregation::Sum(field) => AggregateState::Sum {
                field: field.clone(),
                value: 0.0,
            },
            Aggregation::Avg(field) if is_mux => AggregateState::MuxAvg {
                sum_field: field.clone().with_suffix(MUX_AVG_SUM_SUFFIX),
                count_field: field.clone().with_suffix(MUX_AVG_COUNT_SUFFIX),
                value: 0.0,
                count: 0.0,
            },
            Aggregation::Avg(field) => AggregateState::Avg {
                field: field.clone(),
                sum: 0.0,
                count: 0,
            },
            Aggregation::Min(field) => AggregateState::MinMax {
                field: field.clone(),
                value: None,
                value_memory_size: 0,
                update_order: Ordering::Greater,
            },
            Aggregation::Max(field) => AggregateState::MinMax {
                field: field.clone(),
                value: None,
                value_memory_size: 0,
                update_order: Ordering::Less,
            },
        }
    }

    fn input(&mut self, log: &Log) -> usize {
        match self {
            AggregateState::Count(c) => {
                *c += 1;
                0
            }
            AggregateState::Countif { expr, value } => {
                let interpreter = LogInterpreter { log };
                let keep = match interpreter.eval(expr) {
                    Ok(v) => v.to_bool(),
                    Err(e) => {
                        METRICS
                            .workflow_step_errors
                            .with_label_values(&[STEP_SUMMARIZE, ERROR_EVAL])
                            .inc();
                        warn!("Countif failed: {e}");
                        false
                    }
                };
                if keep {
                    *value += 1;
                }
                0
            }
            AggregateState::DCount { field, seen } => {
                if let Some(v) = get_field_value(log, field) {
                    if seen.insert(v.clone()) {
                        v.estimate_memory_size()
                            + std::mem::size_of::<Value>()
                            + std::mem::size_of::<u64>()
                    } else {
                        0
                    }
                } else {
                    0
                }
            }
            AggregateState::Sum { field, value } => {
                if let Some(v) = get_field_value(log, field)
                    && let Some(x) = v.as_f64()
                {
                    *value += x;
                }
                0
            }
            AggregateState::Avg { field, sum, count } => {
                if let Some(v) = get_field_value(log, field)
                    && let Some(x) = v.as_f64()
                {
                    *sum += x;
                    *count += 1;
                }
                0
            }
            AggregateState::MuxAvg {
                sum_field,
                count_field,
                value,
                count,
            } => {
                let Some(sum_value) = get_field_value(log, sum_field) else {
                    return 0;
                };
                let Some(count_value) = get_field_value(log, count_field) else {
                    return 0;
                };
                let (Some(s), Some(c)) = (sum_value.as_f64(), count_value.as_f64()) else {
                    return 0;
                };
                if c <= 0.0 {
                    return 0;
                }
                let new_count = *count + c;
                let new_avg = *value + (s / c - *value) * (c / new_count);
                *value = new_avg;
                *count = new_count;
                0
            }
            AggregateState::MinMax {
                field,
                value,
                value_memory_size,
                update_order,
            } => {
                let Some(v) = get_field_value(log, field) else {
                    return 0;
                };
                match value {
                    None => {
                        *value = Some(v.clone());
                        *value_memory_size = v.estimate_memory_size();
                        *value_memory_size
                    }
                    Some(stored) if (*stored).cmp(v) == *update_order => {
                        *value = Some(v.clone());
                        let new_size = v.estimate_memory_size();
                        let delta = new_size.wrapping_sub(*value_memory_size);
                        *value_memory_size = new_size;
                        delta
                    }
                    _ => 0,
                }
            }
        }
    }

    fn value(&self) -> Value {
        match self {
            AggregateState::Count(count) => Value::from(*count),
            AggregateState::Countif { value, .. } => Value::from(*value),
            AggregateState::DCount { seen, .. } => Value::from(seen.len()),
            AggregateState::Sum { value, .. } => Value::from(*value),
            AggregateState::Avg { sum, count, .. } => Value::from(if *count == 0 {
                0.0
            } else {
                *sum / *count as f64
            }),
            AggregateState::MuxAvg { value, .. } => Value::from(*value),
            AggregateState::MinMax { value, .. } => value.clone().unwrap_or(Value::Null),
        }
    }

    fn merge(&mut self, other: &Self) {
        match self {
            AggregateState::Count(a) => same_variant!(
                other,
                AggregateState::Count(b) => {
                    *a += b;
                }
            ),

            AggregateState::Countif { value: a, .. } => same_variant!(
                other,
                AggregateState::Countif { value: b, .. } => {
                    *a += b;
                }
            ),

            AggregateState::DCount { seen: a, .. } => same_variant!(
                other,
                AggregateState::DCount { seen: b, .. } => {
                    a.extend(b.iter().cloned());
                }
            ),

            AggregateState::Sum { value: a, .. } => same_variant!(
                other,
                AggregateState::Sum { value: b, .. } => {
                    *a += b;
                }
            ),

            AggregateState::Avg {
                sum: s1, count: c1, ..
            } => same_variant!(
                other,
                AggregateState::Avg { sum: s2, count: c2, .. } => {
                    *s1 += s2;
                    *c1 += c2;
                }
            ),

            AggregateState::MuxAvg {
                value: v1,
                count: c1,
                ..
            } => same_variant!(
                other,
                AggregateState::MuxAvg { value: v2, count: c2, .. } => {
                    if *c2 <= 0.0 {
                        return;
                    }
                    let new_count = *c1 + c2;
                    if new_count > 0.0 {
                        *v1 = (*v1 * *c1 + *v2 * c2) / new_count;
                        *c1 = new_count;
                    }
                }
            ),

            AggregateState::MinMax {
                value: v1,
                value_memory_size,
                update_order,
                ..
            } => same_variant!(
                other,
                AggregateState::MinMax {
                    value: v2,
                    value_memory_size: v2_size,
                    ..
                } => {
                    let Some(val2) = v2 else { return };
                    let should_update = match v1.as_ref() {
                        None => true,
                        Some(val1) => val1.cmp(val2) == *update_order,
                    };
                    if should_update {
                        *v1 = Some(val2.clone());
                        *value_memory_size = *v2_size;
                    }
                }
            ),
        }
    }
}

fn create_aggregates(aggregations: &[Aggregation], is_mux: bool) -> Vec<AggregateState> {
    aggregations
        .iter()
        .map(|a| AggregateState::from_aggregation(a, is_mux))
        .collect()
}

fn expr_to_field(expr: &Expr) -> Option<&Field> {
    match expr {
        Expr::Field(f) => Some(f),
        Expr::Bin(lhs, _) => match &**lhs {
            Expr::Field(f) => Some(f),
            _ => None,
        },
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{make_log, pdone, plog};

    fn create_summarize_iter_with_limit(input: Vec<LogItem>, limit: u64) -> SummarizeIter {
        SummarizeIter::new(
            Box::new(input.into_iter()),
            vec![Expr::Field("x".parse().unwrap())],
            vec!["count_".parse().unwrap()],
            vec![Aggregation::Count],
            false,
            limit,
        )
    }

    fn has_error(iter: SummarizeIter) -> bool {
        iter.into_iter().any(|item| matches!(item, LogItem::Err(_)))
    }

    #[test]
    fn memory_limit_drops_partial_stream_before_erroring() {
        let mut input_with_partial = Vec::new();
        for i in 0..100 {
            input_with_partial.push(plog(make_log(i), 0, 1));
        }
        input_with_partial.push(pdone(0, 1));
        input_with_partial.push(LogItem::Log(make_log(999)));

        let iter = create_summarize_iter_with_limit(input_with_partial, 3000);
        assert!(!has_error(iter));

        let mut input_final_only = Vec::new();
        for i in 0..100 {
            input_final_only.push(LogItem::Log(make_log(i)));
        }

        let iter = create_summarize_iter_with_limit(input_final_only, 500);
        assert!(has_error(iter));
    }
}
