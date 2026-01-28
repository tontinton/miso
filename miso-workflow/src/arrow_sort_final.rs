//! Minimal Arrow-based sorting - KISS implementation with proven optimizations.

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    buffer::NullBuffer,
    compute::{lexsort_to_indices, SortColumn, SortOptions},
};
use miso_workflow_types::{
    field::Field,
    log::Log,
    sort::{NullsOrder, Sort, SortOrder},
    value::Value,
};
use rayon::prelude::*;

#[derive(Debug)]
pub struct ArrowSortError(pub arrow::error::ArrowError);

impl From<arrow::error::ArrowError> for ArrowSortError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self(e)
    }
}

#[derive(Clone, Copy, PartialEq)]
enum ColType {
    Null,
    Bool,
    Int64,
    UInt64,
    Float64,
    Timestamp,
    String,
}

impl ColType {
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Null => Some(Self::Null),
            Value::Bool(_) => Some(Self::Bool),
            Value::Int(_) | Value::Timespan(_) => Some(Self::Int64),
            Value::UInt(_) => Some(Self::UInt64),
            Value::Float(_) => Some(Self::Float64),
            Value::Timestamp(_) => Some(Self::Timestamp),
            Value::String(_) => Some(Self::String),
            Value::Array(_) | Value::Object(_) => None,
        }
    }

    fn unify(self, other: Self) -> Option<Self> {
        match (self, other) {
            (a, b) if a == b => Some(a),
            (Self::Null, b) | (b, Self::Null) => Some(b),
            (Self::Int64, Self::UInt64) | (Self::UInt64, Self::Int64) => Some(Self::Int64),
            (Self::Int64, Self::Float64) | (Self::Float64, Self::Int64) => Some(Self::Float64),
            (Self::UInt64, Self::Float64) | (Self::Float64, Self::UInt64) => Some(Self::Float64),
            _ => None,
        }
    }
}

#[inline]
fn get_value<'a>(log: &'a Log, field: &Field) -> Option<&'a Value> {
    // Fast path for flat fields (the common case)
    if field.len() == 1 && field[0].arr_indices.is_empty() {
        return log.get(&field[0].name);
    }
    crate::interpreter::get_field_value(log, field)
}

fn infer_type(logs: &[Log], field: &Field) -> ColType {
    let mut result = ColType::Null;
    for log in logs.iter().take(100) {
        if let Some(v) = get_value(log, field) {
            match ColType::from_value(v) {
                Some(t) if t != ColType::Null => {
                    result = result.unify(t).unwrap_or(ColType::String);
                    if result == ColType::String {
                        return result;
                    }
                }
                None => return ColType::String,
                _ => {}
            }
        }
    }
    result
}

fn build_array(logs: &[Log], field: &Field, col_type: ColType) -> ArrayRef {
    const PAR_THRESHOLD: usize = 32_768;

    match col_type {
        ColType::Null => Arc::new(NullArray::new(logs.len())),

        ColType::Bool => Arc::new(BooleanArray::from(
            logs.iter()
                .map(|log| match get_value(log, field) {
                    Some(Value::Bool(b)) => Some(*b),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),

        ColType::Int64 => {
            let extract = |log: &Log| match get_value(log, field) {
                Some(Value::Int(i)) => Some(*i),
                Some(Value::UInt(u)) => Some(*u as i64),
                Some(Value::Timespan(d)) => Some(d.whole_nanoseconds() as i64),
                _ => None,
            };

            if logs.len() >= PAR_THRESHOLD {
                let values: Vec<_> = logs.par_iter().map(extract).collect();
                Arc::new(Int64Array::from(values))
            } else {
                Arc::new(Int64Array::from(logs.iter().map(extract).collect::<Vec<_>>()))
            }
        }

        ColType::UInt64 => Arc::new(UInt64Array::from(
            logs.iter()
                .map(|log| match get_value(log, field) {
                    Some(Value::UInt(u)) => Some(*u),
                    Some(Value::Int(i)) if *i >= 0 => Some(*i as u64),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),

        ColType::Float64 => Arc::new(Float64Array::from(
            logs.iter()
                .map(|log| match get_value(log, field) {
                    Some(Value::Float(f)) => Some(*f),
                    Some(Value::Int(i)) => Some(*i as f64),
                    Some(Value::UInt(u)) => Some(*u as f64),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),

        ColType::Timestamp => {
            let extract = |log: &Log| match get_value(log, field) {
                Some(Value::Timestamp(ts)) => Some(ts.unix_timestamp_nanos() as i64),
                _ => None,
            };

            let (values, nulls): (Vec<i64>, Vec<bool>) = if logs.len() >= PAR_THRESHOLD {
                logs.par_iter()
                    .map(|log| match extract(log) {
                        Some(v) => (v, true),
                        None => (0, false),
                    })
                    .unzip()
            } else {
                logs.iter()
                    .map(|log| match extract(log) {
                        Some(v) => (v, true),
                        None => (0, false),
                    })
                    .unzip()
            };

            Arc::new(
                TimestampNanosecondArray::new(values.into(), Some(NullBuffer::from(nulls)))
                    .with_timezone("UTC"),
            )
        }

        ColType::String => Arc::new(StringArray::from(
            logs.iter()
                .map(|log| match get_value(log, field) {
                    Some(Value::String(s)) => Some(s.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
    }
}

/// Reorder vec by indices without intermediate Option allocation.
fn reorder<T>(mut data: Vec<T>, indices: &[u32]) -> Vec<T> {
    let len = data.len();
    let mut result: Vec<T> = Vec::with_capacity(len);

    // SAFETY: indices are valid (from Arrow's lexsort), each source element read exactly once
    unsafe {
        result.set_len(len);
        let src: *const T = data.as_ptr();
        let dst: *mut T = result.as_mut_ptr();

        for (i, &idx) in indices.iter().enumerate() {
            std::ptr::copy_nonoverlapping(src.add(idx as usize), dst.add(i), 1);
        }

        data.set_len(0); // prevent double-drop
    }

    result
}

pub fn arrow_sort(logs: Vec<Log>, sorts: &[Sort]) -> Result<Vec<Log>, ArrowSortError> {
    if logs.is_empty() || sorts.is_empty() {
        return Ok(logs);
    }

    let sort_columns: Vec<SortColumn> = sorts
        .iter()
        .map(|sort| SortColumn {
            values: build_array(&logs, &sort.by, infer_type(&logs, &sort.by)),
            options: Some(SortOptions {
                descending: sort.order == SortOrder::Desc,
                nulls_first: sort.nulls == NullsOrder::First,
            }),
        })
        .collect();

    let indices = lexsort_to_indices(&sort_columns, None)?;
    Ok(reorder(logs, indices.values()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use miso_workflow_types::field_unwrap;
    use std::str::FromStr;

    fn log(fields: Vec<(&str, Value)>) -> Log {
        fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    #[test]
    fn test_sort_int() {
        let logs = vec![
            log(vec![("x", Value::Int(3))]),
            log(vec![("x", Value::Int(1))]),
            log(vec![("x", Value::Int(2))]),
        ];
        let sorts = vec![Sort {
            by: field_unwrap!("x"),
            order: SortOrder::Asc,
            nulls: NullsOrder::Last,
        }];

        let result = arrow_sort(logs, &sorts).unwrap();
        assert_eq!(result[0].get("x"), Some(&Value::Int(1)));
        assert_eq!(result[1].get("x"), Some(&Value::Int(2)));
        assert_eq!(result[2].get("x"), Some(&Value::Int(3)));
    }

    #[test]
    fn test_sort_with_nulls() {
        let logs = vec![
            log(vec![("x", Value::Int(2))]),
            log(vec![("x", Value::Null)]),
            log(vec![("x", Value::Int(1))]),
        ];
        let sorts = vec![Sort {
            by: field_unwrap!("x"),
            order: SortOrder::Asc,
            nulls: NullsOrder::Last,
        }];

        let result = arrow_sort(logs, &sorts).unwrap();
        assert_eq!(result[0].get("x"), Some(&Value::Int(1)));
        assert_eq!(result[1].get("x"), Some(&Value::Int(2)));
        assert_eq!(result[2].get("x"), Some(&Value::Null));
    }

    #[test]
    fn test_sort_desc() {
        let logs = vec![
            log(vec![("x", Value::Int(1))]),
            log(vec![("x", Value::Int(3))]),
            log(vec![("x", Value::Int(2))]),
        ];
        let sorts = vec![Sort {
            by: field_unwrap!("x"),
            order: SortOrder::Desc,
            nulls: NullsOrder::Last,
        }];

        let result = arrow_sort(logs, &sorts).unwrap();
        assert_eq!(result[0].get("x"), Some(&Value::Int(3)));
        assert_eq!(result[1].get("x"), Some(&Value::Int(2)));
        assert_eq!(result[2].get("x"), Some(&Value::Int(1)));
    }
}
