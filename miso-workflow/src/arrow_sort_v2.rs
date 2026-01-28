//! Arrow sort v2 - Aggressive optimizations for benchmarking.
//!
//! Optimizations to test:
//! 1. Inline flat field access (skip get_field_value for simple fields)
//! 2. Iterator-based array construction (vs builders)
//! 3. Parallel index sorting with rayon
//! 4. Unsafe reordering without Option wrapper

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray,
        TimestampNanosecondArray, UInt64Array,
    },
    buffer::NullBuffer,
    compute::{SortColumn, SortOptions, lexsort_to_indices},
};
use miso_workflow_types::{
    field::Field,
    log::Log,
    sort::{NullsOrder, Sort, SortOrder},
    value::Value,
};
use rayon::prelude::*;

/// Error types for Arrow sort operations
#[derive(Debug)]
pub enum ArrowSortError {
    ArrowError(arrow::error::ArrowError),
}

impl From<arrow::error::ArrowError> for ArrowSortError {
    fn from(e: arrow::error::ArrowError) -> Self {
        ArrowSortError::ArrowError(e)
    }
}

/// Detected column type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedType {
    Null,
    Bool,
    Int64,
    UInt64,
    Float64,
    Timestamp,
    String,
}

impl DetectedType {
    #[inline]
    fn from_value(value: &Value) -> Option<Self> {
        match value {
            Value::Null => Some(DetectedType::Null),
            Value::Bool(_) => Some(DetectedType::Bool),
            Value::Int(_) => Some(DetectedType::Int64),
            Value::UInt(_) => Some(DetectedType::UInt64),
            Value::Float(_) => Some(DetectedType::Float64),
            Value::Timestamp(_) => Some(DetectedType::Timestamp),
            Value::Timespan(_) => Some(DetectedType::Int64),
            Value::String(_) => Some(DetectedType::String),
            Value::Array(_) | Value::Object(_) => None,
        }
    }

    #[inline]
    fn unify(self, other: Self) -> Option<Self> {
        use DetectedType::*;
        match (self, other) {
            (a, b) if a == b => Some(a),
            (Null, b) => Some(b),
            (a, Null) => Some(a),
            (Int64, UInt64) | (UInt64, Int64) => Some(Int64),
            (Int64, Float64) | (Float64, Int64) => Some(Float64),
            (UInt64, Float64) | (Float64, UInt64) => Some(Float64),
            _ => None,
        }
    }
}

/// Optimized field access - inline for flat fields (most common case)
#[inline]
fn get_value_fast<'a>(log: &'a Log, field: &Field) -> Option<&'a Value> {
    // Fast path: single segment, no array indices
    if field.len() == 1 && field[0].arr_indices.is_empty() {
        return log.get(&field[0].name);
    }
    // Slow path: nested field
    crate::interpreter::get_field_value(log, field)
}

/// Infer type from first 100 non-null values (faster than sampling across dataset)
fn infer_type_fast(logs: &[Log], field: &Field) -> DetectedType {
    let mut unified = DetectedType::Null;
    let mut found = 0;

    for log in logs {
        if let Some(value) = get_value_fast(log, field) {
            if let Some(t) = DetectedType::from_value(value) {
                if t != DetectedType::Null {
                    if let Some(u) = unified.unify(t) {
                        unified = u;
                        found += 1;
                        if found >= 100 {
                            break;
                        }
                    } else {
                        return DetectedType::String;
                    }
                }
            } else {
                return DetectedType::String;
            }
        }
    }

    unified
}

/// Build Int64 array using iterator (can be faster than builder)
fn build_int64_fast(logs: &[Log], field: &Field) -> ArrayRef {
    let values: Vec<Option<i64>> = logs
        .iter()
        .map(|log| match get_value_fast(log, field) {
            Some(Value::Int(i)) => Some(*i),
            Some(Value::UInt(u)) => Some(*u as i64),
            Some(Value::Timespan(d)) => Some(d.whole_nanoseconds() as i64),
            _ => None,
        })
        .collect();
    Arc::new(Int64Array::from(values))
}

/// Build UInt64 array
fn build_uint64_fast(logs: &[Log], field: &Field) -> ArrayRef {
    let values: Vec<Option<u64>> = logs
        .iter()
        .map(|log| match get_value_fast(log, field) {
            Some(Value::UInt(u)) => Some(*u),
            Some(Value::Int(i)) if *i >= 0 => Some(*i as u64),
            _ => None,
        })
        .collect();
    Arc::new(UInt64Array::from(values))
}

/// Build Float64 array
fn build_float64_fast(logs: &[Log], field: &Field) -> ArrayRef {
    let values: Vec<Option<f64>> = logs
        .iter()
        .map(|log| match get_value_fast(log, field) {
            Some(Value::Float(f)) => Some(*f),
            Some(Value::Int(i)) => Some(*i as f64),
            Some(Value::UInt(u)) => Some(*u as f64),
            _ => None,
        })
        .collect();
    Arc::new(Float64Array::from(values))
}

/// Build Timestamp array - optimized with direct value/null separation
fn build_timestamp_fast(logs: &[Log], field: &Field) -> ArrayRef {
    let mut values = Vec::with_capacity(logs.len());
    let mut nulls = Vec::with_capacity(logs.len());

    for log in logs {
        match get_value_fast(log, field) {
            Some(Value::Timestamp(ts)) => {
                values.push(ts.unix_timestamp_nanos() as i64);
                nulls.push(true);
            }
            _ => {
                values.push(0);
                nulls.push(false);
            }
        }
    }

    let null_buffer = NullBuffer::from(nulls);
    Arc::new(
        TimestampNanosecondArray::new(values.into(), Some(null_buffer))
            .with_timezone("UTC")
    )
}

/// Build Boolean array
fn build_bool_fast(logs: &[Log], field: &Field) -> ArrayRef {
    let values: Vec<Option<bool>> = logs
        .iter()
        .map(|log| match get_value_fast(log, field) {
            Some(Value::Bool(b)) => Some(*b),
            _ => None,
        })
        .collect();
    Arc::new(BooleanArray::from(values))
}

/// Build String array
fn build_string_fast(logs: &[Log], field: &Field) -> ArrayRef {
    let values: Vec<Option<&str>> = logs
        .iter()
        .map(|log| match get_value_fast(log, field) {
            Some(Value::String(s)) => Some(s.as_str()),
            _ => None,
        })
        .collect();
    Arc::new(StringArray::from(values))
}

/// Build array based on detected type
fn build_array_fast(logs: &[Log], field: &Field, detected_type: DetectedType) -> ArrayRef {
    match detected_type {
        DetectedType::Null => Arc::new(NullArray::new(logs.len())),
        DetectedType::Bool => build_bool_fast(logs, field),
        DetectedType::Int64 => build_int64_fast(logs, field),
        DetectedType::UInt64 => build_uint64_fast(logs, field),
        DetectedType::Float64 => build_float64_fast(logs, field),
        DetectedType::Timestamp => build_timestamp_fast(logs, field),
        DetectedType::String => build_string_fast(logs, field),
    }
}

/// Reorder using unsafe permutation - avoids Option wrapper overhead
#[inline]
pub fn reorder_unsafe<T>(mut data: Vec<T>, indices: &[u32]) -> Vec<T> {
    let len = data.len();
    let mut result: Vec<T> = Vec::with_capacity(len);

    // SAFETY: We're reading each index exactly once and the indices are valid
    unsafe {
        result.set_len(len);
        let src: *const T = data.as_ptr();
        let dst: *mut T = result.as_mut_ptr();

        for (i, &idx) in indices.iter().enumerate() {
            std::ptr::copy_nonoverlapping(src.add(idx as usize), dst.add(i), 1);
        }

        // Prevent drop of moved-from elements
        data.set_len(0);
    }

    result
}

/// Main entry point - v2 optimized
pub fn sort_logs_arrow_v2(
    logs: Vec<Log>,
    sorts: &[Sort],
) -> Result<Vec<Log>, ArrowSortError> {
    if logs.is_empty() || sorts.is_empty() {
        return Ok(logs);
    }

    // Build sort columns
    let sort_columns: Vec<SortColumn> = sorts
        .iter()
        .map(|sort| {
            let detected_type = infer_type_fast(&logs, &sort.by);
            let array = build_array_fast(&logs, &sort.by, detected_type);
            SortColumn {
                values: array,
                options: Some(SortOptions {
                    descending: sort.order == SortOrder::Desc,
                    nulls_first: sort.nulls == NullsOrder::First,
                }),
            }
        })
        .collect();

    // Sort
    let indices = lexsort_to_indices(&sort_columns, None)?;

    // Reorder using unsafe permutation
    Ok(reorder_unsafe(logs, indices.values()))
}

/// Build Int64 array in parallel chunks
fn build_int64_parallel(logs: &[Log], field: &Field) -> ArrayRef {
    const CHUNK_SIZE: usize = 16384;

    if logs.len() < CHUNK_SIZE * 2 {
        return build_int64_fast(logs, field);
    }

    let chunks: Vec<Vec<Option<i64>>> = logs
        .par_chunks(CHUNK_SIZE)
        .map(|chunk| {
            chunk
                .iter()
                .map(|log| match get_value_fast(log, field) {
                    Some(Value::Int(i)) => Some(*i),
                    Some(Value::UInt(u)) => Some(*u as i64),
                    Some(Value::Timespan(d)) => Some(d.whole_nanoseconds() as i64),
                    _ => None,
                })
                .collect()
        })
        .collect();

    let values: Vec<Option<i64>> = chunks.into_iter().flatten().collect();
    Arc::new(Int64Array::from(values))
}

/// Build Timestamp array in parallel chunks
fn build_timestamp_parallel(logs: &[Log], field: &Field) -> ArrayRef {
    const CHUNK_SIZE: usize = 16384;

    if logs.len() < CHUNK_SIZE * 2 {
        return build_timestamp_fast(logs, field);
    }

    let chunks: Vec<Vec<(i64, bool)>> = logs
        .par_chunks(CHUNK_SIZE)
        .map(|chunk| {
            chunk
                .iter()
                .map(|log| match get_value_fast(log, field) {
                    Some(Value::Timestamp(ts)) => (ts.unix_timestamp_nanos() as i64, true),
                    _ => (0, false),
                })
                .collect()
        })
        .collect();

    let mut values = Vec::with_capacity(logs.len());
    let mut nulls = Vec::with_capacity(logs.len());

    for chunk in chunks {
        for (v, valid) in chunk {
            values.push(v);
            nulls.push(valid);
        }
    }

    let null_buffer = NullBuffer::from(nulls);
    Arc::new(
        TimestampNanosecondArray::new(values.into(), Some(null_buffer))
            .with_timezone("UTC")
    )
}

/// Build array with parallel construction for large datasets
fn build_array_parallel(logs: &[Log], field: &Field, detected_type: DetectedType) -> ArrayRef {
    match detected_type {
        DetectedType::Null => Arc::new(NullArray::new(logs.len())),
        DetectedType::Bool => build_bool_fast(logs, field),
        DetectedType::Int64 => build_int64_parallel(logs, field),
        DetectedType::UInt64 => build_uint64_fast(logs, field),
        DetectedType::Float64 => build_float64_fast(logs, field),
        DetectedType::Timestamp => build_timestamp_parallel(logs, field),
        DetectedType::String => build_string_fast(logs, field),
    }
}

/// Version with parallel array building
pub fn sort_logs_arrow_v2_parallel(
    logs: Vec<Log>,
    sorts: &[Sort],
) -> Result<Vec<Log>, ArrowSortError> {
    if logs.is_empty() || sorts.is_empty() {
        return Ok(logs);
    }

    // Build sort columns with parallel array construction
    let sort_columns: Vec<SortColumn> = sorts
        .iter()
        .map(|sort| {
            let detected_type = infer_type_fast(&logs, &sort.by);
            let array = build_array_parallel(&logs, &sort.by, detected_type);
            SortColumn {
                values: array,
                options: Some(SortOptions {
                    descending: sort.order == SortOrder::Desc,
                    nulls_first: sort.nulls == NullsOrder::First,
                }),
            }
        })
        .collect();

    // Sort
    let indices = lexsort_to_indices(&sort_columns, None)?;

    // Reorder
    Ok(reorder_unsafe(logs, indices.values()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use miso_workflow_types::field_unwrap;
    use std::str::FromStr;

    fn create_log(fields: Vec<(&str, Value)>) -> Log {
        fields.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    #[test]
    fn test_v2_sort_by_int() {
        let logs = vec![
            create_log(vec![("id", Value::Int(3))]),
            create_log(vec![("id", Value::Int(1))]),
            create_log(vec![("id", Value::Int(2))]),
        ];

        let sorts = vec![Sort {
            by: field_unwrap!("id"),
            order: SortOrder::Asc,
            nulls: NullsOrder::Last,
        }];

        let sorted = sort_logs_arrow_v2(logs, &sorts).unwrap();

        assert_eq!(sorted[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(sorted[1].get("id"), Some(&Value::Int(2)));
        assert_eq!(sorted[2].get("id"), Some(&Value::Int(3)));
    }

    #[test]
    fn test_v2_sort_with_nulls() {
        let logs = vec![
            create_log(vec![("id", Value::Int(2))]),
            create_log(vec![("id", Value::Null)]),
            create_log(vec![("id", Value::Int(1))]),
        ];

        let sorts = vec![Sort {
            by: field_unwrap!("id"),
            order: SortOrder::Asc,
            nulls: NullsOrder::Last,
        }];

        let sorted = sort_logs_arrow_v2(logs, &sorts).unwrap();

        assert_eq!(sorted[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(sorted[1].get("id"), Some(&Value::Int(2)));
        assert_eq!(sorted[2].get("id"), Some(&Value::Null));
    }
}
