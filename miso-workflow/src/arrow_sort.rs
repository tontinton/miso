//! Arrow-based sorting implementation for performance comparison.
//!
//! This module provides a hybrid Arrow-based sort that extracts only the sort key columns
//! to Arrow arrays, uses `lexsort_to_indices` for efficient sorting, and then reorders
//! the original `Vec<Log>` using those indices.
//!
//! This avoids full Log→Arrow→Log conversion overhead while getting Arrow's sorting benefits:
//! - SIMD-accelerated comparisons
//! - Cache-friendly contiguous memory layout
//! - Sort-by-indices (minimal data movement during sort)

use std::sync::Arc;

use arrow::{
    array::{
        ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, NullArray,
        StringBuilder, TimestampNanosecondBuilder, UInt32Array, UInt64Builder,
    },
    compute::{SortColumn, SortOptions, lexsort_to_indices},
};
use miso_workflow_types::{
    field::Field,
    log::Log,
    sort::{NullsOrder, Sort, SortOrder},
    value::Value,
};

use crate::interpreter::get_field_value;

/// Error types for Arrow sort operations
#[derive(Debug)]
pub enum ArrowSortError {
    /// Mixed types in a single column that can't be unified
    MixedTypes { field: String, types: Vec<String> },
    /// Arrow compute error
    ArrowError(arrow::error::ArrowError),
}

impl From<arrow::error::ArrowError> for ArrowSortError {
    fn from(e: arrow::error::ArrowError) -> Self {
        ArrowSortError::ArrowError(e)
    }
}

/// Detected column type from sampling logs
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedType {
    Null,
    Bool,
    Int64,
    UInt64,
    Float64,
    Timestamp,
    String,
    /// Types that can't be efficiently sorted in Arrow (Array, Object)
    Unsupported,
}

impl DetectedType {
    fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => DetectedType::Null,
            Value::Bool(_) => DetectedType::Bool,
            Value::Int(_) => DetectedType::Int64,
            Value::UInt(_) => DetectedType::UInt64,
            Value::Float(_) => DetectedType::Float64,
            Value::Timestamp(_) => DetectedType::Timestamp,
            Value::Timespan(_) => DetectedType::Int64, // Store as nanoseconds
            Value::String(_) => DetectedType::String,
            Value::Array(_) | Value::Object(_) => DetectedType::Unsupported,
        }
    }

    /// Try to unify two types into a common type
    fn unify(self, other: Self) -> Option<Self> {
        use DetectedType::*;
        match (self, other) {
            (a, b) if a == b => Some(a),
            (Null, b) => Some(b),
            (a, Null) => Some(a),
            // Numeric promotions
            (Int64, UInt64) | (UInt64, Int64) => Some(Int64), // Use signed to handle negative
            (Int64, Float64) | (Float64, Int64) => Some(Float64),
            (UInt64, Float64) | (Float64, UInt64) => Some(Float64),
            _ => None,
        }
    }
}

/// Infer the type for a sort key column by sampling logs
fn infer_column_type(logs: &[Log], field: &Field) -> DetectedType {
    let mut unified_type = DetectedType::Null;

    for log in logs {
        if let Some(value) = get_field_value(log, field) {
            let value_type = DetectedType::from_value(value);
            if value_type == DetectedType::Unsupported {
                return DetectedType::Unsupported;
            }
            if let Some(unified) = unified_type.unify(value_type) {
                unified_type = unified;
            } else {
                // Can't unify types, fall back to string representation
                return DetectedType::String;
            }
        }
    }

    unified_type
}

/// Build an Arrow array for a single sort key column
fn build_column_array(logs: &[Log], field: &Field, detected_type: DetectedType) -> ArrayRef {
    match detected_type {
        DetectedType::Null => Arc::new(NullArray::new(logs.len())),

        DetectedType::Bool => {
            let mut builder = BooleanBuilder::with_capacity(logs.len());
            for log in logs {
                match get_field_value(log, field) {
                    Some(Value::Bool(b)) => builder.append_value(*b),
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }

        DetectedType::Int64 => {
            let mut builder = Int64Builder::with_capacity(logs.len());
            for log in logs {
                match get_field_value(log, field) {
                    Some(Value::Int(i)) => builder.append_value(*i),
                    Some(Value::UInt(u)) => builder.append_value(*u as i64),
                    Some(Value::Timespan(d)) => {
                        builder.append_value(d.whole_nanoseconds() as i64)
                    }
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }

        DetectedType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(logs.len());
            for log in logs {
                match get_field_value(log, field) {
                    Some(Value::UInt(u)) => builder.append_value(*u),
                    Some(Value::Int(i)) if *i >= 0 => builder.append_value(*i as u64),
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }

        DetectedType::Float64 => {
            let mut builder = Float64Builder::with_capacity(logs.len());
            for log in logs {
                match get_field_value(log, field) {
                    Some(Value::Float(f)) => builder.append_value(*f),
                    Some(Value::Int(i)) => builder.append_value(*i as f64),
                    Some(Value::UInt(u)) => builder.append_value(*u as f64),
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }

        DetectedType::Timestamp => {
            let mut builder =
                TimestampNanosecondBuilder::with_capacity(logs.len()).with_timezone("UTC");
            for log in logs {
                match get_field_value(log, field) {
                    Some(Value::Timestamp(ts)) => {
                        let nanos = ts.unix_timestamp_nanos() as i64;
                        builder.append_value(nanos);
                    }
                    Some(Value::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }

        DetectedType::String => {
            let mut builder = StringBuilder::with_capacity(logs.len(), logs.len() * 32);
            for log in logs {
                match get_field_value(log, field) {
                    Some(Value::String(s)) => builder.append_value(s),
                    Some(Value::Null) | None => builder.append_null(),
                    Some(other) => {
                        // Convert other types to string for comparison
                        builder.append_value(other.to_string());
                    }
                }
            }
            Arc::new(builder.finish())
        }

        DetectedType::Unsupported => {
            // Serialize to JSON string for comparison
            let mut builder = StringBuilder::with_capacity(logs.len(), logs.len() * 64);
            for log in logs {
                match get_field_value(log, field) {
                    Some(value) => builder.append_value(value.to_string()),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
    }
}

/// Check if Arrow sort is beneficial for the given data
pub fn is_arrow_sort_beneficial(logs: &[Log], sorts: &[Sort]) -> bool {
    // Skip for small datasets - conversion overhead not worth it
    if logs.len() < 1000 {
        return false;
    }

    // Check if any sort key has unsupported types
    for sort in sorts {
        let detected_type = infer_column_type(logs, &sort.by);
        if detected_type == DetectedType::Unsupported {
            return false;
        }
    }

    true
}

/// Extract sort key columns to Arrow arrays
pub fn extract_sort_columns(
    logs: &[Log],
    sorts: &[Sort],
) -> Result<Vec<SortColumn>, ArrowSortError> {
    let mut sort_columns = Vec::with_capacity(sorts.len());

    for sort in sorts {
        let detected_type = infer_column_type(logs, &sort.by);
        let array = build_column_array(logs, &sort.by, detected_type);

        let options = SortOptions {
            descending: sort.order == SortOrder::Desc,
            nulls_first: sort.nulls == NullsOrder::First,
        };

        sort_columns.push(SortColumn {
            values: array,
            options: Some(options),
        });
    }

    Ok(sort_columns)
}

/// Perform Arrow-based sort and return sorted indices
pub fn arrow_sort_indices(sort_columns: &[SortColumn]) -> Result<UInt32Array, ArrowSortError> {
    let indices = lexsort_to_indices(sort_columns, None)?;
    Ok(indices)
}

/// Reorder logs in-place using sorted indices
pub fn reorder_by_indices<T>(data: Vec<T>, indices: &UInt32Array) -> Vec<T> {
    let mut data = data;
    let mut result = Vec::with_capacity(data.len());

    // Create a vector to track which elements have been moved
    // We use Option to allow taking ownership without cloning
    let mut source: Vec<Option<T>> = data.drain(..).map(Some).collect();

    for idx in indices.values() {
        if let Some(item) = source[*idx as usize].take() {
            result.push(item);
        }
    }

    result
}

/// Main entry point: Sort logs using Arrow
///
/// This is a hybrid approach:
/// 1. Extract only sort key columns to Arrow arrays
/// 2. Use lexsort_to_indices for efficient sorting
/// 3. Reorder original Vec<Log> using indices
pub fn sort_logs_arrow(logs: Vec<Log>, sorts: &[Sort]) -> Result<Vec<Log>, ArrowSortError> {
    if logs.is_empty() || sorts.is_empty() {
        return Ok(logs);
    }

    // Extract sort columns to Arrow
    let sort_columns = extract_sort_columns(&logs, sorts)?;

    // Get sorted indices
    let indices = arrow_sort_indices(&sort_columns)?;

    // Reorder logs using indices
    Ok(reorder_by_indices(logs, &indices))
}

#[cfg(test)]
mod tests {
    use super::*;
    use miso_workflow_types::field_unwrap;
    use std::str::FromStr;

    fn create_log(fields: Vec<(&str, Value)>) -> Log {
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    #[test]
    fn test_sort_by_int() {
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

        let sorted = sort_logs_arrow(logs, &sorts).unwrap();

        assert_eq!(sorted[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(sorted[1].get("id"), Some(&Value::Int(2)));
        assert_eq!(sorted[2].get("id"), Some(&Value::Int(3)));
    }

    #[test]
    fn test_sort_by_string() {
        let logs = vec![
            create_log(vec![("name", Value::String("charlie".into()))]),
            create_log(vec![("name", Value::String("alice".into()))]),
            create_log(vec![("name", Value::String("bob".into()))]),
        ];

        let sorts = vec![Sort {
            by: field_unwrap!("name"),
            order: SortOrder::Asc,
            nulls: NullsOrder::Last,
        }];

        let sorted = sort_logs_arrow(logs, &sorts).unwrap();

        assert_eq!(
            sorted[0].get("name"),
            Some(&Value::String("alice".into()))
        );
        assert_eq!(sorted[1].get("name"), Some(&Value::String("bob".into())));
        assert_eq!(
            sorted[2].get("name"),
            Some(&Value::String("charlie".into()))
        );
    }

    #[test]
    fn test_sort_with_nulls() {
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

        let sorted = sort_logs_arrow(logs, &sorts).unwrap();

        assert_eq!(sorted[0].get("id"), Some(&Value::Int(1)));
        assert_eq!(sorted[1].get("id"), Some(&Value::Int(2)));
        assert_eq!(sorted[2].get("id"), Some(&Value::Null));
    }

    #[test]
    fn test_sort_desc() {
        let logs = vec![
            create_log(vec![("id", Value::Int(1))]),
            create_log(vec![("id", Value::Int(3))]),
            create_log(vec![("id", Value::Int(2))]),
        ];

        let sorts = vec![Sort {
            by: field_unwrap!("id"),
            order: SortOrder::Desc,
            nulls: NullsOrder::Last,
        }];

        let sorted = sort_logs_arrow(logs, &sorts).unwrap();

        assert_eq!(sorted[0].get("id"), Some(&Value::Int(3)));
        assert_eq!(sorted[1].get("id"), Some(&Value::Int(2)));
        assert_eq!(sorted[2].get("id"), Some(&Value::Int(1)));
    }

    #[test]
    fn test_multi_key_sort() {
        let logs = vec![
            create_log(vec![
                ("group", Value::String("b".into())),
                ("id", Value::Int(1)),
            ]),
            create_log(vec![
                ("group", Value::String("a".into())),
                ("id", Value::Int(2)),
            ]),
            create_log(vec![
                ("group", Value::String("a".into())),
                ("id", Value::Int(1)),
            ]),
        ];

        let sorts = vec![
            Sort {
                by: field_unwrap!("group"),
                order: SortOrder::Asc,
                nulls: NullsOrder::Last,
            },
            Sort {
                by: field_unwrap!("id"),
                order: SortOrder::Asc,
                nulls: NullsOrder::Last,
            },
        ];

        let sorted = sort_logs_arrow(logs, &sorts).unwrap();

        // First by group "a", then by id
        assert_eq!(sorted[0].get("group"), Some(&Value::String("a".into())));
        assert_eq!(sorted[0].get("id"), Some(&Value::Int(1)));

        assert_eq!(sorted[1].get("group"), Some(&Value::String("a".into())));
        assert_eq!(sorted[1].get("id"), Some(&Value::Int(2)));

        assert_eq!(sorted[2].get("group"), Some(&Value::String("b".into())));
        assert_eq!(sorted[2].get("id"), Some(&Value::Int(1)));
    }
}
