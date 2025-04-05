use serde_json::Value;
use std::cmp::Ordering;

pub fn partial_cmp_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::String(a), Value::String(b)) => a.partial_cmp(b),
        (Value::Number(a), Value::Number(b)) => {
            if a.is_i64() && b.is_i64() {
                a.as_i64().partial_cmp(&b.as_i64())
            } else if a.is_u64() && b.is_u64() {
                a.as_u64().partial_cmp(&b.as_u64())
            } else {
                a.as_f64().partial_cmp(&b.as_f64())
            }
        }
        (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
        (Value::Object(a), Value::Object(b)) => {
            if a.len() != b.len() {
                return a.len().partial_cmp(&b.len());
            }

            let mut keys_a: Vec<&String> = a.keys().collect();
            let mut keys_b: Vec<&String> = b.keys().collect();
            keys_a.sort();
            keys_b.sort();

            let keys_cmp = keys_a.partial_cmp(&keys_b)?;
            if keys_cmp != Ordering::Equal {
                return Some(keys_cmp);
            }

            for key in keys_a.into_iter() {
                let cmp = partial_cmp_values(&a[key], &b[key])?;
                if cmp != Ordering::Equal {
                    return Some(cmp);
                }
            }

            Some(Ordering::Equal)
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return a.len().partial_cmp(&b.len());
            }

            for (x, y) in a.iter().zip(b) {
                let cmp = partial_cmp_values(x, y)?;
                if cmp != Ordering::Equal {
                    return Some(cmp);
                }
            }

            Some(Ordering::Equal)
        }
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        _ => None,
    }
}

pub fn get_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(x) if x.is_i64() => "integer",
        Value::Number(_) => "float",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

pub fn value_to_bool(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(x) => {
            if let Some(i) = x.as_i64() {
                i != 0
            } else if let Some(f) = x.as_f64() {
                f != 0.0
            } else {
                panic!("number not f64 or i64");
            }
        }
        Value::String(x) => !x.is_empty(),
        Value::Array(x) => !x.is_empty(),
        Value::Object(x) => !x.is_empty(),
    }
}
