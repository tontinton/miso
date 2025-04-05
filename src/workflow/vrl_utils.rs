use std::cmp::Ordering;

use vrl::value::{KeyString, Value};

// Should be replaced by: https://github.com/vectordotdev/vrl/pull/1117.
pub fn partial_cmp_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Bytes(a), Value::Bytes(b)) => a.partial_cmp(b),
        (Value::Regex(a), Value::Regex(b)) => a.partial_cmp(b),
        (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
        (Value::Object(a), Value::Object(b)) => {
            let keys_a: Vec<&KeyString> = a.keys().collect();
            let keys_b: Vec<&KeyString> = b.keys().collect();
            let mut keys_a = keys_a;
            let mut keys_b = keys_b;
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
            if a.len() < b.len() {
                return Some(Ordering::Less);
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
