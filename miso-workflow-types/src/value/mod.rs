#[macro_use]
mod macros;

#[cfg(test)]
mod tests;

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    fmt::{self},
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Deserializer, Serialize};
use time::OffsetDateTime;

use crate::field::Field;

pub type Map<K, V> = BTreeMap<K, V>;
pub type Entry<'a, K, V> = std::collections::btree_map::Entry<'a, K, V>;

#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Timestamp(OffsetDateTime),
    String(String),
    Array(Vec<Value>),
    Object(Map<String, Value>),
}

impl Hash for Value {
    fn hash<H: Hasher>(&self, h: &mut H) {
        match self {
            Value::Null => 0u8.hash(h),
            Value::Bool(x) => x.hash(h),
            Value::Int(x) => x.hash(h),
            Value::UInt(x) => x.hash(h),
            Value::Float(f) => {
                if *f == 0.0f64 {
                    // There are 2 zero representations, +0 and -0, which
                    // compare equal but have different bits. We use the +0 hash
                    // for both so that hash(+0) == hash(-0).
                    0.0f64.to_bits().hash(h);
                } else {
                    f.to_bits().hash(h);
                }
            }
            Value::Timestamp(x) => x.hash(h),
            Value::String(x) => x.hash(h),
            Value::Array(x) => x.hash(h),
            Value::Object(x) => x.hash(h),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Int(i) => write!(f, "{}", i),
            Value::UInt(i) => write!(f, "{}", i),
            Value::Float(fl) => {
                write!(f, "{}", fl)
            }
            Value::Timestamp(t) => {
                write!(f, "\"{}\"", t)
            }
            Value::String(s) => {
                write!(f, "\"")?;
                for ch in s.chars() {
                    match ch {
                        '"' => write!(f, "\\\"")?,
                        '\\' => write!(f, "\\\\")?,
                        '\n' => write!(f, "\\n")?,
                        '\r' => write!(f, "\\r")?,
                        '\t' => write!(f, "\\t")?,
                        '\u{08}' => write!(f, "\\b")?,
                        '\u{0C}' => write!(f, "\\f")?,
                        c if c.is_control() => {
                            write!(f, "\\u{:04x}", c as u32)?;
                        }
                        c => write!(f, "{}", c)?,
                    }
                }
                write!(f, "\"")
            }
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, value) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}", value)?;
                }
                write!(f, "]")
            }
            Value::Object(obj) => {
                write!(f, "{{")?;
                let mut first = true;
                for (key, value) in obj.iter() {
                    if !first {
                        write!(f, ",")?;
                    }
                    first = false;

                    write!(f, "\"")?;
                    for ch in key.chars() {
                        match ch {
                            '"' => write!(f, "\\\"")?,
                            '\\' => write!(f, "\\\\")?,
                            '\n' => write!(f, "\\n")?,
                            '\r' => write!(f, "\\r")?,
                            '\t' => write!(f, "\\t")?,
                            '\u{08}' => write!(f, "\\b")?,
                            '\u{0C}' => write!(f, "\\f")?,
                            c if c.is_control() => {
                                write!(f, "\\u{:04x}", c as u32)?;
                            }
                            c => write!(f, "{}", c)?,
                        }
                    }
                    write!(f, "\":")?;
                    write!(f, "{}", value)?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        use Value::*;

        match (self, other) {
            (Null, Null) => Ordering::Equal,
            (Null, _) => Ordering::Less,
            (_, Null) => Ordering::Greater,

            (Bool(a), Bool(b)) => a.cmp(b),
            (Bool(_), _) => Ordering::Less,
            (_, Bool(_)) => Ordering::Greater,

            (Int(a), Int(b)) => a.cmp(b),
            (UInt(a), UInt(b)) => a.cmp(b),
            (Float(a), Float(b)) => a.partial_cmp(b).expect("floats are finite"),

            (Int(a), UInt(b)) => {
                if *a < 0 {
                    Ordering::Less
                } else {
                    (*a as u64).cmp(b)
                }
            }
            (UInt(a), Int(b)) => {
                if *b < 0 {
                    Ordering::Greater
                } else {
                    a.cmp(&(*b as u64))
                }
            }

            (Int(a), Float(b)) => (*a as f64).partial_cmp(b).expect("floats are finite"),
            (Float(a), Int(b)) => a.partial_cmp(&(*b as f64)).expect("floats are finite"),
            (UInt(a), Float(b)) => (*a as f64).partial_cmp(b).expect("floats are finite"),
            (Float(a), UInt(b)) => a.partial_cmp(&(*b as f64)).expect("floats are finite"),

            (Int(_), _) => Ordering::Less,
            (_, Int(_)) => Ordering::Greater,
            (UInt(_), _) => Ordering::Less,
            (_, UInt(_)) => Ordering::Greater,
            (Float(_), _) => Ordering::Less,
            (_, Float(_)) => Ordering::Greater,

            (Timestamp(a), Timestamp(b)) => a.cmp(b),
            (Timestamp(_), _) => Ordering::Less,
            (_, Timestamp(_)) => Ordering::Greater,

            (String(a), String(b)) => a.cmp(b),
            (String(_), _) => Ordering::Less,
            (_, String(_)) => Ordering::Greater,

            (Array(a), Array(b)) => a.cmp(b),
            (Array(_), _) => Ordering::Less,
            (_, Array(_)) => Ordering::Greater,

            (Object(a), Object(b)) => a.cmp(b),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

// Implementing these is fine since float values are always finite.
impl Eq for Value {}

impl From<()> for Value {
    fn from(_: ()) -> Self {
        Value::Null
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Value::UInt(value as u64)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::UInt(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::UInt(value as u64)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::Int(value as i64)
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Value::UInt(value as u64)
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Value::Int(value as i64)
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Value::UInt(value as u64)
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Value::Int(value as i64)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float(value as f64)
    }
}

impl From<OffsetDateTime> for Value {
    fn from(value: OffsetDateTime) -> Self {
        Value::Timestamp(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<Field> for Value {
    fn from(value: Field) -> Self {
        Value::String(value.to_string())
    }
}

impl From<&Field> for Value {
    fn from(value: &Field) -> Self {
        Value::String(value.to_string())
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(value: Vec<T>) -> Self {
        Value::Array(value.into_iter().map(|v| v.into()).collect())
    }
}

impl From<Map<String, Value>> for Value {
    fn from(value: Map<String, Value>) -> Self {
        Value::Object(value)
    }
}

impl From<Map<&str, Value>> for Value {
    fn from(value: Map<&str, Value>) -> Self {
        let converted: Map<String, Value> =
            value.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        Value::Object(converted)
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> serde::de::Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("any valid value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Value, E> {
                Ok(v.into())
            }

            fn visit_i64<E>(self, v: i64) -> Result<Value, E> {
                Ok(v.into())
            }

            fn visit_u64<E>(self, v: u64) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                Ok(v.into())
            }

            fn visit_f64<E>(self, v: f64) -> Result<Value, E>
            where
                E: serde::de::Error,
            {
                if v.is_finite() {
                    Ok(v.into())
                } else {
                    Err(E::custom("invalid number: NaN or infinity not allowed"))
                }
            }

            fn visit_str<E>(self, v: &str) -> Result<Value, E> {
                Ok(v.into())
            }

            fn visit_string<E>(self, v: String) -> Result<Value, E> {
                Ok(v.into())
            }

            fn visit_none<E>(self) -> Result<Value, E> {
                Ok(().into())
            }

            fn visit_unit<E>(self) -> Result<Value, E> {
                Ok(().into())
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut vec = Vec::new();
                while let Some(elem) = seq.next_element()? {
                    vec.push(elem);
                }
                Ok(Value::Array(vec))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut obj: Map<String, Value> = Map::new();
                while let Some((k, v)) = map.next_entry()? {
                    obj.insert(k, v);
                }
                Ok(obj.into())
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Int(i) => serializer.serialize_i64(*i),
            Value::UInt(i) => serializer.serialize_u64(*i),
            Value::Float(f) => {
                if f.is_finite() {
                    serializer.serialize_f64(*f)
                } else {
                    Err(serde::ser::Error::custom("NaN or Infinity not allowed"))
                }
            }
            Value::Timestamp(t) => serializer.serialize_str(&t.to_string()),
            Value::String(s) => serializer.serialize_str(s),
            Value::Array(arr) => arr.serialize(serializer),
            Value::Object(obj) => obj.serialize(serializer),
        }
    }
}

impl Value {
    pub fn kind(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Int(_) => "integer",
            Value::UInt(_) => "integer",
            Value::Float(_) => "float",
            Value::Timestamp(_) => "timestamp",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
        }
    }

    pub fn to_bool(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::UInt(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::Timestamp(_) => true,
            Value::String(x) => !x.is_empty(),
            Value::Array(x) => !x.is_empty(),
            Value::Object(x) => !x.is_empty(),
        }
    }

    pub fn is_object(&self) -> bool {
        self.as_object().is_some()
    }

    pub fn as_object(&self) -> Option<&Map<String, Value>> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> Option<&mut Map<String, Value>> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    pub fn is_array(&self) -> bool {
        self.as_array().is_some()
    }

    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_array_mut(&mut self) -> Option<&mut Vec<Value>> {
        match self {
            Value::Array(list) => Some(list),
            _ => None,
        }
    }

    pub fn is_string(&self) -> bool {
        self.as_str().is_some()
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn is_number(&self) -> bool {
        matches!(*self, Value::Int(_) | Value::UInt(_) | Value::Float(_))
    }

    pub fn is_i64(&self) -> bool {
        matches!(self, Value::Int(_) | Value::UInt(_))
    }

    pub fn is_f64(&self) -> bool {
        matches!(self, Value::Float(_))
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(n) => Some(*n),
            Value::UInt(n) if *n < i64::MAX as u64 => Some(*n as i64),
            Value::Float(n) => Some(*n as i64),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Int(n) => Some(*n as u64),
            Value::UInt(n) => Some(*n),
            Value::Float(n) => Some(*n as u64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Int(n) => Some(*n as f64),
            Value::UInt(n) => Some(*n as f64),
            Value::Float(n) => Some(*n),
            _ => None,
        }
    }

    pub fn is_boolean(&self) -> bool {
        self.as_bool().is_some()
    }

    pub fn as_bool(&self) -> Option<bool> {
        match *self {
            Value::Bool(b) => Some(b),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        self.as_null().is_some()
    }

    pub fn as_null(&self) -> Option<()> {
        match *self {
            Value::Null => Some(()),
            _ => None,
        }
    }
}
