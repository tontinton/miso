mod log_interpreter;
mod string_ops;

pub use log_interpreter::LogInterpreter;

#[cfg(test)]
mod tests;

use std::{borrow::Cow, cmp::Ordering};

use color_eyre::eyre::{Result, bail, eyre};
use miso_workflow_types::{
    expr::CastType,
    field::{Field, FieldAccess},
    log::Log,
    value::{Map, Value},
};
use time::{Duration, OffsetDateTime};

/// Extract the inner value as ref, propagate and return None if no value.
macro_rules! val {
    ($v:expr) => {{
        match &$v.0 {
            Some(cow) => cow.as_ref(),
            None => return Ok(None),
        }
    }};
}

macro_rules! impl_two_strs_fn {
    ($l:expr, $r:expr, $func:expr, $op_str:literal) => {{
        let lhs_val = val!($l);
        let rhs_val = val!($r);

        let Value::String(lhs) = lhs_val else {
            bail!("LHS of '{}' operation must be a string", $op_str);
        };
        let Value::String(rhs) = rhs_val else {
            bail!("RHS of '{}' operation must be a string", $op_str);
        };

        Ok(Some(if rhs.is_empty() {
            true
        } else {
            $func(lhs, rhs)
        }))
    }};
}

macro_rules! impl_cmp {
    ($self:expr, $other:expr, $cmp:expr, $op:expr) => {
        match (&$self.0, &$other.0) {
            (None, None) | (None, Some(_)) | (Some(_), None) => Ok(None),
            (Some(lhs_cow), Some(rhs_cow)) => {
                let lhs = lhs_cow.as_ref();
                let rhs = rhs_cow.as_ref();
                Ok(Some($cmp(lhs.cmp(rhs))))
            }
        }
    };
}

macro_rules! impl_op {
    ($lhs:expr, $rhs:expr, $op:expr, $op_str:literal) => {{
        use Value::*;

        match ($lhs, $rhs) {
            (Int(l), Int(r)) if $op_str != "/" => {
                return Ok(Some(Int($op(l, r))));
            }
            (UInt(l), UInt(r)) if $op_str != "/" => {
                return Ok(Some(UInt($op(l, r))));
            }
            (Int(l), UInt(r)) if $op_str != "/" => {
                if *l < 0 {
                    return Ok(Some(Int($op(*l, *r as i64))));
                } else {
                    return Ok(Some(UInt($op(*l as u64, *r))));
                }
            }
            (UInt(l), Int(r)) if $op_str != "/" => {
                if *r < 0 {
                    return Ok(Some(Int($op(*l as i64, *r))));
                } else {
                    return Ok(Some(UInt($op(*l, *r as u64))));
                }
            }

            (Int(l), Int(r)) => {
                return Ok(Some(Float($op(*l as f64, *r as f64))));
            }
            (UInt(l), UInt(r)) => {
                return Ok(Some(Float($op(*l as f64, *r as f64))));
            }
            (Int(l), UInt(r)) => {
                return Ok(Some(Float($op(*l as f64, *r as f64))));
            }
            (UInt(l), Int(r)) => {
                return Ok(Some(Float($op(*l as f64, *r as f64))));
            }
            (Float(l), Float(r)) => {
                return Ok(Some(Float($op(*l, *r))));
            }
            (Float(l), Int(r)) => {
                return Ok(Some(Float($op(*l, *r as f64))));
            }
            (Int(l), Float(r)) => {
                return Ok(Some(Float($op(*l as f64, *r))));
            }
            (Float(l), UInt(r)) => {
                return Ok(Some(Float($op(*l, *r as f64))));
            }
            (UInt(l), Float(r)) => {
                return Ok(Some(Float($op(*l as f64, *r))));
            }

            _ => bail!(
                "unsupported '{}' operation between: {}, {}",
                $op_str,
                $lhs.kind(),
                $rhs.kind()
            ),
        }
    }};
}

#[derive(Debug)]
pub struct Val<'a>(pub Option<Cow<'a, Value>>);

impl From<Option<bool>> for Val<'_> {
    fn from(value: Option<bool>) -> Self {
        value.map_or(Val::not_exist(), Val::bool)
    }
}

impl From<Option<Value>> for Val<'_> {
    fn from(value: Option<Value>) -> Self {
        value.map_or(Val::not_exist(), Val::owned)
    }
}

impl<'a> Val<'a> {
    pub fn not_exist() -> Val<'a> {
        Val(None)
    }

    pub fn owned(value: Value) -> Val<'a> {
        Val(Some(Cow::Owned(value)))
    }

    pub fn borrowed(value: &'a Value) -> Val<'a> {
        Val(Some(Cow::Borrowed(value)))
    }

    pub fn bool(value: bool) -> Val<'a> {
        Val(Some(Cow::Owned(Value::Bool(value))))
    }

    pub fn is_exist(&self) -> bool {
        self.0.is_some()
    }

    pub fn to_bool(&self) -> bool {
        let Some(cow) = &self.0 else {
            return false;
        };
        cow.as_ref().to_bool()
    }

    pub fn eq(&self, other: &Val) -> Result<Option<bool>> {
        impl_cmp!(self, other, |o| o == Ordering::Equal, "==")
    }

    pub fn ne(&self, other: &Val) -> Result<Option<bool>> {
        impl_cmp!(self, other, |o| o != Ordering::Equal, "!=")
    }

    pub fn gt(&self, other: &Val) -> Result<Option<bool>> {
        impl_cmp!(self, other, |o| o == Ordering::Greater, ">")
    }

    pub fn gte(&self, other: &Val) -> Result<Option<bool>> {
        impl_cmp!(self, other, |o| o >= Ordering::Equal, ">=")
    }

    pub fn lt(&self, other: &Val) -> Result<Option<bool>> {
        impl_cmp!(self, other, |o| o == Ordering::Less, "<")
    }

    pub fn lte(&self, other: &Val) -> Result<Option<bool>> {
        impl_cmp!(self, other, |o| o <= Ordering::Equal, "<=")
    }

    pub fn is_in(&self, others: &[Val]) -> Result<Option<bool>> {
        if !self.is_exist() {
            return Ok(None);
        }

        for other in others {
            if matches!(self.eq(other)?, Some(true)) {
                return Ok(Some(true));
            }
        }
        Ok(Some(false))
    }

    pub fn contains(&self, other: &Val) -> Result<Option<bool>> {
        impl_two_strs_fn!(self, other, |x: &str, y: &str| x.contains(y), "contains")
    }

    pub fn starts_with(&self, other: &Val) -> Result<Option<bool>> {
        impl_two_strs_fn!(
            self,
            other,
            |x: &str, y: &str| x.starts_with(y),
            "starts_with"
        )
    }

    pub fn ends_with(&self, other: &Val) -> Result<Option<bool>> {
        impl_two_strs_fn!(self, other, |x: &str, y: &str| x.ends_with(y), "ends_with")
    }

    pub fn has(&self, other: &Val) -> Result<Option<bool>> {
        impl_two_strs_fn!(self, other, string_ops::has, "has")
    }

    pub fn has_cs(&self, other: &Val) -> Result<Option<bool>> {
        impl_two_strs_fn!(self, other, string_ops::has_cs, "has_cs")
    }

    pub fn add(&self, other: &Val) -> Result<Option<Value>> {
        let lhs = val!(self);
        let rhs = val!(other);

        match (lhs, rhs) {
            (Value::String(x), Value::String(y)) => Ok(Some(Value::String(format!("{x}{y}")))),

            (Value::Timestamp(ts), Value::Timespan(dur))
            | (Value::Timespan(dur), Value::Timestamp(ts)) => match ts.checked_add(*dur) {
                Some(result) => Ok(Some(Value::Timestamp(result))),
                None => bail!("timestamp overflow in addition"),
            },

            (Value::Timespan(dur1), Value::Timespan(dur2)) => match dur1.checked_add(*dur2) {
                Some(result) => Ok(Some(Value::Timespan(result))),
                None => bail!("duration overflow in addition"),
            },

            _ => impl_op!(lhs, rhs, |x, y| x + y, "+"),
        }
    }

    pub fn sub(&self, other: &Val) -> Result<Option<Value>> {
        let lhs = val!(self);
        let rhs = val!(other);

        match (lhs, rhs) {
            (Value::Timestamp(ts), Value::Timespan(dur)) => match ts.checked_sub(*dur) {
                Some(result) => Ok(Some(Value::Timestamp(result))),
                None => bail!("timestamp underflow in subtraction"),
            },

            (Value::Timestamp(ts1), Value::Timestamp(ts2)) => {
                Ok(Some(Value::Timespan(*ts1 - *ts2)))
            }

            (Value::Timespan(dur1), Value::Timespan(dur2)) => match dur1.checked_sub(*dur2) {
                Some(result) => Ok(Some(Value::Timespan(result))),
                None => bail!("duration underflow in subtraction"),
            },

            _ => impl_op!(lhs, rhs, |x, y| x - y, "-"),
        }
    }

    pub fn mul(&self, other: &Val) -> Result<Option<Value>> {
        let lhs = val!(self);
        let rhs = val!(other);

        match (lhs, rhs) {
            (Value::Timespan(dur), Value::Int(n)) | (Value::Int(n), Value::Timespan(dur)) => {
                match dur.checked_mul(*n as i32) {
                    Some(result) => Ok(Some(Value::Timespan(result))),
                    None => bail!("duration overflow in multiplication"),
                }
            }

            (Value::Timespan(dur), Value::UInt(n)) | (Value::UInt(n), Value::Timespan(dur)) => {
                match dur.checked_mul(*n as i32) {
                    Some(result) => Ok(Some(Value::Timespan(result))),
                    None => bail!("duration overflow in multiplication"),
                }
            }

            (Value::Timespan(dur), Value::Float(n)) | (Value::Float(n), Value::Timespan(dur)) => {
                let nanos = dur.whole_nanoseconds() as f64 * n;
                let duration = Duration::nanoseconds(nanos as i64);
                Ok(Some(Value::Timespan(duration)))
            }

            _ => impl_op!(lhs, rhs, |x, y| x * y, "*"),
        }
    }

    pub fn div(&self, other: &Val) -> Result<Option<Value>> {
        let lhs = val!(self);
        let rhs = val!(other);

        match (lhs, rhs) {
            (_, Value::Int(n)) if *n == 0 => {
                bail!("cannot divide by zero");
            }
            (_, Value::UInt(n)) if *n == 0 => {
                bail!("cannot divide by zero");
            }
            (_, Value::Float(n)) if *n == 0.0 => {
                bail!("cannot divide by zero");
            }
            (_, Value::Timespan(d)) if d.is_zero() => {
                bail!("cannot divide by zero");
            }

            (Value::Timespan(dur), Value::Int(n)) => match dur.checked_div(*n as i32) {
                Some(result) => Ok(Some(Value::Timespan(result))),
                None => bail!("duration overflow in division"),
            },

            (Value::Timespan(dur), Value::UInt(n)) => match dur.checked_div(*n as i32) {
                Some(result) => Ok(Some(Value::Timespan(result))),
                None => bail!("duration overflow in division"),
            },

            (Value::Timespan(dur), Value::Float(n)) => {
                let nanos = dur.whole_nanoseconds() as f64 / n;
                Ok(Some(Value::Timespan(Duration::nanoseconds(nanos as i64))))
            }

            (Value::Timespan(dur1), Value::Timespan(dur2)) => {
                let ratio = dur1.whole_nanoseconds() as f64 / dur2.whole_nanoseconds() as f64;
                Ok(Some(Value::Float(ratio)))
            }

            _ => impl_op!(lhs, rhs, |x, y| x / y, "/"),
        }
    }

    pub fn cast(self, ty: CastType) -> Result<Val<'a>> {
        let Some(cow) = self.0 else {
            return Ok(Val::not_exist());
        };

        let casted_value = match ty {
            CastType::Bool => Value::from(cow.as_ref().to_bool()),
            CastType::Float => Value::from(match cow.as_ref() {
                Value::Null => 0.0,
                Value::Bool(x) => {
                    if *x {
                        1.0
                    } else {
                        0.0
                    }
                }
                Value::Int(x) => *x as f64,
                Value::UInt(x) => *x as f64,
                Value::Float(x) => *x,
                Value::String(x) => x
                    .parse::<f64>()
                    .map_err(|_| eyre!("cannot cast '{x}' to float"))?,
                _ => bail!("cannot cast '{}' to float", cow.as_ref().kind()),
            }),
            CastType::Int => Value::from(match cow.as_ref() {
                Value::Null => 0,
                Value::Bool(x) => *x as i64,
                Value::Int(x) => *x,
                Value::UInt(x) => *x as i64, // Can overflow.
                Value::Float(x) => *x as i64,
                Value::String(x) => x
                    .parse::<i64>()
                    .map_err(|_| eyre!("Cannot cast '{x}' to int"))?,
                _ => bail!("cannot cast '{}' to int", cow.as_ref().kind()),
            }),
            CastType::String => {
                if let Cow::Owned(Value::String(x)) = cow {
                    // Optimization: don't clone string if owned.
                    return Ok(Val::owned(Value::String(x)));
                }

                Value::from(match cow.as_ref() {
                    Value::Null => "null".to_string(),
                    Value::Bool(x) => x.to_string(),
                    Value::Int(x) => x.to_string(),
                    Value::UInt(x) => x.to_string(),
                    Value::Float(x) => x.to_string(),
                    Value::String(x) => x.clone(),
                    _ => bail!("cannot cast '{}' to string", cow.as_ref().kind()),
                })
            }
        };

        Ok(Val::owned(casted_value))
    }

    pub fn bin(&self, by: &Val) -> Result<Option<Value>> {
        let self_val = val!(self);
        let by_val = val!(by);

        match (self_val, by_val) {
            (Value::Timestamp(ts), Value::Timespan(dur)) => {
                if dur.is_zero() {
                    bail!("cannot bin timestamp by zero duration");
                }
                if dur.is_negative() {
                    bail!("cannot bin timestamp by negative duration");
                }

                let ts_nanos = ts.unix_timestamp_nanos();
                let dur_nanos = dur.whole_nanoseconds();

                let binned_nanos = (ts_nanos / dur_nanos) * dur_nanos;

                let binned_ts = OffsetDateTime::from_unix_timestamp_nanos(binned_nanos)
                    .map_err(|_| eyre!("timestamp overflow in binning operation"))?;
                Ok(Some(Value::Timestamp(binned_ts)))
            }

            _ => {
                let Some(a) = self_val.as_f64() else {
                    bail!(
                        "cannot bin '{}', currently only numbers and timestamps are supported",
                        self_val.kind()
                    );
                };
                let Some(b) = by_val.as_f64() else {
                    bail!(
                        "cannot bin by '{}', currently only numbers and timespans are supported",
                        by_val.kind()
                    );
                };

                if b == 0.0 {
                    bail!("cannot bin by zero");
                }

                Ok(Some(Value::from((a / b).floor() * b)))
            }
        }
    }
}

fn ident<'a>(log: &'a Log, field: &Field) -> Val<'a> {
    get_field_value(log, field).map_or_else(Val::not_exist, Val::borrowed)
}

fn follow_key<'a>(obj: &'a Log, key: &FieldAccess) -> Option<&'a Value> {
    let mut val = obj.get(&key.name)?;
    for &idx in &key.arr_indices {
        val = val.as_array()?.get(idx)?;
    }
    Some(val)
}

fn follow_key_mut<'a>(obj: &'a mut Log, key: &FieldAccess) -> Option<&'a mut Value> {
    let mut val = obj.get_mut(&key.name)?;
    for &idx in &key.arr_indices {
        val = val.as_array_mut()?.get_mut(idx)?;
    }
    Some(val)
}

pub fn get_field_value<'a>(log: &'a Log, field: &Field) -> Option<&'a Value> {
    let mut obj = log;
    for key in &field[..field.len() - 1] {
        obj = match follow_key(obj, key) {
            Some(Value::Object(map)) => map,
            _ => return None,
        };
    }

    let last = field.last().unwrap();
    follow_key(obj, last)
}

pub fn insert_field_value(log: &mut Log, field: &Field, value: Value) {
    fn insert_part<'a>(current: &'a mut Value, keys: &[FieldAccess]) -> &'a mut Value {
        if keys.is_empty() {
            return current;
        }

        if !current.is_object() {
            *current = Value::Object(Map::new());
        }
        let map = current.as_object_mut().unwrap();
        let key = &keys[0];
        let entry = map.entry(key.name.clone()).or_insert_with(|| {
            if !key.arr_indices.is_empty() {
                Value::Array(vec![])
            } else {
                Value::Object(Map::new())
            }
        });

        let mut current_value = entry;
        for &idx in &key.arr_indices {
            if !current_value.is_array() {
                *current_value = Value::Array(vec![]);
            }
            let arr = current_value.as_array_mut().unwrap();
            while arr.len() <= idx {
                arr.push(Value::Null);
            }
            current_value = &mut arr[idx];
        }

        insert_part(current_value, &keys[1..])
    }

    if field.is_empty() {
        return;
    }
    let mut root = Value::Object(std::mem::take(log));
    *insert_part(&mut root, field) = value;
    if let Value::Object(final_map) = root {
        *log = final_map;
    }
}

pub fn rename_field(log: &mut Log, from: &Field, to: &Field) -> bool {
    if from.is_empty() {
        return false;
    }

    let value = match extract_field(log, from) {
        Some(val) => val,
        None => return false,
    };

    if !to.is_empty() {
        insert_field_value(log, to, value);
    }

    true
}

pub fn extract_field(log: &mut Log, field: &Field) -> Option<Value> {
    let mut obj = log;
    for key in &field[..field.len() - 1] {
        obj = match follow_key_mut(obj, key) {
            Some(Value::Object(map)) => map,
            _ => return None,
        };
    }

    let last = field.last().unwrap();
    if last.arr_indices.is_empty() {
        return obj.remove(&last.name);
    }

    let mut val = obj.get_mut(&last.name)?;
    for &idx in &last.arr_indices[..last.arr_indices.len() - 1] {
        val = val.as_array_mut()?.get_mut(idx)?;
    }

    let final_idx = *last.arr_indices.last()?;
    let arr = val.as_array_mut()?;
    (final_idx < arr.len()).then(|| arr.remove(final_idx))
}
