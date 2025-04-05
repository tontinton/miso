use std::{borrow::Cow, cmp::Ordering};

use color_eyre::eyre::{bail, eyre, OptionExt, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{log::Log, workflow::serde_json_utils::get_value_kind};

use super::serde_json_utils::{partial_cmp_values, value_to_bool};

macro_rules! impl_two_strs_fn {
    ($l:expr, $r:expr, $func:expr, $op_str:literal) => {{
        let lhs_val = match &$l.0 {
            Some(cow) => cow.as_ref(),
            None => bail!("LHS of '{}' operation must exist in log", $op_str),
        };

        let rhs_val = match &$r.0 {
            Some(cow) => cow.as_ref(),
            None => bail!("RHS of '{}' operation must exist in log", $op_str),
        };

        let Value::String(lhs) = lhs_val else {
            bail!("LHS of '{}' operation must be a string", $op_str);
        };

        let Value::String(rhs) = rhs_val else {
            bail!("RHS of '{}' operation must be a string", $op_str);
        };

        if rhs.is_empty() {
            true
        } else {
            $func(lhs, rhs)
        }
    }};
}

macro_rules! impl_cmp {
    ($self:expr, $other:expr, $cmp:expr, $op:expr) => {
        match (&$self.0, &$other.0) {
            (None, None) | (None, Some(_)) | (Some(_), None) => false,
            (Some(lhs_cow), Some(rhs_cow)) => {
                let lhs = lhs_cow.as_ref();
                let rhs = rhs_cow.as_ref();
                match partial_cmp_values(lhs, rhs) {
                    Some(ord) => $cmp(ord),
                    _ => {
                        let lhs_kind = get_value_kind(lhs);
                        let rhs_kind = get_value_kind(rhs);
                        bail!("unsupported '{}' between: {}, {}", $op, lhs_kind, rhs_kind);
                    }
                }
            }
        }
    };
}

macro_rules! impl_op {
    ($self:expr, $other:expr, $op:expr, $op_str:literal) => {{
        let lhs_val = match &$self.0 {
            Some(cow) => cow.as_ref(),
            None => bail!("LHS of '{}' operation must exist in log", $op_str),
        };

        let rhs_val = match &$other.0 {
            Some(cow) => cow.as_ref(),
            None => bail!("RHS of '{}' operation must exist in log", $op_str),
        };

        match (lhs_val, rhs_val) {
            (Value::Number(l), Value::Number(r)) => {
                if let (Some(l_i64), Some(r_i64)) = (l.as_i64(), r.as_i64()) {
                    let result = $op(l_i64, r_i64);
                    return Ok(Val::owned(Value::from(result)));
                } else if let (Some(l_f64), Some(r_f64)) = (l.as_f64(), r.as_f64()) {
                    let result = $op(l_f64, r_f64);
                    match serde_json::Number::from_f64(result) {
                        Some(num) => return Ok(Val::owned(Value::Number(num))),
                        None => bail!(
                            "Result of '{}' operation is not a valid JSON number",
                            $op_str
                        ),
                    }
                }
            }
            _ => {}
        }

        let lhs_kind = get_value_kind(lhs_val);
        let rhs_kind = get_value_kind(rhs_val);
        bail!(
            "unsupported '{}' operation between: {}, {}",
            $op_str,
            lhs_kind,
            rhs_kind
        )
    }};
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CastType {
    Bool,
    Float,
    Int,
    String,
}

#[derive(Debug)]
pub struct Val<'a>(pub Option<Cow<'a, Value>>);

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

    pub fn to_bool(&self) -> bool {
        let Some(cow) = &self.0 else {
            return false;
        };
        value_to_bool(cow.as_ref())
    }

    pub fn eq(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |o| o == Ordering::Equal, "=="))
    }

    pub fn ne(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |o| o != Ordering::Equal, "!="))
    }

    pub fn gt(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |o| o == Ordering::Greater, ">"))
    }

    pub fn gte(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |o| o >= Ordering::Equal, ">="))
    }

    pub fn lt(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |o| o == Ordering::Less, "<"))
    }

    pub fn lte(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |o| o <= Ordering::Equal, "<="))
    }

    pub fn contains(&self, other: &Val) -> Result<bool> {
        Ok(impl_two_strs_fn!(
            self,
            other,
            |x: &str, y: &str| x.contains(y),
            "contains"
        ))
    }

    pub fn starts_with(&self, other: &Val) -> Result<bool> {
        Ok(impl_two_strs_fn!(
            self,
            other,
            |x: &str, y: &str| x.starts_with(y),
            "starts_with"
        ))
    }

    pub fn ends_with(&self, other: &Val) -> Result<bool> {
        Ok(impl_two_strs_fn!(
            self,
            other,
            |x: &str, y: &str| x.ends_with(y),
            "ends_with"
        ))
    }

    pub fn add(self, other: Val) -> Result<Val> {
        if let (Some(lhs_cow), Some(rhs_cow)) = (&self.0, &other.0) {
            if let (Value::String(x), Value::String(y)) = (lhs_cow.as_ref(), rhs_cow.as_ref()) {
                return Ok(Val::owned(Value::String(format!("{}{}", x, y))));
            }
        }
        impl_op!(self, other, |x, y| x + y, "+")
    }

    pub fn sub(self, other: Val) -> Result<Val> {
        impl_op!(self, other, |x, y| x - y, "-")
    }

    pub fn mul(self, other: Val) -> Result<Val> {
        impl_op!(self, other, |x, y| x * y, "*")
    }

    pub fn div(self, other: Val) -> Result<Val> {
        if let Some(rhs_cow) = &other.0 {
            let rhs = rhs_cow.as_ref();
            if let Value::Number(n) = rhs {
                if n.as_i64() == Some(0) || n.as_f64() == Some(0.0) {
                    bail!("division by zero");
                }
            }
        }
        impl_op!(self, other, |x, y| x / y, "/")
    }

    pub fn cast(self, ty: CastType) -> Result<Val<'a>> {
        let Some(cow) = self.0 else {
            return Ok(Val::not_exist());
        };

        let casted_value = match ty {
            CastType::Bool => Value::from(value_to_bool(cow.as_ref())),
            CastType::Float => Value::from(match cow.as_ref() {
                Value::Null => 0.0,
                Value::Bool(x) => {
                    if *x {
                        1.0
                    } else {
                        0.0
                    }
                }
                Value::Number(x) => x.as_f64().ok_or_eyre("number not f64 / i64")?,
                Value::String(x) => x
                    .parse::<f64>()
                    .map_err(|_| eyre!("cannot cast '{x}' to float"))?,
                _ => bail!("cannot cast '{}' to float", get_value_kind(cow.as_ref())),
            }),
            CastType::Int => Value::from(match cow.as_ref() {
                Value::Null => 0,
                Value::Bool(x) => *x as i64,
                Value::Number(x) => {
                    if let Some(i) = x.as_i64() {
                        i
                    } else {
                        x.as_f64().ok_or_eyre("number not f64 / i64")? as i64
                    }
                }
                Value::String(x) => x
                    .parse::<i64>()
                    .map_err(|_| eyre!("Cannot cast '{x}' to int"))?,
                _ => bail!("cannot cast '{}' to int", get_value_kind(cow.as_ref())),
            }),
            CastType::String => {
                if let Cow::Owned(Value::String(x)) = cow {
                    // Optimization: don't clone string if owned.
                    return Ok(Val::owned(Value::String(x)));
                }

                Value::from(match cow.as_ref() {
                    Value::Null => "null".to_string(),
                    Value::Bool(x) => x.to_string(),
                    Value::Number(x) => x.to_string(),
                    Value::String(x) => x.clone(),
                    _ => bail!("cannot cast '{}' to string", get_value_kind(cow.as_ref())),
                })
            }
        };

        Ok(Val::owned(casted_value))
    }
}

pub fn ident<'a>(log: &'a Log, name: &str) -> Result<Val<'a>> {
    let split: Vec<_> = name.split('.').collect();

    let mut obj = log;
    for key in &split[..split.len() - 1] {
        obj = match obj.get(*key) {
            Some(Value::Object(v)) => v,
            _ => return Ok(Val::not_exist()),
        };
    }

    Ok(obj
        .get(split[split.len() - 1])
        .map_or_else(Val::not_exist, Val::borrowed))
}
