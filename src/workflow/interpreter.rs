use std::collections::BTreeMap;

use color_eyre::eyre::{bail, eyre, Result};
use kinded::Kinded;
use serde::{Deserialize, Serialize};

use crate::log::Log;

macro_rules! impl_two_strs_fn {
    ($l:expr, $r:expr, $func:expr, $op_str:literal) => {{
        let lhs_v = $l;
        let rhs_v = $r;

        let Val::Str(lhs) = lhs_v else {
            bail!("LHS of '{}' operation must be a string", $op_str);
        };

        let Val::Str(rhs) = rhs_v else {
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
    ($l:expr, $r:expr, $cmp:expr, $op_str:literal) => {{
        let lhs_v = $l;
        let rhs_v = $r;
        let lhs_kind = lhs_v.kind();
        let rhs_kind = rhs_v.kind();

        match (lhs_v, rhs_v) {
            (Val::NotExist, _) | (_, Val::NotExist) => false,
            (Val::Null, Val::Null) => $cmp(0, 0),
            (Val::Bool(x), Val::Bool(y)) => $cmp(x, y),
            (Val::Int(x), Val::Int(y)) => $cmp(x, y),
            (Val::Float(x), Val::Float(y)) => $cmp(x, y),
            (Val::Str(x), Val::Str(y)) => $cmp(x, y),
            _ => bail!("unsupported '{}' between: {lhs_kind}, {rhs_kind}", $op_str),
        }
    }};
}

macro_rules! impl_op {
    ($l:expr, $r:expr, $op:expr, $op_str:literal) => {{
        let lhs = $l;
        let rhs = $r;
        Ok(match (lhs, rhs) {
            (Val::NotExist, _) | (_, Val::NotExist) => Val::NotExist,
            (Val::Int(x), Val::Int(y)) => Val::Int($op(x, y)),
            (Val::Float(x), Val::Float(y)) => Val::Float($op(x, y)),
            (Val::Int(x), Val::Float(y)) => Val::Float($op(*x as f64, y)),
            (Val::Float(x), Val::Int(y)) => Val::Float($op(x, *y as f64)),
            _ => bail!(
                "unsupported '{}' between: {}, {}",
                $op_str,
                lhs.kind(),
                rhs.kind()
            ),
        })
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

#[derive(Kinded, Debug)]
pub enum Val {
    /// The field doesn't exist.
    NotExist,
    // Explicit null.
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Obj(BTreeMap<String, Val>),
}

impl Val {
    pub fn to_bool(&self) -> bool {
        match self {
            Val::NotExist | Val::Null => false,
            Val::Bool(x) => *x,
            Val::Int(x) => *x > 0,
            Val::Float(x) => *x > 0.0,
            Val::Str(x) => !x.is_empty(),
            Val::Obj(x) => !x.is_empty(),
        }
    }

    pub fn to_vrl(&self) -> vrl::value::Value {
        use vrl::value::Value;

        match self {
            Val::NotExist | Val::Null => Value::Null,
            Val::Bool(x) => Value::Boolean(*x),
            Val::Int(x) => Value::Integer(*x),
            Val::Float(x) => Value::from_f64_or_zero(*x),
            Val::Str(x) => Value::Bytes(x.clone().into_bytes().into()),
            Val::Obj(map) => Value::Object(
                map.iter()
                    .map(|(k, v)| (k.clone().into(), v.to_vrl()))
                    .collect(),
            ),
        }
    }

    pub fn eq(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |x, y| x == y, "=="))
    }

    pub fn ne(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |x, y| x != y, "!="))
    }

    pub fn gt(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |x, y| x > y, ">"))
    }

    pub fn gte(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |x, y| x >= y, ">="))
    }

    pub fn lt(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |x, y| x < y, "<"))
    }

    pub fn lte(&self, other: &Val) -> Result<bool> {
        Ok(impl_cmp!(self, other, |x, y| x <= y, "<="))
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

    pub fn add(&self, other: &Val) -> Result<Val> {
        if let (Val::Str(x), Val::Str(y)) = (self, other) {
            return Ok(Val::Str(format!("{x}{y}")));
        }
        impl_op!(self, other, |x, y| x + y, "+")
    }

    pub fn sub(&self, other: &Val) -> Result<Val> {
        impl_op!(self, other, |x, y| x - y, "-")
    }

    pub fn mul(&self, other: &Val) -> Result<Val> {
        impl_op!(self, other, |x, y| x * y, "*")
    }

    pub fn div(&self, other: &Val) -> Result<Val> {
        if matches!(other, Val::Int(0) | Val::Float(0.0)) {
            bail!("division by zero");
        }
        impl_op!(self, other, |x, y| x / y, "/")
    }

    pub fn cast(self, ty: CastType) -> Result<Val> {
        if matches!(self, Val::NotExist) {
            return Ok(self);
        }

        Ok(match ty {
            CastType::Bool => Val::Bool(self.to_bool()),
            CastType::Int => Val::Int(match self {
                Val::NotExist | Val::Null => 0,
                Val::Bool(x) => x as i64,
                Val::Int(x) => x,
                Val::Float(x) => x as i64,
                Val::Str(x) => x
                    .parse::<i64>()
                    .map_err(|_| eyre!("cannot cast string to int: {x}"))?,
                Val::Obj(..) => bail!("cannot cast object to int"),
            }),
            CastType::Float => Val::Float(match self {
                Val::NotExist | Val::Null => 0.0,
                Val::Bool(x) => {
                    if x {
                        1.0
                    } else {
                        0.0
                    }
                }
                Val::Int(x) => x as f64,
                Val::Float(x) => x,
                Val::Str(x) => x
                    .parse::<f64>()
                    .map_err(|_| eyre!("cannot cast string to float: {x}"))?,
                Val::Obj(..) => bail!("cannot cast object to float"),
            }),
            CastType::String => Val::Str(match self {
                Val::NotExist | Val::Null => "null".to_string(),
                Val::Bool(x) => x.to_string(),
                Val::Int(x) => x.to_string(),
                Val::Float(x) => x.to_string(),
                Val::Str(x) => x,
                Val::Obj(..) => bail!("cannot cast object to string"),
            }),
        })
    }
}

pub fn ident(log: &Log, name: &str) -> Result<Val> {
    let split: Vec<_> = name.split('.').collect();

    let mut obj = log;
    for key in &split[..split.len() - 1] {
        obj = match obj.get(*key) {
            Some(vrl::value::Value::Object(v)) => v,
            _ => return Ok(Val::NotExist),
        };
    }

    match obj.get(split[split.len() - 1]) {
        None => Ok(Val::NotExist),
        Some(value) => vrl_to_val(value),
    }
}

pub fn vrl_to_val(value: &vrl::value::Value) -> Result<Val> {
    Ok(match value {
        vrl::value::Value::Bytes(bytes) => Val::Str(String::from_utf8(bytes.to_vec())?),
        vrl::value::Value::Null => Val::Null,
        vrl::value::Value::Integer(x) => Val::Int(*x),
        vrl::value::Value::Float(x) => Val::Float(**x),
        vrl::value::Value::Boolean(x) => Val::Bool(*x),
        vrl::value::Value::Object(map) => {
            let mut out = BTreeMap::new();
            for (k, v) in map {
                out.insert(k.as_str().to_string(), vrl_to_val(v)?);
            }
            Val::Obj(out)
        }
        vrl::value::Value::Array(..) => todo!("arrays are currently unsupported"),
        vrl::value::Value::Timestamp(..) => todo!("timestamps are currently unsupported"),
        vrl::value::Value::Regex(..) => todo!("regexes are currently unsupported"),
    })
}

pub fn serde_json_to_val(value: &serde_json::Value) -> Result<Val> {
    Ok(match value {
        serde_json::Value::Null => Val::Null,
        serde_json::Value::Number(x) => {
            if let Some(x) = x.as_i64() {
                Val::Int(x)
            } else if let Some(x) = x.as_f64() {
                Val::Float(x)
            } else {
                bail!("'{x}' couldn't be parsed as either an i64 or a f64");
            }
        }
        serde_json::Value::String(x) => Val::Str(x.clone()),
        serde_json::Value::Bool(x) => Val::Bool(*x),
        serde_json::Value::Object(map) => {
            let mut out = BTreeMap::new();
            for (k, v) in map {
                out.insert(k.clone(), serde_json_to_val(v)?);
            }
            Val::Obj(out)
        }
        serde_json::Value::Array(..) => {
            bail!("arrays are currently unsupported");
        }
    })
}
