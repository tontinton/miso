use std::fmt;

use hashbrown::HashSet;
use serde::{Deserialize, Serialize};

use crate::{field::Field, value::Value};

macro_rules! fields_binop {
    ($l:expr, $r:expr, $out:expr) => {{
        $l._fields($out);
        $r._fields($out);
    }};
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Expr {
    Field(Field),
    Literal(Value),
    Exists(Field),
    Cast(CastType, Box<Expr>),

    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),

    In(Box<Expr>, Vec<Expr>), // like python's in
    Bin(Box<Expr>, Box<Expr>),
    Case(Vec<(Expr, Expr)>, Box<Expr>), // switch case default

    Contains(Box<Expr>, Box<Expr>),   // string - left.contains(right)
    StartsWith(Box<Expr>, Box<Expr>), // string - left.starts_with(right)
    EndsWith(Box<Expr>, Box<Expr>),   // string - left.ends_with(right)
    Has(Box<Expr>, Box<Expr>),        // string - left.contains_phrase(right)
    HasCs(Box<Expr>, Box<Expr>),      // string - left.contains_phrase_cs(right)

    Eq(Box<Expr>, Box<Expr>),
    Ne(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Gte(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    Lte(Box<Expr>, Box<Expr>),

    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
    Plus(Box<Expr>, Box<Expr>),
    Minus(Box<Expr>, Box<Expr>),
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CastType {
    Bool,
    Float,
    Int,
    String,
}

impl fmt::Display for CastType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CastType::Bool => write!(f, "bool"),
            CastType::Float => write!(f, "float"),
            CastType::Int => write!(f, "int"),
            CastType::String => write!(f, "string"),
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Field(field) => write!(f, "{field}"),
            Expr::Literal(val) => write!(f, "{val}"),
            Expr::Exists(field) => write!(f, "exists({field})"),
            Expr::Cast(ty, expr) => write!(f, "cast({expr}, {ty})"),

            Expr::Or(lhs, rhs) => write!(f, "({lhs} or {rhs})"),
            Expr::And(lhs, rhs) => write!(f, "({lhs} and {rhs})"),
            Expr::Not(expr) => write!(f, "not({expr})"),

            Expr::In(lhs, list) => {
                write!(f, "({lhs} in [")?;
                let mut first = true;
                for item in list {
                    if !first {
                        write!(f, ", ")?;
                    }
                    first = false;
                    write!(f, "{item}")?;
                }
                write!(f, "])")
            }
            Expr::Bin(lhs, rhs) => {
                write!(f, "({lhs} bin by {rhs})")
            }
            Expr::Case(predicates, default) => {
                write!(f, "case(")?;
                let mut first = true;
                for (predicate, then) in predicates {
                    if !first {
                        write!(f, ", ")?;
                    }
                    first = false;
                    write!(f, "{predicate}, {then}")?;
                }
                write!(f, ", {default})")
            }

            Expr::Contains(lhs, rhs) => write!(f, "{lhs} contains {rhs}"),
            Expr::StartsWith(lhs, rhs) => write!(f, "{lhs} starts_with {rhs}"),
            Expr::EndsWith(lhs, rhs) => write!(f, "{lhs} ends_with {rhs}"),
            Expr::Has(lhs, rhs) => write!(f, "{lhs} has {rhs}"),
            Expr::HasCs(lhs, rhs) => write!(f, "{lhs} has_cs {rhs}"),
            Expr::Eq(lhs, rhs) => write!(f, "{lhs} == {rhs}"),
            Expr::Ne(lhs, rhs) => write!(f, "{lhs} != {rhs}"),
            Expr::Gt(lhs, rhs) => write!(f, "{lhs} > {rhs}"),
            Expr::Gte(lhs, rhs) => write!(f, "{lhs} >= {rhs}"),
            Expr::Lt(lhs, rhs) => write!(f, "{lhs} < {rhs}"),
            Expr::Lte(lhs, rhs) => write!(f, "{lhs} <= {rhs}"),
            Expr::Mul(lhs, rhs) => write!(f, "{lhs} * {rhs}"),
            Expr::Div(lhs, rhs) => write!(f, "{lhs} / {rhs}"),
            Expr::Plus(lhs, rhs) => write!(f, "{lhs} + {rhs}"),
            Expr::Minus(lhs, rhs) => write!(f, "{lhs} - {rhs}"),
        }
    }
}

impl Expr {
    pub(crate) fn _fields(&self, out: &mut HashSet<Field>) {
        match self {
            Expr::Field(f) | Expr::Exists(f) => {
                out.insert(f.clone());
            }

            Expr::Literal(_) => {}

            Expr::Not(e) | Expr::Cast(_, e) => e._fields(out),

            Expr::In(e, arr) => {
                e._fields(out);
                for item in arr {
                    item._fields(out);
                }
            }
            Expr::Case(predicates, default) => {
                for (predicate, then) in predicates {
                    predicate._fields(out);
                    then._fields(out);
                }
                default._fields(out);
            }

            Expr::Bin(l, r) => fields_binop!(l, r, out),
            Expr::Or(l, r) => fields_binop!(l, r, out),
            Expr::And(l, r) => fields_binop!(l, r, out),
            Expr::Contains(l, r) => fields_binop!(l, r, out),
            Expr::StartsWith(l, r) => fields_binop!(l, r, out),
            Expr::EndsWith(l, r) => fields_binop!(l, r, out),
            Expr::Has(l, r) => fields_binop!(l, r, out),
            Expr::HasCs(l, r) => fields_binop!(l, r, out),
            Expr::Eq(l, r) => fields_binop!(l, r, out),
            Expr::Ne(l, r) => fields_binop!(l, r, out),
            Expr::Gt(l, r) => fields_binop!(l, r, out),
            Expr::Gte(l, r) => fields_binop!(l, r, out),
            Expr::Lt(l, r) => fields_binop!(l, r, out),
            Expr::Lte(l, r) => fields_binop!(l, r, out),
            Expr::Mul(l, r) => fields_binop!(l, r, out),
            Expr::Div(l, r) => fields_binop!(l, r, out),
            Expr::Plus(l, r) => fields_binop!(l, r, out),
            Expr::Minus(l, r) => fields_binop!(l, r, out),
        }
    }

    pub fn fields(&self) -> HashSet<Field> {
        let mut out = HashSet::new();
        self._fields(&mut out);
        out
    }
}
