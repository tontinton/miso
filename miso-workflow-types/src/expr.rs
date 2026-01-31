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
    Extract(Box<Expr>, Box<Expr>, Box<Expr>), // extract(regex, captureGroup, source)

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
            Expr::Extract(regex, group, source) => {
                write!(f, "extract({regex}, {group}, {source})")
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

/// Extracted timestamp constraints from a filter expression
#[derive(Debug, Clone, Default)]
pub struct TimestampRange {
    pub earliest: Option<i64>,
    pub latest: Option<i64>,
}

impl Expr {
    /// Extract timestamp constraints from a filter expression.
    /// Returns the constraints and a modified expression without the timestamp predicates.
    ///
    /// The `is_timestamp` predicate determines which fields are timestamp fields.
    pub fn extract_timestamp_range<F>(&self, is_timestamp: F) -> (TimestampRange, Option<Expr>)
    where
        F: Fn(&Field) -> bool + Copy,
    {
        let mut range = TimestampRange::default();

        match self {
            Expr::Gte(lhs, rhs) => {
                if let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs)
                    && is_timestamp(field)
                    && let Some(epoch) = value.as_epoch_seconds()
                {
                    range.earliest = Some(epoch);
                    return (range, None);
                }
            }
            Expr::Gt(lhs, rhs) => {
                if let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs)
                    && is_timestamp(field)
                    && let Some(epoch) = value.as_epoch_seconds()
                {
                    // > is exclusive, so add 1 to make earliest inclusive
                    range.earliest = Some(epoch + 1);
                    return (range, None);
                }
            }
            Expr::Lt(lhs, rhs) => {
                if let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs)
                    && is_timestamp(field)
                    && let Some(epoch) = value.as_epoch_seconds()
                {
                    range.latest = Some(epoch);
                    return (range, None);
                }
            }
            Expr::Lte(lhs, rhs) => {
                if let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs)
                    && is_timestamp(field)
                    && let Some(epoch) = value.as_epoch_seconds()
                {
                    // <= is inclusive, so add 1 since latest is exclusive
                    range.latest = Some(epoch + 1);
                    return (range, None);
                }
            }
            Expr::And(left, right) => {
                let (left_range, left_remaining) = left.extract_timestamp_range(is_timestamp);
                let (right_range, right_remaining) = right.extract_timestamp_range(is_timestamp);

                range.earliest = match (left_range.earliest, right_range.earliest) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    (Some(a), None) | (None, Some(a)) => Some(a),
                    (None, None) => None,
                };
                range.latest = match (left_range.latest, right_range.latest) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (Some(a), None) | (None, Some(a)) => Some(a),
                    (None, None) => None,
                };

                let remaining = match (left_remaining, right_remaining) {
                    (Some(l), Some(r)) => Some(Expr::And(Box::new(l), Box::new(r))),
                    (Some(e), None) | (None, Some(e)) => Some(e),
                    (None, None) => None,
                };

                return (range, remaining);
            }
            _ => {}
        }

        (range, Some(self.clone()))
    }

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
            Expr::Extract(a, b, c) => {
                a._fields(out);
                b._fields(out);
                c._fields(out);
            }
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

    /// Try to invert a branch expression (like `case`) given a target value.
    ///
    /// For `case(cond, val_true, val_false)` with target:
    /// - `target == val_true` -> `cond`
    /// - `target == val_false` -> `NOT(cond)`
    /// - no match -> `false`
    ///
    /// For multi-branch `case(c1, v1, c2, v2, ..., default)`:
    /// - `target == v1` -> `c1`
    /// - `target == v2` -> `NOT(c1) AND c2`
    /// - `target == default` -> `NOT(c1) AND NOT(c2) AND ...`
    /// - multiple matches -> OR the conditions
    ///
    /// Returns `None` if the expression is not a branch or has non-literal values.
    pub fn try_invert_branch(&self, target: &Value) -> Option<Expr> {
        match self {
            Expr::Case(predicates, default) => invert_case(predicates, default, target),
            _ => None,
        }
    }
}

fn invert_case(predicates: &[(Expr, Expr)], default: &Expr, target: &Value) -> Option<Expr> {
    let Expr::Literal(default_val) = default else {
        return None;
    };

    let mut matching_conditions: Vec<Expr> = Vec::new();
    let mut negated_conditions: Vec<Expr> = Vec::new();

    for (cond, then_expr) in predicates {
        let Expr::Literal(then_val) = then_expr else {
            return None;
        };

        let branch_cond = and_all(negated_conditions.iter().cloned().chain([cond.clone()]));
        if then_val == target {
            matching_conditions.push(branch_cond);
        }
        negated_conditions.push(Expr::Not(Box::new(cond.clone())));
    }

    if default_val == target {
        if negated_conditions.is_empty() {
            matching_conditions.push(Expr::Literal(Value::Bool(true)));
        } else {
            matching_conditions.push(and_all(negated_conditions));
        }
    }

    if matching_conditions.is_empty() {
        Some(Expr::Literal(Value::Bool(false)))
    } else {
        Some(or_all(matching_conditions))
    }
}

fn fold_exprs(
    exprs: impl IntoIterator<Item = Expr>,
    combine: fn(Box<Expr>, Box<Expr>) -> Expr,
) -> Expr {
    exprs
        .into_iter()
        .reduce(|acc, e| combine(Box::new(acc), Box::new(e)))
        .expect("fold_exprs requires at least one element")
}

fn and_all(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    fold_exprs(exprs, Expr::And)
}

fn or_all(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    fold_exprs(exprs, Expr::Or)
}
