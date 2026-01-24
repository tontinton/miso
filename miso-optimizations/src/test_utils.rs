use std::str::FromStr;

use hashbrown::HashMap;
use miso_common::hashmap;
use miso_workflow::WorkflowStep as S;
use miso_workflow_types::{
    expand::Expand,
    expr::Expr,
    field::Field,
    field_unwrap,
    project::ProjectField,
    sort::{NullsOrder, Sort, SortOrder},
    summarize::{Aggregation, Summarize},
    value::Value,
};

pub fn field(name: &str) -> Field {
    field_unwrap!(name)
}

pub fn field_expr(name: &str) -> Expr {
    Expr::Field(field(name))
}

pub fn sort(sorts: Vec<Sort>) -> S {
    S::Sort(sorts)
}

pub fn sort_asc(by: Field) -> Sort {
    Sort {
        by,
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }
}

pub fn sort_desc(by: Field) -> Sort {
    Sort {
        by,
        order: SortOrder::Desc,
        nulls: NullsOrder::Last,
    }
}

pub fn project_field(to: &str, from: Expr) -> ProjectField {
    ProjectField {
        to: field(to),
        from,
    }
}

pub fn literal_project(to: &str, value: Value) -> ProjectField {
    project_field(to, Expr::Literal(value))
}

pub fn rename_project(to: &str, from: &str) -> ProjectField {
    project_field(to, Expr::Field(field(from)))
}

pub fn noop_project(to: &str) -> ProjectField {
    project_field(to, Expr::Field(field(to)))
}

pub fn expand(fields: Vec<Field>) -> Expand {
    Expand {
        fields,
        ..Default::default()
    }
}

pub fn string_val(s: &str) -> Value {
    Value::String(s.to_string())
}

pub fn int_val(n: i32) -> Value {
    Value::from(n)
}

pub fn lit(val: i64) -> Expr {
    Expr::Literal(Value::Int(val))
}

pub fn gt(l: Expr, r: Expr) -> Expr {
    Expr::Gt(Box::new(l), Box::new(r))
}

pub fn lt(l: Expr, r: Expr) -> Expr {
    Expr::Lt(Box::new(l), Box::new(r))
}

pub fn mul(l: Expr, r: Expr) -> Expr {
    Expr::Mul(Box::new(l), Box::new(r))
}

pub fn and(l: Expr, r: Expr) -> Expr {
    Expr::And(Box::new(l), Box::new(r))
}

pub fn or(l: Expr, r: Expr) -> Expr {
    Expr::Or(Box::new(l), Box::new(r))
}

pub fn not(e: Expr) -> Expr {
    Expr::Not(Box::new(e))
}

pub fn case(predicates: Vec<(Expr, Expr)>, default: Expr) -> Expr {
    Expr::Case(predicates, Box::new(default))
}

pub fn summarize(agg_field: &str, agg: Aggregation, by: Vec<Expr>) -> S {
    S::Summarize(Summarize {
        aggs: hashmap! { field(agg_field) => agg },
        by,
    })
}

pub fn summarize_by(fields: &[&str]) -> S {
    S::Summarize(Summarize {
        aggs: HashMap::new(),
        by: fields.iter().map(|f| Expr::Field(field(f))).collect(),
    })
}
