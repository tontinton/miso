use std::str::FromStr;

use miso_workflow::WorkflowStep as S;
use miso_workflow_types::{
    expand::Expand,
    expr::Expr,
    field::Field,
    field_unwrap,
    join::JoinType,
    project::ProjectField,
    sort::{NullsOrder, Sort, SortOrder},
    summarize::{Aggregation, ByField, Summarize},
    value::Value,
};
use std::collections::BTreeMap;

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

pub fn eq(l: Expr, r: Expr) -> Expr {
    Expr::Eq(Box::new(l), Box::new(r))
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

pub fn by_field(expr: Expr, name: &str) -> ByField {
    ByField {
        expr,
        name: field(name),
    }
}

pub fn summarize(agg_field: &str, agg: Aggregation, by: Vec<ByField>) -> S {
    S::Summarize(Summarize {
        aggs: BTreeMap::from([(field(agg_field), agg)]),
        by,
    })
}

pub fn summarize_by(fields: &[&str]) -> S {
    S::Summarize(Summarize {
        aggs: BTreeMap::new(),
        by: fields
            .iter()
            .map(|f| ByField {
                expr: Expr::Field(field(f)),
                name: field(f),
            })
            .collect(),
    })
}

pub fn join(type_: JoinType, left_key: &str, right_key: &str, right_steps: Vec<S>) -> S {
    use miso_workflow::Workflow;
    use miso_workflow_types::join::Join;
    S::Join(
        Join {
            on: (field(left_key), field(right_key)),
            type_,
            partitions: 1,
        },
        Workflow::new(right_steps),
    )
}

pub fn right_project(fields: &[&str]) -> Vec<S> {
    vec![S::Project(fields.iter().map(|f| noop_project(f)).collect())]
}

pub fn project(fields: &[&str]) -> S {
    S::Project(fields.iter().map(|f| noop_project(f)).collect())
}
