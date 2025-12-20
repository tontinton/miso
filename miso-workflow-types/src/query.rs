use serde::{Deserialize, Serialize};

use crate::{
    expand::Expand, expr::Expr, field::Field, join::Join, project::ProjectField, sort::Sort,
    summarize::Summarize,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryStep {
    Let(String, Vec<QueryStep>),
    Scan(ScanKind),
    Filter(Expr),
    Project(Vec<ProjectField>),
    Extend(Vec<ProjectField>),
    Rename(Vec<(Field, Field)>),
    Expand(Expand),
    Limit(u64),
    Sort(Vec<Sort>),
    Top(Vec<Sort>, u64),
    Summarize(Summarize),
    Distinct(Vec<Field>),
    Union(Vec<QueryStep>),
    Join(Join, Vec<QueryStep>),
    Count,
    Tee {
        connector: String,
        collection: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScanKind {
    Var(String),
    Collection {
        connector: String,
        collection: String,
    },
}
