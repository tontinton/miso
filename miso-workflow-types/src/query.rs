use serde::{Deserialize, Serialize};

use crate::{
    expand::Expand, expr::Expr, field::Field, join::Join, project::ProjectField, sort::Sort,
    summarize::Summarize,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryStep {
    Scan(/*connector=*/ String, /*collection=*/ String),
    Filter(Expr),
    Project(Vec<ProjectField>),
    Extend(Vec<ProjectField>),
    Rename(Vec<(Field, Field)>),
    Expand(Expand),
    Limit(u32),
    Sort(Vec<Sort>),
    Top(Vec<Sort>, u32),
    Summarize(Summarize),
    Distinct(Vec<Field>),
    Union(Vec<QueryStep>),
    Join(Join, Vec<QueryStep>),
    Count,
}
