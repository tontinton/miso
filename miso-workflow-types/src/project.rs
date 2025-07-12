use serde::{Deserialize, Serialize};

use crate::CastType;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectAst {
    Id(String),
    Lit(serde_json::Value),
    Cast(CastType, Box<ProjectAst>),
    #[serde(rename = "*")]
    Mul(Box<ProjectAst>, Box<ProjectAst>),
    #[serde(rename = "/")]
    Div(Box<ProjectAst>, Box<ProjectAst>),
    #[serde(rename = "+")]
    Plus(Box<ProjectAst>, Box<ProjectAst>),
    #[serde(rename = "-")]
    Minus(Box<ProjectAst>, Box<ProjectAst>),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ProjectField {
    pub from: ProjectAst,
    pub to: String,
}
