use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FilterAst {
    Id(String),             // field
    Lit(serde_json::Value), // a json literal
    Exists(String),         // field exists

    Or(Vec<FilterAst>),  // ||
    And(Vec<FilterAst>), // &&
    Not(Box<FilterAst>), // !

    In(Box<FilterAst>, Vec<FilterAst>), // like python's in

    Contains(Box<FilterAst>, Box<FilterAst>), // string - left.contains(right)
    StartsWith(Box<FilterAst>, Box<FilterAst>), // string - left.starts_with(right)
    EndsWith(Box<FilterAst>, Box<FilterAst>), // string - left.ends_with(right)
    Has(Box<FilterAst>, Box<FilterAst>),      // string - left.contains_phrase(right)
    HasCs(Box<FilterAst>, Box<FilterAst>),    // string - left.contains_phrase_cs(right)

    #[serde(rename = "==")]
    Eq(Box<FilterAst>, Box<FilterAst>),
    #[serde(rename = "!=")]
    Ne(Box<FilterAst>, Box<FilterAst>),
    #[serde(rename = ">")]
    Gt(Box<FilterAst>, Box<FilterAst>),
    #[serde(rename = ">=")]
    Gte(Box<FilterAst>, Box<FilterAst>),
    #[serde(rename = "<")]
    Lt(Box<FilterAst>, Box<FilterAst>),
    #[serde(rename = "<=")]
    Lte(Box<FilterAst>, Box<FilterAst>),

    #[serde(rename = "*")]
    Mul(Box<FilterAst>, Box<FilterAst>),
    #[serde(rename = "/")]
    Div(Box<FilterAst>, Box<FilterAst>),
    #[serde(rename = "+")]
    Plus(Box<FilterAst>, Box<FilterAst>),
    #[serde(rename = "-")]
    Minus(Box<FilterAst>, Box<FilterAst>),
}
