use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum GroupAst {
    Id(String),
    Bin(String, serde_json::Value),
}

impl GroupAst {
    #[must_use]
    pub fn field(&self) -> &str {
        match self {
            GroupAst::Id(field) | GroupAst::Bin(field, _) => field,
        }
    }
}

impl fmt::Display for GroupAst {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GroupAst::Id(name) => write!(f, "{name}"),
            GroupAst::Bin(name, by) => write!(f, "bin({name}, {by})"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Count,
    #[serde(rename = "dcount")]
    DCount(/*field=*/ String),
    Sum(/*field=*/ String),
    Min(/*field=*/ String),
    Max(/*field=*/ String),
}

impl fmt::Display for Aggregation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Aggregation::Count => write!(f, "Count"),
            Aggregation::DCount(x) => write!(f, "DCount({x})"),
            Aggregation::Sum(x) => write!(f, "Sum({x})"),
            Aggregation::Min(x) => write!(f, "Min({x})"),
            Aggregation::Max(x) => write!(f, "Max({x})"),
        }
    }
}

impl Aggregation {
    #[must_use]
    pub fn convert_to_mux(self, field: &str) -> Self {
        match self {
            Aggregation::Count => Aggregation::Sum(field.to_string()),
            Aggregation::Sum(..) => Aggregation::Sum(field.to_string()),
            Aggregation::Min(..) => Aggregation::Min(field.to_string()),
            Aggregation::Max(..) => Aggregation::Max(field.to_string()),
            Aggregation::DCount(field) => Aggregation::DCount(field),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Summarize {
    pub aggs: BTreeMap<String, Aggregation>,
    pub by: Vec<GroupAst>,
}

impl fmt::Display for Summarize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "by=[")?;
        for (i, by) in self.by.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{by}")?;
        }
        write!(f, "], aggs=[")?;
        for (i, (key, agg)) in self.aggs.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{key}={agg}")?;
        }
        write!(f, "]")
    }
}

impl Summarize {
    pub fn convert_to_partial(mut self) -> Self {
        let mut aggs = BTreeMap::new();
        for (field, agg) in self.aggs {
            match agg {
                Aggregation::Count
                | Aggregation::Sum(..)
                | Aggregation::Min(..)
                | Aggregation::Max(..) => {
                    aggs.insert(field, agg);
                }
                Aggregation::DCount(field) => {
                    let new_by = GroupAst::Id(field);
                    if !self.by.contains(&new_by) {
                        self.by.push(new_by);
                    }
                }
            }
        }
        Self { aggs, by: self.by }
    }

    pub fn convert_to_mux(self) -> Self {
        let mut aggs = BTreeMap::new();
        for (field, agg) in self.aggs {
            let mux = agg.convert_to_mux(&field);
            aggs.insert(field, mux);
        }
        Self { aggs, by: self.by }
    }
}
