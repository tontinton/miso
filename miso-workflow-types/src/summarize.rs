use std::fmt;

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

use crate::{expr::Expr, field::Field};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Count,
    #[serde(rename = "dcount")]
    DCount(Field),
    Sum(Field),
    Min(Field),
    Max(Field),
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
    pub fn convert_to_mux(self, field: &Field) -> Self {
        match self {
            Aggregation::Count => Aggregation::Sum(field.clone()),
            Aggregation::Sum(..) => Aggregation::Sum(field.clone()),
            Aggregation::Min(..) => Aggregation::Min(field.clone()),
            Aggregation::Max(..) => Aggregation::Max(field.clone()),
            Aggregation::DCount(field) => Aggregation::DCount(field),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Summarize {
    pub aggs: HashMap<Field, Aggregation>,
    pub by: Vec<Expr>,
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
        let mut aggs = HashMap::new();
        for (field, agg) in self.aggs {
            match agg {
                Aggregation::Count
                | Aggregation::Sum(..)
                | Aggregation::Min(..)
                | Aggregation::Max(..) => {
                    aggs.insert(field, agg);
                }
                Aggregation::DCount(field) => {
                    let new_by = Expr::Field(field);
                    if !self.by.contains(&new_by) {
                        self.by.push(new_by);
                    }
                }
            }
        }
        Self { aggs, by: self.by }
    }

    pub fn convert_to_mux(self) -> Self {
        let mut aggs = HashMap::new();
        for (field, agg) in self.aggs {
            let mux = agg.convert_to_mux(&field);
            aggs.insert(field, mux);
        }
        Self { aggs, by: self.by }
    }

    pub fn is_empty(&self) -> bool {
        self.aggs.is_empty() && self.by.is_empty()
    }
}
