use std::fmt;

use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

use crate::{expr::Expr, field::Field};

pub const MUX_AVG_SUM_SUFFIX: &str = "_sum";
pub const MUX_AVG_COUNT_SUFFIX: &str = "_num";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ByField {
    pub expr: Expr,
    pub name: Field,
}

impl fmt::Display for ByField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.name, self.expr)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Aggregation {
    Count,
    Countif(Expr),
    #[serde(rename = "dcount")]
    DCount(Field),
    Sum(Field),
    Avg(Field),
    Min(Field),
    Max(Field),
}

impl fmt::Display for Aggregation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Aggregation::Count => write!(f, "Count"),
            Aggregation::Countif(x) => write!(f, "Countif({x})"),
            Aggregation::DCount(x) => write!(f, "DCount({x})"),
            Aggregation::Sum(x) => write!(f, "Sum({x})"),
            Aggregation::Avg(x) => write!(f, "Avg({x})"),
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
            Aggregation::Countif(..) => Aggregation::Sum(field.clone()),
            Aggregation::Sum(..) => Aggregation::Sum(field.clone()),
            Aggregation::Avg(..) => Aggregation::Avg(field.clone()),
            Aggregation::Min(..) => Aggregation::Min(field.clone()),
            Aggregation::Max(..) => Aggregation::Max(field.clone()),
            Aggregation::DCount(field) => Aggregation::DCount(field),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Summarize {
    pub aggs: HashMap<Field, Aggregation>,
    pub by: Vec<ByField>,
}

impl fmt::Display for Summarize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "by=[")?;
        for (i, pf) in self.by.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{pf}")?;
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
                | Aggregation::Countif(..)
                | Aggregation::Sum(..)
                | Aggregation::Min(..)
                | Aggregation::Max(..) => {
                    aggs.insert(field, agg);
                }
                Aggregation::DCount(input_field) => {
                    let new_by_expr = Expr::Field(input_field.clone());
                    if !self.by.iter().any(|bf| bf.expr == new_by_expr) {
                        self.by.push(ByField {
                            name: input_field.clone(),
                            expr: new_by_expr,
                        });
                    }
                }
                Aggregation::Avg(input_field) => {
                    aggs.insert(
                        field.clone().with_suffix(MUX_AVG_SUM_SUFFIX),
                        Aggregation::Sum(input_field.clone()),
                    );
                    aggs.insert(
                        field.with_suffix(MUX_AVG_COUNT_SUFFIX),
                        Aggregation::Countif(Expr::Exists(Box::new(Expr::Field(input_field)))),
                    );
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

    pub fn used_fields(&self) -> HashSet<Field> {
        let mut fields = HashSet::new();

        for agg in self.aggs.values() {
            match agg {
                Aggregation::Count => {}
                Aggregation::Countif(expr) => fields.extend(expr.fields()),
                Aggregation::DCount(f)
                | Aggregation::Sum(f)
                | Aggregation::Avg(f)
                | Aggregation::Min(f)
                | Aggregation::Max(f) => {
                    fields.insert(f.clone());
                }
            }
        }

        for bf in &self.by {
            fields.extend(bf.expr.fields());
        }

        fields
    }
}
