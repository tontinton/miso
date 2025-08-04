use std::fmt;

use serde::{Deserialize, Serialize};

use crate::field::Field;

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Serialize, Deserialize, Default, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum NullsOrder {
    #[default]
    Last,
    First,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Sort {
    pub by: Field,

    #[serde(default)]
    pub order: SortOrder,

    #[serde(default)]
    pub nulls: NullsOrder,
}

impl fmt::Display for SortOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SortOrder::Asc => write!(f, "asc"),
            SortOrder::Desc => write!(f, "desc"),
        }
    }
}

impl fmt::Display for NullsOrder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NullsOrder::Last => write!(f, "last"),
            NullsOrder::First => write!(f, "first"),
        }
    }
}

impl fmt::Display for Sort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} (order: {}, nulls: {})",
            self.by, self.order, self.nulls
        )
    }
}
