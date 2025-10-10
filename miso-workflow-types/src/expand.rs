use std::fmt;

use serde::{Deserialize, Serialize};

use crate::field::Field;

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct Expand {
    pub fields: Vec<Field>,
    pub kind: ExpandKind,
}

impl fmt::Display for Expand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "kind={}, fields=[", self.kind)?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{field}")?;
        }
        write!(f, "]")
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub enum ExpandKind {
    #[default]
    Bag,
    Array,
}

impl fmt::Display for ExpandKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bag => write!(f, "bag"),
            Self::Array => write!(f, "array"),
        }
    }
}
