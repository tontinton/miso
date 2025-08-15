use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{expr::Expr, field::Field};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ProjectField {
    pub from: Expr,
    pub to: Field,
}

impl fmt::Display for ProjectField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.to, self.from)
    }
}
