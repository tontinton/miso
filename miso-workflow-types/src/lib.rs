pub mod filter;
pub mod join;
pub mod log;
pub mod project;
pub mod sort;
pub mod summarize;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Copy, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CastType {
    Bool,
    Float,
    Int,
    String,
}
