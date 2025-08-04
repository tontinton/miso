use std::fmt;

use serde::{Deserialize, Serialize};

use crate::field::Field;

#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    #[default]
    Inner,
    Outer,
    Left,
    Right,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Inner => write!(f, "inner"),
            JoinType::Outer => write!(f, "outer"),
            JoinType::Left => write!(f, "left"),
            JoinType::Right => write!(f, "right"),
        }
    }
}

fn default_partitions() -> usize {
    1
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
pub struct Join {
    pub on: (Field, Field),

    #[serde(default, rename = "type")]
    pub type_: JoinType,

    #[serde(default = "default_partitions")]
    pub partitions: usize,
}

impl fmt::Display for Join {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "on=({}, {}), type={}, partitions={}",
            self.on.0, self.on.1, self.type_, self.partitions
        )
    }
}
