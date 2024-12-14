use std::{cmp::Ordering, ops::Deref};

use vrl::core::Value;

use super::vrl_utils::partial_cmp_values;

#[derive(Debug, Clone)]
pub struct SortableValue(pub Value);

impl AsRef<Value> for SortableValue {
    fn as_ref(&self) -> &Value {
        &self.0
    }
}

impl Deref for SortableValue {
    type Target = Value;
    fn deref(&self) -> &Value {
        &self.0
    }
}

impl From<SortableValue> for Value {
    fn from(s: SortableValue) -> Self {
        s.0
    }
}

impl Ord for SortableValue {
    fn cmp(&self, other: &Self) -> Ordering {
        partial_cmp_values(&self.0, &other.0).unwrap_or(Ordering::Less)
    }
}

impl PartialOrd for SortableValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for SortableValue {}

impl PartialEq for SortableValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
