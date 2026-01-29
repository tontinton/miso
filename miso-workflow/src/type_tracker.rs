use std::{fmt::Display, mem::Discriminant};

use color_eyre::eyre::{Result, bail};
use miso_workflow_types::value::{Value, ValueKind};

pub struct TypeTracker {
    tracked: Vec<Option<(Discriminant<Value>, ValueKind)>>,
}

impl TypeTracker {
    pub fn new(len: usize) -> Self {
        Self {
            tracked: vec![None; len],
        }
    }

    pub fn check(&mut self, index: usize, value: &Value, key: impl Display) -> Result<()> {
        if *value == Value::Null {
            return Ok(());
        }

        let value_type = std::mem::discriminant(value);
        let tracked = &mut self.tracked[index];

        if let Some((t, _)) = tracked {
            if *t != value_type {
                bail!(
                    "cannot operate over differing types (key '{}'): {:?} != {:?}",
                    key,
                    *t,
                    value_type
                );
            }
        } else {
            *tracked = Some((value_type, value.kind()));
        }

        Ok(())
    }

    pub fn into_types(self) -> Vec<ValueKind> {
        self.tracked
            .into_iter()
            .map(|t| t.map(|(_, col_type)| col_type).unwrap_or(ValueKind::Null))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consistent_types_ok() {
        let mut tracker = TypeTracker::new(2);
        assert!(tracker.check(0, &Value::from(1), "a").is_ok());
        assert!(tracker.check(0, &Value::from(2), "a").is_ok());
        assert!(tracker.check(1, &Value::from("x"), "b").is_ok());
        assert!(tracker.check(1, &Value::from("y"), "b").is_ok());
    }

    #[test]
    fn null_values_ignored() {
        let mut tracker = TypeTracker::new(1);
        assert!(tracker.check(0, &Value::Null, "a").is_ok());
        assert!(tracker.check(0, &Value::from(1), "a").is_ok());
        assert!(tracker.check(0, &Value::Null, "a").is_ok());
        assert!(tracker.check(0, &Value::from(2), "a").is_ok());
    }

    #[test]
    fn type_mismatch_error() {
        let mut tracker = TypeTracker::new(1);
        assert!(tracker.check(0, &Value::from(1), "field").is_ok());
        let err = tracker.check(0, &Value::from("str"), "field").unwrap_err();
        assert!(err.to_string().contains("differing types"));
        assert!(err.to_string().contains("field"));
    }

    #[test]
    fn independent_indices() {
        let mut tracker = TypeTracker::new(2);
        assert!(tracker.check(0, &Value::from(1), "a").is_ok());
        assert!(tracker.check(1, &Value::from("x"), "b").is_ok());
        assert!(tracker.check(0, &Value::from(2), "a").is_ok());
        assert!(tracker.check(1, &Value::from("y"), "b").is_ok());
    }
}
