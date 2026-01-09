use miso_workflow::WorkflowStep;
use miso_workflow_types::{expr::Expr, value::Value};

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct RemoveNoOpFilter;

impl Optimization for RemoveNoOpFilter {
    fn pattern(&self) -> Pattern {
        pattern!(Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Filter(expr) = &steps[0] else {
            return None;
        };

        match expr {
            Expr::Literal(Value::Bool(true)) => Some(vec![]),
            _ => None,
        }
    }
}
