use miso_workflow::WorkflowStep;
use miso_workflow_types::{expr::Expr, value::Value};

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct RemoveNoOpFilter;

impl Optimization for RemoveNoOpFilter {
    fn pattern(&self) -> Pattern {
        pattern!(Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Filter(expr) = &steps[0] else {
            return OptimizationResult::Unchanged;
        };

        match expr {
            Expr::Literal(Value::Bool(true)) => OptimizationResult::Changed(vec![]),
            _ => OptimizationResult::Unchanged,
        }
    }
}
