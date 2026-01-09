use miso_workflow::WorkflowStep;
use miso_workflow_types::{expr::Expr, value::Value};

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct ShortCircuitFalseFilter;

impl Optimization for ShortCircuitFalseFilter {
    fn pattern(&self) -> Pattern {
        pattern!(Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Filter(expr) = &steps[0] else {
            return OptimizationResult::Unchanged;
        };

        match expr {
            Expr::Literal(Value::Bool(false)) => OptimizationResult::ShortCircuit,
            _ => OptimizationResult::Unchanged,
        }
    }
}

#[cfg(test)]
mod tests {
    use miso_workflow::WorkflowStep as S;
    use miso_workflow_types::{expr::Expr, value::Value};
    use test_case::test_case;

    use super::ShortCircuitFalseFilter;
    use crate::{Optimization, OptimizationResult};

    #[test_case(Expr::Literal(Value::Bool(false)), OptimizationResult::ShortCircuit)]
    #[test_case(Expr::Literal(Value::Bool(true)), OptimizationResult::Unchanged)]
    fn short_circuit_false_filter_sanity(expr: Expr, expected: OptimizationResult) {
        assert_eq!(
            ShortCircuitFalseFilter.apply(&[S::Filter(expr)], &[]),
            expected
        );
    }
}
