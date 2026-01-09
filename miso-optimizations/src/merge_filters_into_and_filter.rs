use miso_workflow::WorkflowStep;
use miso_workflow_types::expr::Expr;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

/// Assumes all connectors can predicate pushdown AND, otherwise this optimization is actually bad.
pub struct MergeFiltersIntoAndFilter;

impl Optimization for MergeFiltersIntoAndFilter {
    fn pattern(&self) -> Pattern {
        pattern!(Filter Filter+)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let mut filters = Vec::with_capacity(steps.len());
        for step in steps {
            let WorkflowStep::Filter(filter) = step else {
                return OptimizationResult::Unchanged;
            };
            filters.push(filter.clone());
        }

        let Some(combined) = filters
            .into_iter()
            .reduce(|acc, filter| Expr::And(Box::new(acc), Box::new(filter)))
        else {
            return OptimizationResult::Unchanged;
        };

        OptimizationResult::Changed(vec![WorkflowStep::Filter(combined)])
    }
}
