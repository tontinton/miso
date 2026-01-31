//! Combines consecutive filters into a single AND filter.
//!
//! Multiple back-to-back filters are equivalent to one filter with AND. Merging
//! them simplifies the pipeline and makes it easier for connectors to push down
//! the entire predicate in one go.
//!
//! Example:
//!   where x > 0 | where y < 10
//! becomes:
//!   where x > 0 and y < 10

use miso_workflow::WorkflowStep;
use miso_workflow_types::expr::Expr;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

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
