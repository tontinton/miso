use crate::{
    pattern,
    workflow::{filter::FilterAst, WorkflowStep},
};

use super::{Group, Optimization, Pattern};

/// Assumes all connectors can predicate pushdown AND, otherwise this optimization is actually bad.
pub struct MergeFiltersIntoAndFilter;

impl Optimization for MergeFiltersIntoAndFilter {
    fn pattern(&self) -> Pattern {
        pattern!(Filter Filter+)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let mut filters = Vec::with_capacity(steps.len());
        for step in steps {
            let WorkflowStep::Filter(filter) = step else {
                return None;
            };
            filters.push(filter.clone());
        }
        Some(vec![WorkflowStep::Filter(FilterAst::And(filters))])
    }
}
