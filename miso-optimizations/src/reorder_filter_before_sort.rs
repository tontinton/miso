//! Moves filters before sorts to reduce work and enable further optimizations.
//!
//! Filtering after sorting wastes effort - we sort rows that get thrown away.
//! By filtering first, we sort fewer items. This also enables filter pushdown
//! to the connector, and lets sort-limit combinations become top-n.
//!
//! Example:
//!   sort by ts | where status == "error"
//! becomes:
//!   where status == "error" | sort by ts

use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct ReorderFilterBeforeSort;

impl Optimization for ReorderFilterBeforeSort {
    fn pattern(&self) -> Pattern {
        pattern!(Sort Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        OptimizationResult::Changed(vec![steps[1].clone(), steps[0].clone()])
    }
}
