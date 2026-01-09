use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct PushJoinIntoScan;

impl Optimization for PushJoinIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Join)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Scan(mut left_scan) = steps[0].clone() else {
            return OptimizationResult::Unchanged;
        };
        let WorkflowStep::Join(join_config, right_workflow) = &steps[1] else {
            return OptimizationResult::Unchanged;
        };

        if right_workflow.steps.len() != 1 {
            return OptimizationResult::Unchanged;
        }
        let WorkflowStep::Scan(right_scan) = right_workflow.steps[0].clone() else {
            return OptimizationResult::Unchanged;
        };

        if left_scan.connector_name != right_scan.connector_name {
            return OptimizationResult::Unchanged;
        }

        let Some(new_handle) = left_scan.connector.apply_join(
            join_config,
            &left_scan.collection,
            &right_scan.collection,
            left_scan.handle.as_ref(),
            right_scan.handle.as_ref(),
        ) else {
            return OptimizationResult::Unchanged;
        };

        left_scan.handle = new_handle.into();

        left_scan.dynamic_filter_tx = None;
        left_scan.dynamic_filter_rx = None;

        OptimizationResult::Changed(vec![WorkflowStep::Scan(left_scan)])
    }
}
