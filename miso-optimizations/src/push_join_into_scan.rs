use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct PushJoinIntoScan;

impl Optimization for PushJoinIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Join)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut left_scan) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Join(join_config, right_workflow) = &steps[1] else {
            return None;
        };

        if right_workflow.steps.len() != 1 {
            return None;
        }
        let WorkflowStep::Scan(right_scan) = right_workflow.steps[0].clone() else {
            return None;
        };

        // Joins can only be pushed down if both scans use the same connector.
        if left_scan.connector_name != right_scan.connector_name {
            return None;
        }

        let new_handle = left_scan.connector.apply_join(
            join_config,
            &left_scan.collection,
            &right_scan.collection,
            left_scan.handle.as_ref(),
            right_scan.handle.as_ref(),
        )?;

        left_scan.handle = new_handle.into();

        // Disable dynamic filtering.
        left_scan.dynamic_filter_tx = None;
        left_scan.dynamic_filter_rx = None;

        Some(vec![WorkflowStep::Scan(left_scan)])
    }
}
