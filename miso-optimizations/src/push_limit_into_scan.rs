use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct PushLimitIntoScan;

impl Optimization for PushLimitIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Limit)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Limit(max) = &steps[1] else {
            return None;
        };

        scan.handle = scan
            .connector
            .apply_limit(*max, scan.handle.as_ref())?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
