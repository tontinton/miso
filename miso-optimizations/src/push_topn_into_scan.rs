use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct PushTopNIntoScan;

impl Optimization for PushTopNIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan TopN)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::TopN(sorts, max) = &steps[1] else {
            return None;
        };

        scan.handle = scan
            .connector
            .apply_topn(sorts, *max, scan.handle.as_ref())?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
