use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushSummarizeIntoScan;

impl Optimization for PushSummarizeIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Summarize)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Summarize(config) = &steps[1] else {
            return None;
        };

        scan.handle = scan
            .connector
            .apply_summarize(config, scan.handle.as_ref())?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
