use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct PushSummarizeIntoScan;

impl Optimization for PushSummarizeIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Summarize)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Summarize(summarize) = &steps[1] else {
            return None;
        };
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };

        scan.handle = scan
            .connector
            .apply_summarize(summarize, scan.handle.as_ref())?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
