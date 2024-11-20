use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushCountIntoScan;

impl Optimization for PushCountIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Count)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };

        scan.handle = scan.connector.apply_count(scan.handle.as_ref())?.into();
        Some(vec![WorkflowStep::Scan(scan)])
    }
}
