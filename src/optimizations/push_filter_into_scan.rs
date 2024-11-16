use crate::{pattern, workflow::WorkflowStep};

use super::{Optimization, Pattern};

pub struct PushFilterIntoScan;

impl Optimization for PushFilterIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan -> Filter)
    }

    fn apply(&self, steps: &[WorkflowStep]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Filter(ast) = &steps[1] else {
            return None;
        };

        scan.handle = scan
            .connector
            .apply_filter(ast, scan.handle.as_ref())?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
