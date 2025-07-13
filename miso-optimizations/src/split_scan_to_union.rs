use miso_workflow::{Workflow, WorkflowStep};

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct SplitScanIntoUnion;

impl Optimization for SplitScanIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!(Scan)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(scan) = steps[0].clone() else {
            return None;
        };

        let splits = scan.connector.get_splits();
        if splits.is_empty() {
            return Some(vec![WorkflowStep::Scan(scan)]);
        }

        let steps = splits
            .into_iter()
            .enumerate()
            .map(|(i, split)| {
                let mut scan_with_split = scan.clone();
                scan_with_split.split = Some(split.into());
                let step = WorkflowStep::Scan(scan_with_split);
                if i == 0 {
                    step
                } else {
                    WorkflowStep::Union(Workflow::new(vec![step]))
                }
            })
            .collect();

        Some(steps)
    }
}
