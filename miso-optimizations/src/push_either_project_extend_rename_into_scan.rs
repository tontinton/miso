use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct PushEitherProjectExtendRenameIntoScan;

impl Optimization for PushEitherProjectExtendRenameIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan [Project Extend Rename])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };

        match &steps[1] {
            WorkflowStep::Project(projections) => {
                scan.handle = scan
                    .connector
                    .apply_project(projections.clone(), scan.handle.as_ref())?
                    .into();
            }
            WorkflowStep::Extend(projections) => {
                scan.handle = scan
                    .connector
                    .apply_extend(projections.clone(), scan.handle.as_ref())?
                    .into();
            }
            WorkflowStep::Rename(renames) => {
                scan.handle = scan
                    .connector
                    .apply_rename(renames, scan.handle.as_ref())?
                    .into();
            }
            _ => return None,
        }

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
