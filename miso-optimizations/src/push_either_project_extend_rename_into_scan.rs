use miso_workflow::WorkflowStep;

use crate::{field_replacer::FieldReplacer, pattern};

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

        let replacer = FieldReplacer::new(&scan.static_fields);

        match &steps[1] {
            WorkflowStep::Project(projections) => {
                scan.handle = scan
                    .connector
                    .apply_project(
                        &replacer.transform_project(projections.to_vec()),
                        scan.handle.as_ref(),
                    )?
                    .into();
            }
            WorkflowStep::Extend(projections) => {
                scan.handle = scan
                    .connector
                    .apply_project(
                        &replacer.transform_project(projections.to_vec()),
                        scan.handle.as_ref(),
                    )?
                    .into();
            }
            WorkflowStep::Rename(renames) => {
                scan.handle = scan
                    .connector
                    .apply_rename(
                        &replacer.transform_rename(renames.to_vec()),
                        scan.handle.as_ref(),
                    )?
                    .into();
            }
            _ => return None,
        }

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
