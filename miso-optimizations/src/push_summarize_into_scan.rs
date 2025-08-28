use miso_workflow::WorkflowStep;

use crate::{field_replacer::FieldReplacer, pattern};

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

        let replacer = FieldReplacer::new(&scan.static_fields);

        scan.handle = scan
            .connector
            .apply_summarize(
                &replacer.transform_summarize(summarize.clone()),
                scan.handle.as_ref(),
            )?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
