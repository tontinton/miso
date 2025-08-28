use miso_workflow::WorkflowStep;

use crate::{field_replacer::FieldReplacer, pattern};

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

        let replacer = FieldReplacer::new(&scan.static_fields);

        scan.handle = scan
            .connector
            .apply_topn(
                &replacer.transform_sort(sorts.to_vec()),
                *max,
                scan.handle.as_ref(),
            )?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
