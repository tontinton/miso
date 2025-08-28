use miso_workflow::WorkflowStep;
use miso_workflow_types::expr_visitor::ExprTransformer;

use crate::{field_replacer::FieldReplacer, pattern};

use super::{Group, Optimization, Pattern};

pub struct PushFilterIntoScan;

impl Optimization for PushFilterIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };
        let WorkflowStep::Filter(ast) = &steps[1] else {
            return None;
        };

        let replacer = FieldReplacer::new(&scan.static_fields);

        scan.handle = scan
            .connector
            .apply_filter(&replacer.transform(ast.clone()), scan.handle.as_ref())?
            .into();

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
