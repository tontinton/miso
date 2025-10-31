mod case_transformer;

use miso_workflow::WorkflowStep;
use miso_workflow_types::expr_visitor::ExprTransformer;

use crate::{
    const_folding::partial_evaluator::partial_eval, field_replacer::FieldReplacer, pattern,
    push_into_scan::case_transformer::case_transform,
};

use super::{Group, Optimization, Pattern};

pub struct PushIntoScan;

impl Optimization for PushIntoScan {
    fn pattern(&self) -> Pattern {
        pattern!(Scan [Project Extend Rename Expand Limit TopN Filter Summarize Count])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Scan(mut scan) = steps[0].clone() else {
            return None;
        };

        let replacer = FieldReplacer::new(&scan.static_fields);

        scan.handle = match &steps[1] {
            WorkflowStep::Project(projections) => scan
                .connector
                .apply_project(
                    &replacer.transform_project(projections.to_vec()),
                    scan.handle.as_ref(),
                )?
                .into(),
            WorkflowStep::Extend(projections) => scan
                .connector
                .apply_project(
                    &replacer.transform_project(projections.to_vec()),
                    scan.handle.as_ref(),
                )?
                .into(),
            WorkflowStep::Rename(renames) => scan
                .connector
                .apply_rename(
                    &replacer.transform_rename(renames.to_vec()),
                    scan.handle.as_ref(),
                )?
                .into(),
            WorkflowStep::Expand(expand) => scan
                .connector
                .apply_expand(
                    &replacer.transform_expand(expand.clone()),
                    scan.handle.as_ref(),
                )?
                .into(),
            WorkflowStep::Limit(max) => scan
                .connector
                .apply_limit(*max, scan.handle.as_ref())?
                .into(),
            WorkflowStep::TopN(sorts, max) => scan
                .connector
                .apply_topn(
                    &replacer.transform_sort(sorts.to_vec()),
                    *max,
                    scan.handle.as_ref(),
                )?
                .into(),
            WorkflowStep::Filter(ast) => {
                let ast = &replacer.transform(ast.clone());
                scan.connector
                    .apply_filter(ast, scan.handle.as_ref())
                    .or_else(|| {
                        // If case is unsupported (cannot be pushdown to connector via apply_filter),
                        // replace it with ORs and ANDs, and try again.
                        let ast_without_case = case_transform(ast.clone());
                        let final_ast = partial_eval(&ast_without_case).ok()?;
                        scan.connector
                            .apply_filter(&final_ast, scan.handle.as_ref())
                    })?
                    .into()
            }
            WorkflowStep::Summarize(summarize) => scan
                .connector
                .apply_summarize(
                    &replacer.transform_summarize(summarize.clone()),
                    scan.handle.as_ref(),
                )?
                .into(),
            WorkflowStep::Count => scan.connector.apply_count(scan.handle.as_ref())?.into(),
            _ => return None,
        };

        Some(vec![WorkflowStep::Scan(scan)])
    }
}
