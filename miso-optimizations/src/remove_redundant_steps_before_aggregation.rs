use miso_workflow::WorkflowStep;
use miso_workflow_types::field::Field;
use std::collections::BTreeSet;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct RemoveRedundantStepsBeforeAggregation;

impl Optimization for RemoveRedundantStepsBeforeAggregation {
    fn pattern(&self) -> Pattern {
        pattern!([Project Extend Rename] [Count Summarize MuxSummarize])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let agg_step = steps.last().unwrap();

        if matches!(agg_step, WorkflowStep::Count) {
            return OptimizationResult::Changed(vec![agg_step.clone()]);
        }

        let summarize = match agg_step {
            WorkflowStep::Summarize(s) | WorkflowStep::MuxSummarize(s) => s,
            _ => return OptimizationResult::Unchanged,
        };

        let defined_fields = get_defined_fields(&steps[0]);

        if defined_fields.is_disjoint(&summarize.used_fields()) {
            OptimizationResult::Changed(vec![agg_step.clone()])
        } else {
            OptimizationResult::Unchanged
        }
    }
}

fn get_defined_fields(step: &WorkflowStep) -> BTreeSet<Field> {
    match step {
        WorkflowStep::Project(fields) | WorkflowStep::Extend(fields) => {
            fields.iter().map(|f| f.to.clone()).collect()
        }
        WorkflowStep::Rename(renames) => renames.iter().map(|(_, to)| to.clone()).collect(),
        _ => BTreeSet::new(),
    }
}

#[cfg(test)]
mod tests {
    use miso_workflow::WorkflowStep as S;
    use miso_workflow_types::{
        expr::Expr,
        summarize::{Aggregation, Summarize},
    };
    use test_case::test_case;

    use super::RemoveRedundantStepsBeforeAggregation;
    use crate::test_utils::{by_field, field, project_field, summarize};
    use crate::{Optimization, OptimizationResult};

    #[test_case(
        S::Project(vec![project_field("unused", Expr::Field(field("a")))]),
        summarize("c", Aggregation::Sum(field("x")), vec![by_field(Expr::Field(field("y")), "y")])
        ; "project"
    )]
    #[test_case(
        S::Extend(vec![project_field("unused", Expr::Field(field("a")))]),
        summarize("c", Aggregation::Count, vec![])
        ; "extend"
    )]
    #[test_case(
        S::Rename(vec![(field("old"), field("new"))]),
        summarize("c", Aggregation::Min(field("x")), vec![])
        ; "rename"
    )]
    fn removes_step_when_fields_unused(step: S, summarize: S) {
        let opt = RemoveRedundantStepsBeforeAggregation;
        let steps = vec![step, summarize.clone()];
        assert_eq!(
            opt.apply(&steps, &[]),
            OptimizationResult::Changed(vec![summarize])
        );
    }

    #[test_case(
        S::Project(vec![project_field("x", Expr::Field(field("a")))]),
        summarize("c", Aggregation::Sum(field("x")), vec![])
        ; "project_field_in_aggregation"
    )]
    #[test_case(
        S::Extend(vec![project_field("x", Expr::Field(field("a")))]),
        S::Summarize(Summarize { aggs: Default::default(), by: vec![by_field(Expr::Field(field("x")), "x")] })
        ; "extend_field_in_group_by"
    )]
    #[test_case(
        S::Rename(vec![(field("old"), field("x"))]),
        summarize("c", Aggregation::Max(field("x")), vec![])
        ; "rename_field_in_aggregation"
    )]
    fn keeps_step_when_fields_used(step: S, summarize: S) {
        let opt = RemoveRedundantStepsBeforeAggregation;
        let steps = vec![step, summarize];
        assert_eq!(opt.apply(&steps, &[]), OptimizationResult::Unchanged);
    }
}
