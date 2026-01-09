use hashbrown::HashSet;
use miso_workflow::WorkflowStep;
use miso_workflow_types::field::Field;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct RemoveRedundantStepsBeforeAggregation;

impl Optimization for RemoveRedundantStepsBeforeAggregation {
    fn pattern(&self) -> Pattern {
        pattern!([Project Extend Rename] [Count Summarize MuxSummarize])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let agg_step = steps.last().unwrap();

        if matches!(agg_step, WorkflowStep::Count) {
            return Some(vec![agg_step.clone()]);
        }

        let summarize = match agg_step {
            WorkflowStep::Summarize(s) | WorkflowStep::MuxSummarize(s) => s,
            _ => return None,
        };

        let defined_fields = get_defined_fields(&steps[0]);

        if defined_fields.is_disjoint(&summarize.used_fields()) {
            Some(vec![agg_step.clone()])
        } else {
            None
        }
    }
}

fn get_defined_fields(step: &WorkflowStep) -> HashSet<Field> {
    match step {
        WorkflowStep::Project(fields) | WorkflowStep::Extend(fields) => {
            fields.iter().map(|f| f.to.clone()).collect()
        }
        WorkflowStep::Rename(renames) => renames.iter().map(|(_, to)| to.clone()).collect(),
        _ => HashSet::new(),
    }
}

#[cfg(test)]
mod tests {
    use miso_common::hashmap;
    use miso_workflow::WorkflowStep as S;
    use miso_workflow_types::{
        expr::Expr,
        summarize::{Aggregation, Summarize},
    };
    use test_case::test_case;

    use super::RemoveRedundantStepsBeforeAggregation;
    use crate::Optimization;
    use crate::test_utils::{field, project_field, summarize};

    #[test_case(
        S::Project(vec![project_field("unused", Expr::Field(field("a")))]),
        summarize("c", Aggregation::Sum(field("x")), vec![Expr::Field(field("y"))])
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
        assert_eq!(opt.apply(&steps, &[]), Some(vec![summarize]));
    }

    #[test_case(
        S::Project(vec![project_field("x", Expr::Field(field("a")))]),
        summarize("c", Aggregation::Sum(field("x")), vec![])
        ; "project_field_in_aggregation"
    )]
    #[test_case(
        S::Extend(vec![project_field("x", Expr::Field(field("a")))]),
        S::Summarize(Summarize { aggs: hashmap! {}, by: vec![Expr::Field(field("x"))] })
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
        assert_eq!(opt.apply(&steps, &[]), None);
    }
}
