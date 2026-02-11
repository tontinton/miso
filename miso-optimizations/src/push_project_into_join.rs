//! Narrows both sides of a join to only the fields that later steps need.
//!
//! A join merges two streams, but often the steps after it only use a few
//! fields. Without pruning, both sides carry unused fields through the join.
//!
//! We walk backwards from the last step to find which fields are needed, then
//! split them into left vs right based on the right workflow's output fields.
//! The left side has unknown schema, so we always inject a project before the
//! join. The right side has known schema (project or summarize), so we only
//! add a project when it actually drops fields.
//!
//! Example:
//!   join (scan | project rf, rid, extra) on id == rid | project lf, rf
//! becomes:
//!   project id, lf | join (scan | project rf, rid, extra | project rf, rid) on id == rid | project lf, rf
//!
//! When the only step after the join is count, both sides shrink to just the key:
//!   join (scan | project rf, rid) on id == rid | count
//! becomes:
//!   project id | join (scan | project rf, rid | project rid) on id == rid | count

use std::collections::BTreeSet;

use miso_workflow::WorkflowStep;
use miso_workflow_types::field::Field;

use crate::eliminate_unused_fields::{compute_required_before_step, create_identity_project};
use crate::pattern;
use crate::push_filter_into_join::right_workflow_fields;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct PushProjectIntoJoin;

impl Optimization for PushProjectIntoJoin {
    fn pattern(&self) -> Pattern {
        pattern!(Join ([^Join Union Tee Write]*?) [Project Summarize MuxSummarize Count MuxCount])
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Join(join, right_workflow) = &steps[0] else {
            return OptimizationResult::Unchanged;
        };

        let Some(mut right_fields) = right_workflow_fields(&right_workflow.steps) else {
            return OptimizationResult::Unchanged;
        };

        if join.on.0 == join.on.1 {
            right_fields.remove(&join.on.1);
        }

        let mut required: BTreeSet<Field> = BTreeSet::new();
        for step in steps[1..].iter().rev() {
            required = compute_required_before_step(step, required);
        }
        required.insert(join.on.0.clone());
        required.insert(join.on.1.clone());

        let left_required: BTreeSet<Field> = required
            .iter()
            .filter(|f| !right_fields.contains(*f))
            .cloned()
            .collect();
        let right_required: BTreeSet<Field> = required
            .iter()
            .filter(|f| right_fields.contains(*f))
            .cloned()
            .collect();

        let prune_right = right_required.len() < right_fields.len();

        if left_required.is_empty() && !prune_right {
            return OptimizationResult::Unchanged;
        }

        let mut result = Vec::new();

        if !left_required.is_empty() {
            result.push(create_identity_project(left_required));
        }

        let mut right_wf = right_workflow.clone();
        if prune_right {
            right_wf.steps.push(create_identity_project(right_required));
        }
        result.push(WorkflowStep::Join(join.clone(), right_wf));

        let group = groups[0];
        result.extend(steps[group.0..group.1].iter().cloned());
        result.push(steps.last().unwrap().clone());

        OptimizationResult::Changed(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        field, field_expr, gt, join, lit, project, right_project, summarize_by,
    };
    use miso_workflow::WorkflowStep;
    use miso_workflow_types::join::JoinType;

    fn apply(steps: &[WorkflowStep]) -> OptimizationResult {
        let opt = PushProjectIntoJoin;
        let pattern = opt.pattern();
        let mut groups = Vec::new();
        let kinds: Vec<_> = steps.iter().map(|s| s.kind()).collect();
        let Some((start, end)) = Pattern::search_first_with_groups(&pattern, &kinds, &mut groups)
        else {
            return OptimizationResult::Unchanged;
        };
        opt.apply(&steps[start..end], &groups)
    }

    #[test]
    fn prunes_both_sides() {
        let steps = vec![
            join(
                JoinType::Inner,
                "id",
                "rid",
                right_project(&["rf", "rid", "extra"]),
            ),
            project(&["lf", "rf"]),
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        assert_eq!(new_steps.len(), 3);
        assert!(matches!(&new_steps[0], WorkflowStep::Project(fields) if fields.len() == 2));
        let WorkflowStep::Join(_, rw) = &new_steps[1] else {
            panic!("expected Join");
        };
        assert_eq!(rw.steps.len(), 2);
        assert!(matches!(&new_steps[2], WorkflowStep::Project(fields) if fields.len() == 2));
    }

    #[test]
    fn count_prunes_to_join_keys_only() {
        let steps = vec![
            join(JoinType::Inner, "id", "rid", right_project(&["rf", "rid"])),
            WorkflowStep::Count,
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        assert!(matches!(&new_steps[0], WorkflowStep::Project(fields) if fields.len() == 1));
        let WorkflowStep::Join(_, rw) = &new_steps[1] else {
            panic!("expected Join");
        };
        assert!(
            matches!(rw.steps.last().unwrap(), WorkflowStep::Project(fields) if fields.len() == 1)
        );
    }

    #[test]
    fn preserves_intermediate_steps() {
        let steps = vec![
            join(
                JoinType::Inner,
                "id",
                "rid",
                right_project(&["rf", "rid", "extra"]),
            ),
            WorkflowStep::Filter(gt(field_expr("lf"), lit(5))),
            project(&["lf", "rf"]),
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        assert_eq!(new_steps.len(), 4);
        assert!(matches!(&new_steps[0], WorkflowStep::Project(_)));
        assert!(matches!(&new_steps[2], WorkflowStep::Filter(_)));
        assert!(matches!(&new_steps[3], WorkflowStep::Project(_)));
    }

    #[test]
    fn right_fields_unknown_unchanged() {
        let steps = vec![
            join(
                JoinType::Inner,
                "id",
                "rid",
                vec![WorkflowStep::Filter(gt(field_expr("x"), lit(1)))],
            ),
            project(&["lf", "rf"]),
        ];

        assert_eq!(apply(&steps), OptimizationResult::Unchanged);
    }

    #[test]
    fn overlapping_join_keys() {
        let steps = vec![
            join(JoinType::Inner, "id", "id", right_project(&["rf", "id"])),
            project(&["rf"]),
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        assert!(matches!(&new_steps[0], WorkflowStep::Project(fields) if fields.len() == 1));
        let WorkflowStep::Project(left_proj) = &new_steps[0] else {
            panic!("expected Project");
        };
        assert_eq!(left_proj[0].to, field("id"));
    }

    #[test]
    fn summarize_right_workflow() {
        let steps = vec![
            join(
                JoinType::Inner,
                "id",
                "rid",
                vec![summarize_by(&["rf", "rid", "extra"])],
            ),
            project(&["lf", "rf"]),
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        let WorkflowStep::Join(_, rw) = &new_steps[1] else {
            panic!("expected Join");
        };
        assert_eq!(rw.steps.len(), 2);
        assert!(
            matches!(rw.steps.last().unwrap(), WorkflowStep::Project(fields) if fields.len() == 2)
        );
    }
}
