//! Pushes filter predicates through joins to filter each side independently.
//!
//! A filter after a join applies to the already-merged result, but often its
//! individual conditions only reference fields from one side. By splitting the
//! filter and pushing each part into the appropriate join branch, we reduce data
//! before the join runs - sometimes dramatically.
//!
//! Which side a condition can be pushed to depends on the join type: inner joins
//! allow pushing to either side, left joins only to the left, right joins only
//! to the right. Outer joins block all pushdown. Conditions that reference fields
//! from both sides stay after the join.
//!
//! Example (inner join):
//!   join (scan | project rf, rid) on id == rid | where lf > 5 and rf == 10
//! becomes:
//!   where lf > 5 | join (scan | project rf, rid | where rf == 10) on id == rid

use hashbrown::HashSet;
use miso_workflow::WorkflowStep;
use miso_workflow_types::{
    expr::{Expr, and_all},
    field::Field,
    join::JoinType,
};

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct PushFilterIntoJoin;

#[derive(PartialEq)]
enum ConditionSide {
    Left,
    Right,
    Both,
}

impl Optimization for PushFilterIntoJoin {
    fn pattern(&self) -> Pattern {
        pattern!(Join Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Join(join, right_workflow) = &steps[0] else {
            return OptimizationResult::Unchanged;
        };
        let WorkflowStep::Filter(filter_expr) = &steps[1] else {
            return OptimizationResult::Unchanged;
        };

        if join.type_ == JoinType::Outer {
            return OptimizationResult::Unchanged;
        }

        let Some(mut right_fields) = right_workflow_fields(&right_workflow.steps) else {
            return OptimizationResult::Unchanged;
        };

        // The join merge drops the right's copy of a same-named key (see merge_left_with_right).
        if join.on.0 == join.on.1 {
            right_fields.remove(&join.on.1);
        }

        let conditions = flatten_and_conditions(filter_expr.clone());

        let mut left_conditions = Vec::new();
        let mut right_conditions = Vec::new();
        let mut remaining_conditions = Vec::new();

        for condition in conditions {
            let side = classify_condition(&condition, &right_fields);
            match (side, join.type_) {
                (ConditionSide::Left, JoinType::Inner | JoinType::Left) => {
                    left_conditions.push(condition);
                }
                (ConditionSide::Right, JoinType::Inner | JoinType::Right) => {
                    right_conditions.push(condition);
                }
                _ => remaining_conditions.push(condition),
            }
        }

        if left_conditions.is_empty() && right_conditions.is_empty() {
            return OptimizationResult::Unchanged;
        }

        let mut result = Vec::new();

        if !left_conditions.is_empty() {
            result.push(WorkflowStep::Filter(and_all(left_conditions)));
        }

        let mut right_wf = right_workflow.clone();
        if !right_conditions.is_empty() {
            right_wf
                .steps
                .push(WorkflowStep::Filter(and_all(right_conditions)));
        }
        result.push(WorkflowStep::Join(join.clone(), right_wf));

        if !remaining_conditions.is_empty() {
            result.push(WorkflowStep::Filter(and_all(remaining_conditions)));
        }

        OptimizationResult::Changed(result)
    }
}

fn flatten_and_conditions(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::And(l, r) => {
            let mut out = flatten_and_conditions(*l);
            out.extend(flatten_and_conditions(*r));
            out
        }
        other => vec![other],
    }
}

pub(crate) fn right_workflow_fields(steps: &[WorkflowStep]) -> Option<HashSet<Field>> {
    match steps.last()? {
        WorkflowStep::Project(fields) => Some(fields.iter().map(|pf| pf.to.clone()).collect()),
        WorkflowStep::Summarize(summarize) => {
            let mut fields: HashSet<Field> = summarize.aggs.keys().cloned().collect();
            fields.extend(summarize.by.iter().map(|bf| bf.name.clone()));
            Some(fields)
        }
        _ => None,
    }
}

fn classify_condition(condition: &Expr, right_fields: &HashSet<Field>) -> ConditionSide {
    let fields = condition.fields();
    let has_right = fields.iter().any(|f| right_fields.contains(f));
    let has_left = fields.iter().any(|f| !right_fields.contains(f));
    match (has_left, has_right) {
        (true, false) => ConditionSide::Left,
        (false, true) => ConditionSide::Right,
        _ => ConditionSide::Both,
    }
}

#[cfg(test)]
mod tests {
    use miso_workflow_types::join::JoinType;

    use super::*;
    use crate::test_utils::{and, eq, field_expr, gt, join, lit, right_project, summarize_by};

    fn apply(steps: &[WorkflowStep]) -> OptimizationResult {
        let opt = PushFilterIntoJoin;
        opt.apply(steps, &[])
    }

    #[test]
    fn inner_join_split_push() {
        let steps = vec![
            join(JoinType::Inner, "id", "rid", right_project(&["rf", "rid"])),
            WorkflowStep::Filter(and(
                gt(field_expr("lf"), lit(5)),
                eq(field_expr("rf"), lit(10)),
            )),
        ];

        let result = apply(&steps);
        let OptimizationResult::Changed(new_steps) = result else {
            panic!("expected Changed");
        };

        assert_eq!(new_steps.len(), 2);
        assert!(
            matches!(&new_steps[0], WorkflowStep::Filter(expr) if *expr == gt(field_expr("lf"), lit(5)))
        );

        let WorkflowStep::Join(_, rw) = &new_steps[1] else {
            panic!("expected Join");
        };
        assert_eq!(rw.steps.len(), 2);
        assert!(
            matches!(&rw.steps[1], WorkflowStep::Filter(expr) if *expr == eq(field_expr("rf"), lit(10)))
        );
    }

    #[test]
    fn inner_join_both_side_predicate() {
        let steps = vec![
            join(JoinType::Inner, "id", "rid", right_project(&["rf", "rid"])),
            WorkflowStep::Filter(gt(
                Expr::Plus(Box::new(field_expr("lf")), Box::new(field_expr("rf"))),
                lit(10),
            )),
        ];

        assert_eq!(apply(&steps), OptimizationResult::Unchanged);
    }

    #[test]
    fn left_join_only_left_pushed() {
        let steps = vec![
            join(JoinType::Left, "id", "rid", right_project(&["rf", "rid"])),
            WorkflowStep::Filter(and(
                gt(field_expr("lf"), lit(5)),
                eq(field_expr("rf"), lit(10)),
            )),
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        assert_eq!(new_steps.len(), 3);
        assert!(
            matches!(&new_steps[0], WorkflowStep::Filter(expr) if *expr == gt(field_expr("lf"), lit(5)))
        );
        let WorkflowStep::Join(_, rw) = &new_steps[1] else {
            panic!("expected Join");
        };
        assert_eq!(rw.steps.len(), 1);
        assert!(
            matches!(&new_steps[2], WorkflowStep::Filter(expr) if *expr == eq(field_expr("rf"), lit(10)))
        );
    }

    #[test]
    fn right_join_only_right_pushed() {
        let steps = vec![
            join(JoinType::Right, "id", "rid", right_project(&["rf", "rid"])),
            WorkflowStep::Filter(and(
                gt(field_expr("lf"), lit(5)),
                eq(field_expr("rf"), lit(10)),
            )),
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        assert_eq!(new_steps.len(), 2);
        let WorkflowStep::Join(_, rw) = &new_steps[0] else {
            panic!("expected Join");
        };
        assert_eq!(rw.steps.len(), 2);
        assert!(
            matches!(&rw.steps[1], WorkflowStep::Filter(expr) if *expr == eq(field_expr("rf"), lit(10)))
        );
        assert!(
            matches!(&new_steps[1], WorkflowStep::Filter(expr) if *expr == gt(field_expr("lf"), lit(5)))
        );
    }

    #[test]
    fn outer_join_unchanged() {
        let steps = vec![
            join(JoinType::Outer, "id", "rid", right_project(&["rf", "rid"])),
            WorkflowStep::Filter(gt(field_expr("lf"), lit(5))),
        ];

        assert_eq!(apply(&steps), OptimizationResult::Unchanged);
    }

    #[test]
    fn join_key_overlap_classified_as_left() {
        let steps = vec![
            join(JoinType::Inner, "id", "id", right_project(&["rf", "id"])),
            WorkflowStep::Filter(gt(field_expr("id"), lit(5))),
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        assert_eq!(new_steps.len(), 2);
        assert!(
            matches!(&new_steps[0], WorkflowStep::Filter(expr) if *expr == gt(field_expr("id"), lit(5)))
        );
        assert!(matches!(&new_steps[1], WorkflowStep::Join(..)));
    }

    #[test]
    fn single_left_only_predicate_pushed() {
        let steps = vec![
            join(JoinType::Inner, "id", "rid", right_project(&["rf", "rid"])),
            WorkflowStep::Filter(gt(field_expr("lf"), lit(5))),
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        assert_eq!(new_steps.len(), 2);
        assert!(matches!(&new_steps[0], WorkflowStep::Filter(..)));
        assert!(matches!(&new_steps[1], WorkflowStep::Join(..)));
    }

    #[test]
    fn no_field_info_unchanged() {
        let steps = vec![
            join(
                JoinType::Inner,
                "id",
                "rid",
                vec![WorkflowStep::Filter(gt(field_expr("x"), lit(1)))],
            ),
            WorkflowStep::Filter(gt(field_expr("lf"), lit(5))),
        ];

        assert_eq!(apply(&steps), OptimizationResult::Unchanged);
    }

    #[test]
    fn summarize_right_workflow() {
        let steps = vec![
            join(
                JoinType::Inner,
                "id",
                "rid",
                vec![summarize_by(&["rf", "rid"])],
            ),
            WorkflowStep::Filter(and(
                gt(field_expr("lf"), lit(5)),
                eq(field_expr("rf"), lit(10)),
            )),
        ];

        let OptimizationResult::Changed(new_steps) = apply(&steps) else {
            panic!("expected Changed");
        };

        assert_eq!(new_steps.len(), 2);
        assert!(matches!(&new_steps[0], WorkflowStep::Filter(..)));
        let WorkflowStep::Join(_, rw) = &new_steps[1] else {
            panic!("expected Join");
        };
        assert_eq!(rw.steps.len(), 2);
    }
}
