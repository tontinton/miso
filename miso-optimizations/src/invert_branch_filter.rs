//! Converts filters on computed case expressions back to their original conditions.
//!
//! When you create a field via `case()` and then filter on it, we can "invert"
//! the filter back to the original condition. This eliminates the intermediate
//! field and enables further optimizations (like pushing the filter to the connector).
//!
//! Example:
//!   extend x = case(id == 7, "no", "yes") | where x == "no"
//! becomes:
//!   where id == 7
//!
//! For multi-branch cases or filtering on the default value, we generate the
//! appropriate AND/NOT combinations to match exactly the right rows.

use miso_workflow::WorkflowStep;
use miso_workflow_types::{expr::Expr, field::Field, project::ProjectField};

use crate::{Group, Optimization, OptimizationResult, Pattern, pattern};

pub struct InvertBranchFilter;

impl Optimization for InvertBranchFilter {
    fn pattern(&self) -> Pattern {
        pattern!([Extend Project] Filter)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let (fields, is_extend) = match &steps[0] {
            WorkflowStep::Extend(fields) => (fields, true),
            WorkflowStep::Project(fields) => (fields, false),
            _ => return OptimizationResult::Unchanged,
        };
        let WorkflowStep::Filter(filter_expr) = &steps[1] else {
            return OptimizationResult::Unchanged;
        };

        let mut inverted_fields = Vec::new();
        let new_filter = invert_expr(filter_expr, fields, &mut inverted_fields);

        if inverted_fields.is_empty() {
            return OptimizationResult::Unchanged;
        }

        let remaining_fields: Vec<ProjectField> = fields
            .iter()
            .filter(|f| !inverted_fields.contains(&f.to))
            .cloned()
            .collect();

        let mut new_steps = vec![WorkflowStep::Filter(new_filter)];

        if !remaining_fields.is_empty() {
            if is_extend {
                new_steps.push(WorkflowStep::Extend(remaining_fields));
            } else {
                new_steps.push(WorkflowStep::Project(remaining_fields));
            }
        }

        OptimizationResult::Changed(new_steps)
    }
}

fn invert_expr(expr: &Expr, fields: &[ProjectField], inverted_fields: &mut Vec<Field>) -> Expr {
    match expr {
        Expr::Eq(left, right) => {
            if let Some(inverted) = try_invert_equality(left, right, fields, inverted_fields) {
                return inverted;
            }
            if let Some(inverted) = try_invert_equality(right, left, fields, inverted_fields) {
                return inverted;
            }
            expr.clone()
        }
        Expr::And(left, right) => Expr::And(
            Box::new(invert_expr(left, fields, inverted_fields)),
            Box::new(invert_expr(right, fields, inverted_fields)),
        ),
        Expr::Or(left, right) => Expr::Or(
            Box::new(invert_expr(left, fields, inverted_fields)),
            Box::new(invert_expr(right, fields, inverted_fields)),
        ),
        _ => expr.clone(),
    }
}

fn try_invert_equality(
    left: &Expr,
    right: &Expr,
    fields: &[ProjectField],
    inverted_fields: &mut Vec<Field>,
) -> Option<Expr> {
    let Expr::Field(field) = left else {
        return None;
    };
    let Expr::Literal(target_value) = right else {
        return None;
    };
    let project_field = fields.iter().find(|f| &f.to == field)?;
    let inverted = project_field.from.try_invert_branch(target_value)?;
    inverted_fields.push(field.clone());
    Some(inverted)
}

#[cfg(test)]
mod tests {
    use miso_workflow::WorkflowStep as S;
    use miso_workflow_types::expr::Expr;
    use miso_workflow_types::value::Value;

    use super::InvertBranchFilter;
    use crate::test_utils::{and, case, eq, field, field_expr, not, or, project_field, string_val};
    use crate::{Optimization, OptimizationResult};

    fn lit_str(s: &str) -> Expr {
        Expr::Literal(string_val(s))
    }

    fn lit_int(n: i64) -> Expr {
        Expr::Literal(Value::Int(n))
    }

    #[test]
    fn simple_branch_true_match() {
        // extend x = case(questionId == 7, "no", "yes") | where x == "no"
        // ->
        // where questionId == 7
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            case(
                vec![(eq(field_expr("questionId"), lit_int(7)), lit_str("no"))],
                lit_str("yes"),
            ),
        )]);
        let filter = S::Filter(eq(field_expr("x"), lit_str("no")));

        let result = opt.apply(&[extend, filter], &[]);

        match result {
            OptimizationResult::Changed(steps) => {
                assert_eq!(steps.len(), 1);
                let S::Filter(new_filter) = &steps[0] else {
                    panic!("expected Filter")
                };
                assert_eq!(*new_filter, eq(field_expr("questionId"), lit_int(7)));
            }
            _ => panic!("expected Changed"),
        }
    }

    #[test]
    fn simple_branch_false_match() {
        // extend x = case(questionId == 7, "no", "yes") | where x == "yes"
        // ->
        // where not(questionId == 7)
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            case(
                vec![(eq(field_expr("questionId"), lit_int(7)), lit_str("no"))],
                lit_str("yes"),
            ),
        )]);
        let filter = S::Filter(eq(field_expr("x"), lit_str("yes")));

        let result = opt.apply(&[extend, filter], &[]);

        match result {
            OptimizationResult::Changed(steps) => {
                assert_eq!(steps.len(), 1);
                let S::Filter(new_filter) = &steps[0] else {
                    panic!("expected Filter")
                };
                assert_eq!(*new_filter, not(eq(field_expr("questionId"), lit_int(7))));
            }
            _ => panic!("expected Changed"),
        }
    }

    #[test]
    fn no_match_gives_false() {
        // extend x = case(questionId == 7, "no", "yes") | where x == "maybe"
        // ->
        // where false
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            case(
                vec![(eq(field_expr("questionId"), lit_int(7)), lit_str("no"))],
                lit_str("yes"),
            ),
        )]);
        let filter = S::Filter(eq(field_expr("x"), lit_str("maybe")));

        let result = opt.apply(&[extend, filter], &[]);

        match result {
            OptimizationResult::Changed(steps) => {
                assert_eq!(steps.len(), 1);
                let S::Filter(new_filter) = &steps[0] else {
                    panic!("expected Filter")
                };
                assert_eq!(
                    *new_filter,
                    Expr::Literal(miso_workflow_types::value::Value::Bool(false))
                );
            }
            _ => panic!("expected Changed"),
        }
    }

    #[test]
    fn multi_branch_middle_match() {
        // extend x = case(a == 1, "one", a == 2, "two", "other") | where x == "two"
        // ->
        // where not(a == 1) and a == 2
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            case(
                vec![
                    (eq(field_expr("a"), lit_int(1)), lit_str("one")),
                    (eq(field_expr("a"), lit_int(2)), lit_str("two")),
                ],
                lit_str("other"),
            ),
        )]);
        let filter = S::Filter(eq(field_expr("x"), lit_str("two")));

        let result = opt.apply(&[extend, filter], &[]);

        match result {
            OptimizationResult::Changed(steps) => {
                assert_eq!(steps.len(), 1);
                let S::Filter(new_filter) = &steps[0] else {
                    panic!("expected Filter")
                };
                let expected = and(
                    not(eq(field_expr("a"), lit_int(1))),
                    eq(field_expr("a"), lit_int(2)),
                );
                assert_eq!(*new_filter, expected);
            }
            _ => panic!("expected Changed"),
        }
    }

    #[test]
    fn multi_branch_default_match() {
        // extend x = case(a == 1, "one", a == 2, "two", "other") | where x == "other"
        // ->
        // where not(a == 1) and NOT(a == 2)
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            case(
                vec![
                    (eq(field_expr("a"), lit_int(1)), lit_str("one")),
                    (eq(field_expr("a"), lit_int(2)), lit_str("two")),
                ],
                lit_str("other"),
            ),
        )]);
        let filter = S::Filter(eq(field_expr("x"), lit_str("other")));

        let result = opt.apply(&[extend, filter], &[]);

        match result {
            OptimizationResult::Changed(steps) => {
                assert_eq!(steps.len(), 1);
                let S::Filter(new_filter) = &steps[0] else {
                    panic!("expected Filter")
                };
                let expected = and(
                    not(eq(field_expr("a"), lit_int(1))),
                    not(eq(field_expr("a"), lit_int(2))),
                );
                assert_eq!(*new_filter, expected);
            }
            _ => panic!("expected Changed"),
        }
    }

    #[test]
    fn project_variant() {
        // project x = case(questionId == 7, "no", "yes") | where x == "no"
        // ->
        // where questionId == 7
        let opt = InvertBranchFilter;
        let project = S::Project(vec![project_field(
            "x",
            case(
                vec![(eq(field_expr("questionId"), lit_int(7)), lit_str("no"))],
                lit_str("yes"),
            ),
        )]);
        let filter = S::Filter(eq(field_expr("x"), lit_str("no")));

        let result = opt.apply(&[project, filter], &[]);

        match result {
            OptimizationResult::Changed(steps) => {
                assert_eq!(steps.len(), 1);
                let S::Filter(new_filter) = &steps[0] else {
                    panic!("expected Filter")
                };
                assert_eq!(*new_filter, eq(field_expr("questionId"), lit_int(7)));
            }
            _ => panic!("expected Changed"),
        }
    }

    #[test]
    fn preserves_other_fields() {
        // extend x = case(a == 1, "yes", "no"), y = b + 1 | where x == "yes"
        // ->
        // where a == 1 | extend y = b + 1
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![
            project_field(
                "x",
                case(
                    vec![(eq(field_expr("a"), lit_int(1)), lit_str("yes"))],
                    lit_str("no"),
                ),
            ),
            project_field(
                "y",
                Expr::Plus(Box::new(field_expr("b")), Box::new(lit_int(1))),
            ),
        ]);
        let filter = S::Filter(eq(field_expr("x"), lit_str("yes")));

        let result = opt.apply(&[extend, filter], &[]);

        match result {
            OptimizationResult::Changed(steps) => {
                assert_eq!(steps.len(), 2);
                let S::Filter(new_filter) = &steps[0] else {
                    panic!("expected Filter")
                };
                assert_eq!(*new_filter, eq(field_expr("a"), lit_int(1)));
                let S::Extend(remaining) = &steps[1] else {
                    panic!("expected Extend")
                };
                assert_eq!(remaining.len(), 1);
                assert_eq!(remaining[0].to, field("y"));
            }
            _ => panic!("expected Changed"),
        }
    }

    #[test]
    fn filter_with_and() {
        // extend x = case(a == 1, "yes", "no") | where x == "yes" AND b > 5
        // ->
        // where a == 1 AND b > 5
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            case(
                vec![(eq(field_expr("a"), lit_int(1)), lit_str("yes"))],
                lit_str("no"),
            ),
        )]);
        let filter = S::Filter(and(
            eq(field_expr("x"), lit_str("yes")),
            Expr::Gt(Box::new(field_expr("b")), Box::new(lit_int(5))),
        ));

        let result = opt.apply(&[extend, filter], &[]);

        match result {
            OptimizationResult::Changed(steps) => {
                assert_eq!(steps.len(), 1);
                let S::Filter(new_filter) = &steps[0] else {
                    panic!("expected Filter")
                };
                let expected = and(
                    eq(field_expr("a"), lit_int(1)),
                    Expr::Gt(Box::new(field_expr("b")), Box::new(lit_int(5))),
                );
                assert_eq!(*new_filter, expected);
            }
            _ => panic!("expected Changed"),
        }
    }

    #[test]
    fn unchanged_non_literal_branch_values() {
        // extend x = case(a == 1, b, "no") | where x == "yes"
        // ->
        // unchanged (b is not a literal)
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            case(
                vec![(eq(field_expr("a"), lit_int(1)), field_expr("b"))],
                lit_str("no"),
            ),
        )]);
        let filter = S::Filter(eq(field_expr("x"), lit_str("yes")));

        let result = opt.apply(&[extend, filter], &[]);

        assert_eq!(result, OptimizationResult::Unchanged);
    }

    #[test]
    fn unchanged_non_case_expression() {
        // extend x = a + 1 | where x == 5
        // ->
        // unchanged (not a case expression)
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            Expr::Plus(Box::new(field_expr("a")), Box::new(lit_int(1))),
        )]);
        let filter = S::Filter(eq(field_expr("x"), lit_int(5)));

        let result = opt.apply(&[extend, filter], &[]);

        assert_eq!(result, OptimizationResult::Unchanged);
    }

    #[test]
    fn unchanged_filter_without_equality() {
        // extend x = case(a == 1, "yes", "no") | where x
        // ->
        // unchanged (filter is not an equality)
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            case(
                vec![(eq(field_expr("a"), lit_int(1)), lit_str("yes"))],
                lit_str("no"),
            ),
        )]);
        let filter = S::Filter(field_expr("x"));

        let result = opt.apply(&[extend, filter], &[]);

        assert_eq!(result, OptimizationResult::Unchanged);
    }

    #[test]
    fn multiple_matches_gives_or() {
        // extend x = case(a == 1, "yes", a == 2, "yes", "no") | where x == "yes"
        // ->
        // where (a == 1) or (not(a == 1) and a == 2)
        let opt = InvertBranchFilter;
        let extend = S::Extend(vec![project_field(
            "x",
            case(
                vec![
                    (eq(field_expr("a"), lit_int(1)), lit_str("yes")),
                    (eq(field_expr("a"), lit_int(2)), lit_str("yes")),
                ],
                lit_str("no"),
            ),
        )]);
        let filter = S::Filter(eq(field_expr("x"), lit_str("yes")));

        let result = opt.apply(&[extend, filter], &[]);

        match result {
            OptimizationResult::Changed(steps) => {
                assert_eq!(steps.len(), 1);
                let S::Filter(new_filter) = &steps[0] else {
                    panic!("expected Filter")
                };
                let expected = or(
                    eq(field_expr("a"), lit_int(1)),
                    and(
                        not(eq(field_expr("a"), lit_int(1))),
                        eq(field_expr("a"), lit_int(2)),
                    ),
                );
                assert_eq!(*new_filter, expected);
            }
            _ => panic!("expected Changed"),
        }
    }
}
