use hashbrown::HashMap;
use miso_workflow::WorkflowStep;
use miso_workflow_types::{
    expr::Expr,
    field::Field,
    summarize::{ByField, Summarize},
    value::Value,
};

use crate::{
    Group, Optimization, OptimizationResult, Pattern, pattern,
    project_propagation::expr_substitude::ExprSubstitute,
};

pub struct FilterPropagation;

impl Optimization for FilterPropagation {
    fn pattern(&self) -> Pattern {
        pattern!(Filter ([Summarize MuxSummarize Extend Project]+))
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> OptimizationResult {
        let WorkflowStep::Filter(filter_expr) = &steps[0] else {
            return OptimizationResult::Unchanged;
        };

        let constraints = extract_equality_constraints(filter_expr);
        if constraints.is_empty() {
            return OptimizationResult::Unchanged;
        }

        let renames = HashMap::new();
        let expr_subst = ExprSubstitute::new(&renames, &constraints);

        let (middle_start, middle_end) = groups[0];
        let mut changed = false;
        let mut new_steps = vec![steps[0].clone()];

        for step in &steps[middle_start..middle_end] {
            let new_step = match step {
                WorkflowStep::Summarize(sum) => {
                    let (new_sum, did_change) = rewrite_summarize(sum, &expr_subst);
                    changed |= did_change;
                    WorkflowStep::Summarize(new_sum)
                }
                WorkflowStep::MuxSummarize(sum) => {
                    let (new_sum, did_change) = rewrite_summarize(sum, &expr_subst);
                    changed |= did_change;
                    WorkflowStep::MuxSummarize(new_sum)
                }
                _ => step.clone(),
            };
            new_steps.push(new_step);
        }

        if changed {
            OptimizationResult::Changed(new_steps)
        } else {
            OptimizationResult::Unchanged
        }
    }
}

fn extract_equality_constraints(expr: &Expr) -> HashMap<Field, Value> {
    match expr {
        Expr::Eq(left, right) => match (left.as_ref(), right.as_ref()) {
            (Expr::Field(f), Expr::Literal(v)) | (Expr::Literal(v), Expr::Field(f)) => {
                [(f.clone(), v.clone())].into_iter().collect()
            }
            _ => HashMap::new(),
        },
        Expr::And(left, right) => {
            let mut constraints = extract_equality_constraints(left);
            for (field, value) in extract_equality_constraints(right) {
                if !constraints.contains_key(&field) {
                    constraints.insert(field, value);
                }
            }
            constraints
        }
        _ => HashMap::new(),
    }
}

fn rewrite_summarize(sum: &Summarize, expr_subst: &ExprSubstitute) -> (Summarize, bool) {
    let mut changed = false;
    let new_by = sum
        .by
        .iter()
        .map(|bf| {
            let new_expr = expr_subst.substitute(bf.expr.clone());
            if new_expr != bf.expr {
                changed = true;
            }
            ByField {
                name: bf.name.clone(),
                expr: new_expr,
            }
        })
        .collect();

    (
        Summarize {
            aggs: sum.aggs.clone(),
            by: new_by,
        },
        changed,
    )
}

#[cfg(test)]
mod tests {
    use miso_workflow::WorkflowStep as S;
    use miso_workflow_types::{expr::Expr, summarize::Summarize};

    use super::FilterPropagation;
    use crate::test_utils::{by_field, case, field_expr, int_val, string_val};
    use crate::{Optimization, OptimizationResult};

    fn eq(l: Expr, r: Expr) -> Expr {
        Expr::Eq(Box::new(l), Box::new(r))
    }

    fn lit_int(n: i64) -> Expr {
        Expr::Literal(int_val(n as i32))
    }

    fn lit_str(s: &str) -> Expr {
        Expr::Literal(string_val(s))
    }

    #[test]
    fn substitutes_field_in_case_expr() {
        let opt = FilterPropagation;
        let filter = S::Filter(eq(field_expr("x"), lit_int(5)));
        let sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(
                case(
                    vec![(eq(field_expr("x"), lit_int(7)), lit_str("no"))],
                    lit_str("yes"),
                ),
                "result",
            )],
        });

        let result = opt.apply(&[filter.clone(), sum], &[(1, 2)]);

        let expected_sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(
                case(
                    vec![(eq(lit_int(5), lit_int(7)), lit_str("no"))],
                    lit_str("yes"),
                ),
                "result",
            )],
        });
        assert_eq!(
            result,
            OptimizationResult::Changed(vec![filter, expected_sum])
        );
    }

    #[test]
    fn extracts_multiple_constraints_from_and() {
        let opt = FilterPropagation;
        let filter = S::Filter(Expr::And(
            Box::new(eq(field_expr("x"), lit_int(5))),
            Box::new(eq(field_expr("y"), lit_str("foo"))),
        ));
        let sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![
                by_field(field_expr("x"), "a"),
                by_field(field_expr("y"), "b"),
            ],
        });

        let result = opt.apply(&[filter.clone(), sum], &[(1, 2)]);

        let expected_sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(lit_int(5), "a"), by_field(lit_str("foo"), "b")],
        });
        assert_eq!(
            result,
            OptimizationResult::Changed(vec![filter, expected_sum])
        );
    }

    #[test]
    fn unchanged_when_no_matching_fields() {
        let opt = FilterPropagation;
        let filter = S::Filter(eq(field_expr("x"), lit_int(5)));
        let sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(field_expr("y"), "y")],
        });

        assert_eq!(
            opt.apply(&[filter, sum], &[(1, 2)]),
            OptimizationResult::Unchanged
        );
    }

    #[test]
    fn unchanged_for_non_equality_filter() {
        let opt = FilterPropagation;
        let filter = S::Filter(Expr::Gt(Box::new(field_expr("x")), Box::new(lit_int(5))));
        let sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(field_expr("x"), "x")],
        });

        assert_eq!(
            opt.apply(&[filter, sum], &[(1, 2)]),
            OptimizationResult::Unchanged
        );
    }
}
