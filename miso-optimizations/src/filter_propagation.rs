//! Pushes filter constraints into downstream summarize expressions.
//!
//! When you filter on `x == 5` and then summarize by a `case` expression that
//! references `x`, we can substitute the literal `5` for `x` inside that case.
//! This often simplifies the expression (e.g., `case(x == 7, ...)` becomes
//! `case(5 == 7, ...)` which constant-folds to false).
//!
//! Range constraints (`>`, `>=`, `<`, `<=`) are also propagated: if
//! `x > 5`, then `x > 3` is replaced with `true` and `x < 3` with `false`.
//!
//! Example:
//!   where x == 5 | summarize by case(x == 7, "yes", "no")
//! becomes:
//!   where x == 5 | summarize by case(5 == 7, "yes", "no")
//!
//! The const folding optimization will further simplify the query.

use miso_workflow::WorkflowStep;
use miso_workflow_types::{
    expr::Expr,
    field::Field,
    summarize::{ByField, Summarize},
    value::Value,
};
use std::collections::BTreeMap;

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

        let eq_constraints = extract_equality_constraints(filter_expr);
        let range_constraints = extract_range_constraints(filter_expr);

        if eq_constraints.is_empty() && range_constraints.is_empty() {
            return OptimizationResult::Unchanged;
        }

        let renames = BTreeMap::new();
        let expr_subst = ExprSubstitute::new(&renames, &eq_constraints);

        let (middle_start, middle_end) = groups[0];
        let mut changed = false;
        let mut new_steps = vec![steps[0].clone()];

        for step in &steps[middle_start..middle_end] {
            let new_step = match step {
                WorkflowStep::Summarize(sum) => {
                    let (new_sum, did_change) =
                        rewrite_summarize(sum, &expr_subst, &range_constraints);
                    changed |= did_change;
                    WorkflowStep::Summarize(new_sum)
                }
                WorkflowStep::MuxSummarize(sum) => {
                    let (new_sum, did_change) =
                        rewrite_summarize(sum, &expr_subst, &range_constraints);
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

#[derive(Debug, Clone, Copy, PartialEq)]
enum CmpOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

impl CmpOp {
    fn flip(self) -> Self {
        match self {
            Self::Gt => Self::Lt,
            Self::Gte => Self::Lte,
            Self::Lt => Self::Gt,
            Self::Lte => Self::Gte,
        }
    }

    fn is_lower(self) -> bool {
        matches!(self, Self::Gt | Self::Gte)
    }

    fn is_strict(self) -> bool {
        matches!(self, Self::Gt | Self::Lt)
    }
}

#[derive(Debug, Clone)]
struct RangeBound {
    op: CmpOp,
    val: f64,
}

fn cmp_operands(expr: &Expr) -> Option<(CmpOp, &Expr, &Expr)> {
    match expr {
        Expr::Gt(l, r) => Some((CmpOp::Gt, l, r)),
        Expr::Gte(l, r) => Some((CmpOp::Gte, l, r)),
        Expr::Lt(l, r) => Some((CmpOp::Lt, l, r)),
        Expr::Lte(l, r) => Some((CmpOp::Lte, l, r)),
        _ => None,
    }
}

fn normalize_field_cmp<'a>(
    op: CmpOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Option<(&'a Field, CmpOp, f64)> {
    match (left, right) {
        (Expr::Field(f), Expr::Literal(v)) => Some((f, op, v.as_f64()?)),
        (Expr::Literal(v), Expr::Field(f)) => Some((f, op.flip(), v.as_f64()?)),
        _ => None,
    }
}

fn extract_range_constraints(expr: &Expr) -> BTreeMap<Field, Vec<RangeBound>> {
    let mut result: BTreeMap<Field, Vec<RangeBound>> = BTreeMap::new();
    if let Some((op, left, right)) = cmp_operands(expr) {
        if let Some((field, op, val)) = normalize_field_cmp(op, left, right) {
            result
                .entry(field.clone())
                .or_default()
                .push(RangeBound { op, val });
        }
    } else if let Expr::And(left, right) = expr {
        result = extract_range_constraints(left);
        for (field, bounds) in extract_range_constraints(right) {
            result.entry(field).or_default().extend(bounds);
        }
    }
    result
}

fn check_implication(bounds: &[RangeBound], check_op: CmpOp, check_val: f64) -> Option<bool> {
    bounds
        .iter()
        .find_map(|b| check_single_implication(b.op, b.val, check_op, check_val))
}

fn check_single_implication(known_op: CmpOp, kv: f64, check_op: CmpOp, cv: f64) -> Option<bool> {
    if known_op.is_lower() == check_op.is_lower() {
        let stronger = if known_op.is_lower() {
            kv > cv || (kv == cv && (known_op.is_strict() || !check_op.is_strict()))
        } else {
            kv < cv || (kv == cv && (known_op.is_strict() || !check_op.is_strict()))
        };
        stronger.then_some(true)
    } else {
        // Opposite direction: contradiction when ranges don't overlap
        let contradicts = if known_op.is_lower() {
            cv < kv || (cv == kv && (known_op.is_strict() || check_op.is_strict()))
        } else {
            cv > kv || (cv == kv && (known_op.is_strict() || check_op.is_strict()))
        };
        contradicts.then_some(false)
    }
}

fn try_resolve_cmp(expr: &Expr, constraints: &BTreeMap<Field, Vec<RangeBound>>) -> Option<bool> {
    let (op, left, right) = cmp_operands(expr)?;
    let (field, op, val) = normalize_field_cmp(op, left, right)?;
    check_implication(constraints.get(field)?, op, val)
}

fn apply_range_constraints(expr: Expr, constraints: &BTreeMap<Field, Vec<RangeBound>>) -> Expr {
    if let Some(result) = try_resolve_cmp(&expr, constraints) {
        return Expr::Literal(Value::Bool(result));
    }
    let f = |e: Expr| apply_range_constraints(e, constraints);
    let fb = |e: Box<Expr>| Box::new(f(*e));
    match expr {
        Expr::Gt(a, b) => Expr::Gt(fb(a), fb(b)),
        Expr::Gte(a, b) => Expr::Gte(fb(a), fb(b)),
        Expr::Lt(a, b) => Expr::Lt(fb(a), fb(b)),
        Expr::Lte(a, b) => Expr::Lte(fb(a), fb(b)),
        Expr::Eq(a, b) => Expr::Eq(fb(a), fb(b)),
        Expr::Ne(a, b) => Expr::Ne(fb(a), fb(b)),
        Expr::And(a, b) => Expr::And(fb(a), fb(b)),
        Expr::Or(a, b) => Expr::Or(fb(a), fb(b)),
        Expr::Not(e) => Expr::Not(fb(e)),
        Expr::Case(preds, def) => Expr::Case(
            preds.into_iter().map(|(p, t)| (f(p), f(t))).collect(),
            fb(def),
        ),
        other => other,
    }
}

fn extract_equality_constraints(expr: &Expr) -> BTreeMap<Field, Value> {
    match expr {
        Expr::Eq(left, right) => match (left.as_ref(), right.as_ref()) {
            (Expr::Field(f), Expr::Literal(v)) | (Expr::Literal(v), Expr::Field(f)) => {
                [(f.clone(), v.clone())].into_iter().collect()
            }
            _ => BTreeMap::new(),
        },
        Expr::And(left, right) => {
            let mut constraints = extract_equality_constraints(left);
            for (field, value) in extract_equality_constraints(right) {
                constraints.entry(field).or_insert(value);
            }
            constraints
        }
        _ => BTreeMap::new(),
    }
}

fn rewrite_summarize(
    sum: &Summarize,
    expr_subst: &ExprSubstitute,
    range_constraints: &BTreeMap<Field, Vec<RangeBound>>,
) -> (Summarize, bool) {
    let mut changed = false;
    let new_by = sum
        .by
        .iter()
        .map(|bf| {
            let mut new_expr = expr_subst.substitute(bf.expr.clone());
            if !range_constraints.is_empty() {
                new_expr = apply_range_constraints(new_expr, range_constraints);
            }
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
    fn unchanged_for_non_equality_filter_without_comparisons() {
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

    fn gt(l: Expr, r: Expr) -> Expr {
        Expr::Gt(Box::new(l), Box::new(r))
    }

    fn lt(l: Expr, r: Expr) -> Expr {
        Expr::Lt(Box::new(l), Box::new(r))
    }

    fn and(l: Expr, r: Expr) -> Expr {
        Expr::And(Box::new(l), Box::new(r))
    }

    fn lit_bool(b: bool) -> Expr {
        Expr::Literal(miso_workflow_types::value::Value::Bool(b))
    }

    #[test]
    fn range_implies_true() {
        let opt = FilterPropagation;
        let filter = S::Filter(gt(field_expr("x"), lit_int(5)));
        let sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(
                case(
                    vec![(gt(field_expr("x"), lit_int(3)), lit_str("a"))],
                    lit_str("b"),
                ),
                "result",
            )],
        });

        let result = opt.apply(&[filter.clone(), sum], &[(1, 2)]);

        let expected_sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(
                case(vec![(lit_bool(true), lit_str("a"))], lit_str("b")),
                "result",
            )],
        });
        assert_eq!(
            result,
            OptimizationResult::Changed(vec![filter, expected_sum])
        );
    }

    #[test]
    fn range_implies_false() {
        let opt = FilterPropagation;
        let filter = S::Filter(gt(field_expr("x"), lit_int(5)));
        let sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(
                case(
                    vec![(lt(field_expr("x"), lit_int(3)), lit_str("a"))],
                    lit_str("b"),
                ),
                "result",
            )],
        });

        let result = opt.apply(&[filter.clone(), sum], &[(1, 2)]);

        let expected_sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(
                case(vec![(lit_bool(false), lit_str("a"))], lit_str("b")),
                "result",
            )],
        });
        assert_eq!(
            result,
            OptimizationResult::Changed(vec![filter, expected_sum])
        );
    }

    #[test]
    fn range_multiple_bounds() {
        let opt = FilterPropagation;
        let filter = S::Filter(and(
            gt(field_expr("x"), lit_int(3)),
            lt(field_expr("x"), lit_int(10)),
        ));
        let sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(
                case(
                    vec![(gt(field_expr("x"), lit_int(1)), lit_str("a"))],
                    lit_str("b"),
                ),
                "result",
            )],
        });

        let result = opt.apply(&[filter.clone(), sum], &[(1, 2)]);

        let expected_sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(
                case(vec![(lit_bool(true), lit_str("a"))], lit_str("b")),
                "result",
            )],
        });
        assert_eq!(
            result,
            OptimizationResult::Changed(vec![filter, expected_sum])
        );
    }

    #[test]
    fn range_undetermined_unchanged() {
        let opt = FilterPropagation;
        let filter = S::Filter(gt(field_expr("x"), lit_int(5)));
        let sum = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(
                case(
                    vec![(gt(field_expr("x"), lit_int(10)), lit_str("a"))],
                    lit_str("b"),
                ),
                "result",
            )],
        });

        assert_eq!(
            opt.apply(&[filter, sum], &[(1, 2)]),
            OptimizationResult::Unchanged
        );
    }
}
