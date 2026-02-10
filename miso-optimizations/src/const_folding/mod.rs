pub mod partial_evaluator;

use miso_workflow::WorkflowStep;
use miso_workflow_types::{
    project::ProjectField,
    summarize::{ByField, Summarize},
};

use crate::{OptimizationResult, const_folding::partial_evaluator::partial_eval, pattern};

use super::{Group, Optimization, Pattern};

/// Turns 'where x > 50 * 10 - 2' into 'where x > 498'.
pub struct ConstFolding;

impl Optimization for ConstFolding {
    fn pattern(&self) -> Pattern {
        pattern!([Filter Project Extend Summarize MuxSummarize]+)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let mut changed = false;
        let new_steps: Vec<_> = steps
            .iter()
            .map(|step| match step {
                WorkflowStep::Filter(expr) => {
                    let new_expr = partial_eval(expr).unwrap_or_else(|_| expr.clone());
                    changed |= new_expr != *expr;
                    WorkflowStep::Filter(new_expr)
                }
                WorkflowStep::Project(fields) => {
                    let (new_fields, did_change) = rewrite_project_fields(fields);
                    changed |= did_change;
                    WorkflowStep::Project(new_fields)
                }
                WorkflowStep::Extend(fields) => {
                    let (new_fields, did_change) = rewrite_project_fields(fields);
                    changed |= did_change;
                    WorkflowStep::Extend(new_fields)
                }
                WorkflowStep::Summarize(sum) => {
                    let (new_sum, did_change) = rewrite_summarize(sum);
                    changed |= did_change;
                    WorkflowStep::Summarize(new_sum)
                }
                WorkflowStep::MuxSummarize(sum) => {
                    let (new_sum, did_change) = rewrite_summarize(sum);
                    changed |= did_change;
                    WorkflowStep::MuxSummarize(new_sum)
                }
                _ => unreachable!("not in const folding pattern"),
            })
            .collect();

        if changed {
            OptimizationResult::Changed(new_steps)
        } else {
            OptimizationResult::Unchanged
        }
    }
}

fn rewrite_project_fields(fields: &[ProjectField]) -> (Vec<ProjectField>, bool) {
    let mut changed = false;
    let new_fields = fields
        .iter()
        .cloned()
        .map(|mut pf| {
            if let Ok(expr) = partial_eval(&pf.from)
                && expr != pf.from
            {
                changed = true;
                pf.from = expr;
            }
            pf
        })
        .collect();
    (new_fields, changed)
}

fn rewrite_summarize(sum: &Summarize) -> (Summarize, bool) {
    let mut changed = false;
    let new_by: Vec<ByField> = sum
        .by
        .iter()
        .map(|bf| {
            let new_expr = partial_eval(&bf.expr).unwrap_or_else(|_| bf.expr.clone());
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
    use test_case::test_case;

    use super::ConstFolding;
    use crate::test_utils::{by_field, field_expr, project_field};
    use crate::{Optimization, OptimizationResult};

    fn summarize_step() -> Summarize {
        Summarize {
            aggs: Default::default(),
            by: vec![by_field(field_expr("x"), "x")],
        }
    }

    #[test_case(vec![S::Filter(field_expr("x"))] ; "filter")]
    #[test_case(vec![S::Project(vec![project_field("a", field_expr("b"))])] ; "project")]
    #[test_case(vec![S::Extend(vec![project_field("a", field_expr("b"))])] ; "extend")]
    #[test_case(vec![S::Summarize(summarize_step())] ; "summarize")]
    #[test_case(vec![S::MuxSummarize(summarize_step())] ; "mux_summarize")]
    fn unchanged_no_folding(input: Vec<S>) {
        let opt = ConstFolding;
        assert_eq!(opt.apply(&input, &[]), OptimizationResult::Unchanged);
    }

    #[test]
    fn changed_filter_with_folding() {
        let opt = ConstFolding;
        let foldable = Expr::Plus(
            Box::new(Expr::Literal(1.into())),
            Box::new(Expr::Literal(2.into())),
        );
        let input = vec![S::Filter(Expr::Eq(
            Box::new(field_expr("x")),
            Box::new(foldable),
        ))];
        assert!(matches!(
            opt.apply(&input, &[]),
            OptimizationResult::Changed(_)
        ));
    }

    #[test]
    fn changed_summarize_with_folding() {
        let opt = ConstFolding;
        let foldable = Expr::Plus(
            Box::new(Expr::Literal(1.into())),
            Box::new(Expr::Literal(2.into())),
        );
        let input = vec![S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(foldable, "x")],
        })];
        let result = opt.apply(&input, &[]);
        match result {
            OptimizationResult::Changed(steps) => {
                let S::Summarize(sum) = &steps[0] else {
                    panic!()
                };
                assert_eq!(sum.by[0].expr, Expr::Literal(3.into()));
            }
            _ => panic!("expected Changed"),
        }
    }
}
