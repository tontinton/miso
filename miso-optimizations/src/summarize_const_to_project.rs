use miso_workflow::WorkflowStep;
use miso_workflow_types::{expr::Expr, project::ProjectField};

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct SummarizeConstToProject;

impl Optimization for SummarizeConstToProject {
    fn pattern(&self) -> Pattern {
        pattern!([Summarize MuxSummarize])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let summarize = match &steps[0] {
            WorkflowStep::Summarize(s) | WorkflowStep::MuxSummarize(s) => s,
            _ => return OptimizationResult::Unchanged,
        };

        if !summarize.aggs.is_empty() {
            return OptimizationResult::Unchanged;
        }

        if !summarize
            .by
            .iter()
            .all(|bf| matches!(bf.expr, Expr::Literal(_)))
        {
            return OptimizationResult::Unchanged;
        }

        let fields = summarize
            .by
            .iter()
            .map(|bf| ProjectField {
                from: bf.expr.clone(),
                to: bf.name.clone(),
            })
            .collect();

        OptimizationResult::Changed(vec![WorkflowStep::Limit(1), WorkflowStep::Project(fields)])
    }
}

#[cfg(test)]
mod tests {
    use miso_workflow::WorkflowStep as S;
    use miso_workflow_types::{expr::Expr, summarize::Summarize};

    use super::SummarizeConstToProject;
    use crate::test_utils::{by_field, field, int_val, literal_project, string_val};
    use crate::{Optimization, OptimizationResult};

    #[test]
    fn converts_single_const() {
        let opt = SummarizeConstToProject;
        let input = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(Expr::Literal(string_val("yes")), "Column1")],
        });
        assert_eq!(
            opt.apply(&[input], &[]),
            OptimizationResult::Changed(vec![
                S::Limit(1),
                S::Project(vec![literal_project("Column1", string_val("yes"))])
            ])
        );
    }

    #[test]
    fn converts_multiple_consts() {
        let opt = SummarizeConstToProject;
        let input = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![
                by_field(Expr::Literal(int_val(1)), "a"),
                by_field(Expr::Literal(string_val("x")), "b"),
            ],
        });
        assert_eq!(
            opt.apply(&[input], &[]),
            OptimizationResult::Changed(vec![
                S::Limit(1),
                S::Project(vec![
                    literal_project("a", int_val(1)),
                    literal_project("b", string_val("x")),
                ])
            ])
        );
    }

    #[test]
    fn converts_mux_summarize() {
        let opt = SummarizeConstToProject;
        let input = S::MuxSummarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(Expr::Literal(int_val(42)), "x")],
        });
        assert_eq!(
            opt.apply(&[input], &[]),
            OptimizationResult::Changed(vec![
                S::Limit(1),
                S::Project(vec![literal_project("x", int_val(42))])
            ])
        );
    }

    #[test]
    fn unchanged_with_aggregations() {
        let opt = SummarizeConstToProject;
        let input = S::Summarize(Summarize {
            aggs: [(
                field("c"),
                miso_workflow_types::summarize::Aggregation::Count,
            )]
            .into_iter()
            .collect(),
            by: vec![by_field(Expr::Literal(int_val(1)), "a")],
        });
        assert_eq!(opt.apply(&[input], &[]), OptimizationResult::Unchanged);
    }

    #[test]
    fn unchanged_with_field_reference() {
        let opt = SummarizeConstToProject;
        let input = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![by_field(Expr::Field(field("x")), "x")],
        });
        assert_eq!(opt.apply(&[input], &[]), OptimizationResult::Unchanged);
    }

    #[test]
    fn unchanged_with_mixed_const_and_field() {
        let opt = SummarizeConstToProject;
        let input = S::Summarize(Summarize {
            aggs: Default::default(),
            by: vec![
                by_field(Expr::Literal(int_val(1)), "a"),
                by_field(Expr::Field(field("x")), "x"),
            ],
        });
        assert_eq!(opt.apply(&[input], &[]), OptimizationResult::Unchanged);
    }
}
