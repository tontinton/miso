use hashbrown::HashSet;
use miso_workflow::WorkflowStep;
use miso_workflow_types::{expr::Expr, field::Field, project::ProjectField};

use crate::{Group, Optimization, OptimizationResult, Pattern, pattern};

/// Insert project after scan to drop unused fields early.
/// This optimization has great synergy with the project propagation optimization.
///
/// Example:
///   scan | filter x > 0 | count
///  ->
///   scan | project x | filter x > 0 | count
pub struct EliminateUnusedFields;

impl Optimization for EliminateUnusedFields {
    fn pattern(&self) -> Pattern {
        // End at steps that outputs different fields.
        pattern!(Scan ([^Tee]+?) [Count MuxCount Summarize MuxSummarize Project])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        apply(steps)
    }
}

fn apply(steps: &[WorkflowStep]) -> OptimizationResult {
    match transform_steps(steps) {
        Some(steps) => OptimizationResult::Changed(steps),
        None => OptimizationResult::Unchanged,
    }
}

fn transform_steps(steps: &[WorkflowStep]) -> Option<Vec<WorkflowStep>> {
    if steps.is_empty() {
        return None;
    }

    let mut required = HashSet::new();
    for step in steps.iter().rev() {
        required = compute_required_before_step(step, required);
    }

    if !required.is_empty() {
        let mut result = steps.to_vec();
        let project = create_identity_project(required);
        result.insert(1, project);
        return Some(result);
    }

    None
}

fn create_identity_project(fields: HashSet<Field>) -> WorkflowStep {
    WorkflowStep::Project(
        fields
            .into_iter()
            .map(|f| {
                let to = f.clone();
                ProjectField {
                    from: Expr::Field(f),
                    to,
                }
            })
            .collect(),
    )
}

fn compute_required_before_step(step: &WorkflowStep, mut after: HashSet<Field>) -> HashSet<Field> {
    match step {
        WorkflowStep::Count | WorkflowStep::MuxCount => HashSet::new(),

        WorkflowStep::Summarize(s) | WorkflowStep::MuxSummarize(s) => s.used_fields(),

        WorkflowStep::Project(fields) => {
            let mut required = HashSet::new();
            for pf in fields {
                required.extend(pf.from.fields());
            }
            required
        }

        WorkflowStep::Extend(fields) => {
            let mut inputs_needed = HashSet::new();
            for pf in fields {
                if after.contains(&pf.to) {
                    inputs_needed.extend(pf.from.fields());
                }
            }
            for pf in fields {
                after.remove(&pf.to);
            }
            after.extend(inputs_needed);
            after
        }

        WorkflowStep::Rename(renames) => {
            let mut inputs_needed = HashSet::new();
            for (from, to) in renames {
                if after.contains(to) {
                    inputs_needed.insert(from.clone());
                }
            }
            for (_, to) in renames {
                after.remove(to);
            }
            after.extend(inputs_needed);
            after
        }

        WorkflowStep::Filter(expr) => {
            after.extend(expr.fields());
            after
        }

        WorkflowStep::Sort(sorts)
        | WorkflowStep::TopN(sorts, _)
        | WorkflowStep::MuxTopN(sorts, _) => {
            for s in sorts {
                after.insert(s.by.clone());
            }
            after
        }

        WorkflowStep::Expand(expand) => {
            for f in &expand.fields {
                after.insert(f.clone());
            }
            after
        }

        WorkflowStep::Limit(_) | WorkflowStep::MuxLimit(_) => after,

        WorkflowStep::Join(join, _) => {
            after.insert(join.on.0.clone());
            after
        }

        WorkflowStep::Union(_) | WorkflowStep::Tee(_) | WorkflowStep::Scan(_) => after,
    }
}

#[cfg(test)]
mod tests {
    use hashbrown::HashSet;
    use miso_workflow::{Workflow, WorkflowStep as S};
    use miso_workflow_types::{
        expr::Expr,
        join::{Join, JoinType},
        summarize::Aggregation,
    };
    use test_case::test_case;

    use super::compute_required_before_step;
    use crate::test_utils::{by_field, field, project_field, sort_asc, summarize};

    fn required(step: &S, after: &[&str]) -> HashSet<String> {
        let after = after.iter().map(|s| field(s)).collect();
        compute_required_before_step(step, after)
            .iter()
            .map(|f| f.to_string())
            .collect()
    }

    #[test]
    fn count_requires_nothing() {
        assert!(required(&S::Count, &["x"]).is_empty());
    }

    #[test]
    fn summarize_requires_used_fields() {
        let step = summarize(
            "r",
            Aggregation::Sum(field("x")),
            vec![by_field(Expr::Field(field("y")), "y")],
        );
        assert_eq!(
            required(&step, &[]),
            HashSet::from(["x".into(), "y".into()])
        );
    }

    #[test]
    fn filter_unions_with_after() {
        let step = S::Filter(Expr::Field(field("a")));
        assert_eq!(
            required(&step, &["b"]),
            HashSet::from(["a".into(), "b".into()])
        );
    }

    #[test_case(&[("a", "b")], &["a"], &["b"]; "simple")]
    #[test_case(&[("c", "a+b")], &["c"], &["a", "b"]; "expr with multiple inputs")]
    #[test_case(&[("a", "b"), ("c", "d")], &["a"], &["b"]; "ignores unused outputs")]
    #[test_case(&[("a", "b"), ("b", "c")], &["a"], &["b"]; "overlapping names")]
    #[test_case(&[("a", "b"), ("b", "c")], &["a", "b"], &["b", "c"]; "both outputs needed")]
    fn extend(mappings: &[(&str, &str)], after: &[&str], expected: &[&str]) {
        let fields = mappings
            .iter()
            .map(|(to, from)| {
                let expr = if from.contains('+') {
                    let parts: Vec<_> = from.split('+').collect();
                    Expr::Plus(
                        Box::new(Expr::Field(field(parts[0]))),
                        Box::new(Expr::Field(field(parts[1]))),
                    )
                } else {
                    Expr::Field(field(from))
                };
                project_field(to, expr)
            })
            .collect();
        let expected: HashSet<String> = expected.iter().map(|s| (*s).into()).collect();
        assert_eq!(required(&S::Extend(fields), after), expected);
    }

    #[test_case(&[("a", "b")], &["b"], &["a"]; "simple")]
    #[test_case(&[("a", "b"), ("c", "d")], &["b"], &["a"]; "ignores unused")]
    #[test_case(&[("a", "b"), ("b", "a")], &["b"], &["a"]; "swap")]
    #[test_case(&[("a", "b"), ("b", "a")], &["a", "b"], &["a", "b"]; "swap both needed")]
    fn rename(mappings: &[(&str, &str)], after: &[&str], expected: &[&str]) {
        let renames = mappings.iter().map(|(f, t)| (field(f), field(t))).collect();
        let expected: HashSet<String> = expected.iter().map(|s| (*s).into()).collect();
        assert_eq!(required(&S::Rename(renames), after), expected);
    }

    #[test]
    fn sort_adds_sort_field() {
        let step = S::Sort(vec![sort_asc(field("ts"))]);
        assert_eq!(
            required(&step, &["x"]),
            HashSet::from(["ts".into(), "x".into()])
        );
    }

    #[test]
    fn join_adds_left_key() {
        let join = Join {
            on: (field("l"), field("r")),
            type_: JoinType::Inner,
            partitions: 1,
        };
        let step = S::Join(join, Workflow::new(vec![]));
        let fields = required(&step, &["x"]);
        assert!(fields.contains("l") && fields.contains("x") && !fields.contains("r"));
    }
}
