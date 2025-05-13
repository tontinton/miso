use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

/// Allow union subquery to predicate pushdown the summarize step.
///
/// From:
///   scan x
///   | union (scan y)
///   | summarize by f, a=min(z), b=max(z), c=count()
///
/// To:
///   scan x
///   | summarize by f, a=min(z), b=max(z), c=count()
///   | union (
///       scan y
///       | summarize by f, a=min(z), b=max(z), c=count()
///     )
///   | summarize by f, a=min(a), b=max(b), c=sum(c)
pub struct PushSummarizeIntoUnion;

impl Optimization for PushSummarizeIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!(Union + Summarize)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let summarize_step = &steps[steps.len() - 1];
        let WorkflowStep::Summarize(summarize) = summarize_step.clone() else {
            return None;
        };
        if summarize.is_final() {
            return None;
        }

        let post_summarize_step = WorkflowStep::Summarize(summarize.convert_to_final());

        let mut new_steps = Vec::with_capacity(1 + steps.len());
        new_steps.push(summarize_step.clone());
        new_steps.extend(steps[..steps.len() - 1].to_vec());
        new_steps.push(post_summarize_step);

        for step in &mut new_steps[1..steps.len()] {
            if let WorkflowStep::Union(ref mut workflow) = step {
                workflow.steps.push(summarize_step.clone());
            }
        }

        Some(new_steps)
    }
}
