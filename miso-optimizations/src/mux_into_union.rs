use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

/// Some steps after unions, when inserted as a step into the union subquery, can allow for
/// predicate pushdowns.
/// Also insert these steps right before the union, for the same reasons, just for
/// the outer query before the union step.
pub struct MuxIntoUnion;

impl Optimization for MuxIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!(Union+ [Limit TopN Count Summarize])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let orig_step = &steps[steps.len() - 1];

        let (partial_step, mux_step) = match orig_step {
            WorkflowStep::Limit(limit) => {
                (WorkflowStep::Limit(*limit), WorkflowStep::MuxLimit(*limit))
            }
            WorkflowStep::TopN(sort, limit) => (
                WorkflowStep::TopN(sort.clone(), *limit),
                WorkflowStep::MuxTopN(sort.clone(), *limit),
            ),
            WorkflowStep::Count => (WorkflowStep::Count, WorkflowStep::MuxCount),
            WorkflowStep::Summarize(summarize) => (
                WorkflowStep::Summarize(summarize.clone().convert_to_partial()),
                WorkflowStep::MuxSummarize(summarize.clone().convert_to_mux()),
            ),

            _ => return OptimizationResult::Unchanged,
        };

        let mut new_steps = Vec::with_capacity(1 + steps.len());
        new_steps.push(partial_step.clone());
        new_steps.extend(steps[..steps.len() - 1].to_vec());
        new_steps.push(mux_step);

        for step in &mut new_steps[1..steps.len()] {
            if let WorkflowStep::Union(workflow) = step {
                workflow.steps.push(partial_step.clone());
            }
        }

        OptimizationResult::Changed(new_steps)
    }
}
