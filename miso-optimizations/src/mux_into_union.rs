//! Pushes aggregation steps into union branches to reduce data before merging.
//!
//! When a `limit 10` sits after a union, each branch might return thousands of
//! rows - but we only need 10 total. By pushing `limit 10` into each branch,
//! we allow for predicate pushdowns.
//!
//! Example:
//!   scan | union (scan) | limit 10
//! becomes:
//!   limit 10 | scan | limit 10 | union (scan | limit 10) | mux_limit 10
//!
//! Same idea for `topN`, `count`, and `summarize`. For aggregations, we use
//! partial/mux variants: each branch computes partial results, then a final
//! "mux" step merges them. For example, `summarize count()` becomes
//! `summarize count()` in each branch, then `mux_summarize sum()` to combine.
//!
//! The step also gets added *before* the first union (for the outer pipeline),
//! so all branches including the implicit outer one benefit.

use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

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
