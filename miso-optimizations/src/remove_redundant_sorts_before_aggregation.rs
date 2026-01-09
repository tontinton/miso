use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, OptimizationResult, Pattern};

pub struct RemoveRedundantSortsBeforeAggregation;

impl Optimization for RemoveRedundantSortsBeforeAggregation {
    fn pattern(&self) -> Pattern {
        pattern!(Sort ([^Limit]*?) [Count Summarize MuxSummarize])
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> OptimizationResult {
        assert_eq!(groups.len(), 1);

        let (start, end) = groups[0];

        let mut modified_steps = steps[start..end]
            .iter()
            .filter(|step| !matches!(step, WorkflowStep::Sort(..)))
            .cloned()
            .collect::<Vec<_>>();
        modified_steps.push(steps.last().unwrap().clone());

        OptimizationResult::Changed(modified_steps)
    }
}
