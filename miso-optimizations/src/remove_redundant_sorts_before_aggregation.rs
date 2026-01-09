use miso_workflow::WorkflowStep;

use crate::pattern;

use super::{Group, Optimization, Pattern};

pub struct RemoveRedundantSortsBeforeAggregation;

impl Optimization for RemoveRedundantSortsBeforeAggregation {
    fn pattern(&self) -> Pattern {
        // Sort -> Limit modifies count, so we don't want to match on limit between sort and aggregation.
        pattern!(Sort ([^Limit]*?) [Count Summarize MuxSummarize])
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        assert_eq!(groups.len(), 1);

        let (start, end) = groups[0];

        let mut modified_steps = steps[start..end]
            .iter()
            .filter(|step| !matches!(step, WorkflowStep::Sort(..)))
            .cloned()
            .collect::<Vec<_>>();
        modified_steps.push(steps.last().unwrap().clone());

        Some(modified_steps)
    }
}
