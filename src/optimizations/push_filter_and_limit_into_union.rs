use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

/// Some steps after unions, when inserted as a step into the subquery, can allow for
/// predicate pushdowns to limit the amount of results.
/// Also insert these steps right before the union, for the same reasons, just for
/// the outer query before the union step.
pub struct PushFilterAndLimitIntoUnion;

impl Optimization for PushFilterAndLimitIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!((Union+) (Filter*? [Limit TopN]))
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        assert_eq!(groups.len(), 2);

        let (unions_start, unions_end) = groups[0];
        let (push_start, push_end) = groups[1];

        let num_unions = unions_end - unions_start;

        let union_steps = steps[unions_start..unions_end].to_vec();
        let push_steps = steps[push_start..push_end].to_vec();
        let limit_step = steps[steps.len() - 1].clone();

        let mut new_steps = Vec::with_capacity(push_steps.len() + num_unions + 1);
        new_steps.extend(push_steps.clone());
        new_steps.extend(union_steps);
        new_steps.push(limit_step);

        for step in &mut new_steps[push_steps.len()..push_steps.len() + num_unions] {
            if let WorkflowStep::Union(ref mut workflow) = step {
                workflow.steps.extend(push_steps.clone());
            }
        }

        Some(new_steps)
    }
}
