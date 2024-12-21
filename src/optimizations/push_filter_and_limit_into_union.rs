use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushFilterAndLimitIntoUnion;

/// Some steps after unions, when inserted as a step into the subquery, can allow for
/// predicate pushdowns to limit the amount of results.
/// Also insert these steps right before the union, for the same reasons, just for
/// the outer query before the union step.
impl Optimization for PushFilterAndLimitIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!((Union+) (Filter* [Limit TopN]))
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        assert_eq!(groups.len(), 2);

        let (unions_start, unions_end) = groups[0];
        let (push_start, push_end) = groups[1];

        let push_steps = steps[push_start..push_end].to_vec();

        let mut new_steps = Vec::with_capacity(push_steps.len() + steps.len());
        new_steps.extend(push_steps.clone());
        new_steps.extend(steps.to_vec());

        let union_range = push_steps.len() + unions_start..push_steps.len() + unions_end;
        for step in &mut new_steps[union_range] {
            let WorkflowStep::Union(ref mut workflow) = step else {
                continue;
            };
            workflow.steps.extend(push_steps.clone());
        }

        Some(new_steps)
    }
}
