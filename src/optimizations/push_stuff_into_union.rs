use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushStuffIntoUnion;

/// Some steps after unions, when inserted as a step into the subquery, can allow for predicate
/// pushdowns to limit the amount of results.
/// Also insert these steps right before the union, for the same reasons, just for
/// the query before the union.
impl Optimization for PushStuffIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!((Union+) (Filter* [Limit TopN]))
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        assert_eq!(groups.len(), 2);

        let (unions_start, unions_end) = groups[0];
        let (push_start, push_end) = groups[1];

        let push_steps = steps[push_start..push_end].to_vec();
        let offset = push_steps.len();

        let mut new_steps = Vec::with_capacity(offset + steps.len());
        new_steps.extend(push_steps.clone());
        new_steps.extend(steps.to_vec());

        for step in &mut new_steps[offset + unions_start..offset + unions_end] {
            let WorkflowStep::Union(ref mut workflow) = step else {
                continue;
            };
            workflow.steps.extend(push_steps.clone());
        }

        Some(new_steps)
    }
}
