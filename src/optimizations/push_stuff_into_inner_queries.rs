use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushStuffIntoInnerQueries;

/// Some steps after unions / joins, when inserted as a step into the subquery, can allow for
/// predicate pushdowns to limit the amount of results.
/// Also insert these steps right before the union / join, for the same reasons, just for
/// the outer query before the union / join step.
impl Optimization for PushStuffIntoInnerQueries {
    fn pattern(&self) -> Pattern {
        pattern!(([Union Join]+) (Filter* [Limit TopN]))
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        assert_eq!(groups.len(), 2);

        let (unions_start, unions_end) = groups[0];
        let (push_start, push_end) = groups[1];

        let push_steps = steps[push_start..push_end].to_vec();

        // Mustn't limit the outer query if there's a join step, it will interfere with the join.
        let join_exists = steps.iter().any(|x| matches!(x, WorkflowStep::Join(..)));

        let num_to_push = if join_exists {
            push_steps.len() - 1
        } else {
            push_steps.len()
        };

        let mut new_steps = Vec::with_capacity(num_to_push + steps.len());
        new_steps.extend(push_steps.clone().into_iter().take(num_to_push));
        new_steps.extend(steps.to_vec());

        let mut saw_join = false;
        for step in new_steps[num_to_push + unions_start..num_to_push + unions_end]
            .iter_mut()
            .rev()
        {
            match step {
                WorkflowStep::Join((_, ref mut workflow)) => {
                    saw_join = true;
                    workflow
                        .steps
                        .extend(push_steps.clone().into_iter().take(num_to_push));
                }
                WorkflowStep::Union(ref mut workflow) if saw_join => {
                    // Can't limit a subquery if a join is going to happen after.
                    workflow
                        .steps
                        .extend(push_steps.clone().into_iter().take(num_to_push));
                }
                WorkflowStep::Union(ref mut workflow) => {
                    workflow.steps.extend(push_steps.clone());
                }
                _ => {
                    panic!("Only join and union should have been captured")
                }
            }
        }

        Some(new_steps)
    }
}
