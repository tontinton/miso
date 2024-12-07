use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

pub struct PushLimitOrTopNIntoUnion;

/// When there's a limit / top-n after unions, insert that limit / top-n as the last
/// step of the unions, it can allow for predicate pushdowns to limit the amount of
/// results that need to be returned.
/// Also insert that limit step to right before the union, for the same reasons, just for
/// the outer query.
impl Optimization for PushLimitOrTopNIntoUnion {
    fn pattern(&self) -> Pattern {
        pattern!(Union [^Limit TopN]*? [Limit TopN])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        assert!(steps.len() >= 2);

        let push_step = steps[steps.len() - 1].clone();

        let mut new_steps = Vec::with_capacity(steps.len() + 1);
        new_steps.push(push_step.clone());
        new_steps.extend(steps.to_vec());

        for step in &mut new_steps {
            let WorkflowStep::Union(ref mut workflow) = step else {
                continue;
            };
            workflow.steps.push(push_step.clone());
        }

        Some(new_steps)
    }
}
