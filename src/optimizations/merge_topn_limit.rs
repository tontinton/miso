use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

/// This optimization actually obstructs the results, doing a limit and then top-n, will not return
/// the same results as only doing the top-n.
/// BUT because limit returns "random" results, it's fine to simply ignore it if there's a top-n
/// after, it just means the returned "random" results won't really be "random", which is still
/// "random".
pub struct MergeTopNLimit;

impl Optimization for MergeTopNLimit {
    fn pattern(&self) -> Pattern {
        pattern!(Limit TopN)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        let WorkflowStep::Limit(a) = &steps[0] else {
            return None;
        };
        let WorkflowStep::TopN(sorts, b) = &steps[1] else {
            return None;
        };
        Some(vec![WorkflowStep::TopN(
            sorts.clone(),
            std::cmp::min(*a, *b),
        )])
    }
}
