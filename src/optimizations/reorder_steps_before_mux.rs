use crate::{pattern, workflow::WorkflowStep};

use super::{Group, Optimization, Pattern};

/// When a mux is the last step in the query, we are able to send partial results to the user.
/// For example, a MuxCount can send the user the count it calculated for one of the split / union
/// scans, while still continuing to execute the query, to allow for better UX.
pub struct ReorderStepsBeforeMux;

impl Optimization for ReorderStepsBeforeMux {
    fn pattern(&self) -> Pattern {
        pattern!([MuxCount MuxLimit MuxTopN] [Filter Project Extend])
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        Some(vec![steps[1].clone(), steps[0].clone()])
    }
}
