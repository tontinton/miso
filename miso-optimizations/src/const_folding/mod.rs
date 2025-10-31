pub mod partial_evaluator;

use miso_workflow::WorkflowStep;
use miso_workflow_types::project::ProjectField;

use crate::{const_folding::partial_evaluator::partial_eval, pattern};

use super::{Group, Optimization, Pattern};

/// Turns 'where x > 50 * 10 - 2' into 'where x > 498'.
pub struct ConstFolding;

impl Optimization for ConstFolding {
    fn pattern(&self) -> Pattern {
        pattern!([Filter Project Extend]+)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        Some(
            steps
                .iter()
                .map(|step| match step {
                    WorkflowStep::Filter(expr) => {
                        WorkflowStep::Filter(partial_eval(expr).unwrap_or_else(|_| expr.clone()))
                    }
                    WorkflowStep::Project(fields) => {
                        WorkflowStep::Project(rewrite_project_fields(fields))
                    }
                    WorkflowStep::Extend(fields) => {
                        WorkflowStep::Extend(rewrite_project_fields(fields))
                    }
                    _ => unreachable!("not in const folding pattern"),
                })
                .collect(),
        )
    }
}

fn rewrite_project_fields(fields: &[ProjectField]) -> Vec<ProjectField> {
    fields
        .iter()
        .cloned()
        .map(|mut pf| {
            if let Ok(expr) = partial_eval(&pf.from) {
                pf.from = expr;
            }
            pf
        })
        .collect()
}
