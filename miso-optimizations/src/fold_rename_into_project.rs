use std::collections::BTreeMap;

use miso_workflow::WorkflowStep;
use miso_workflow_types::{field::Field, project::ProjectField, value::Value};

use crate::{
    Group, Optimization, OptimizationResult, Pattern, expr_substitude::ExprSubstitute, pattern,
};

pub struct FoldRenameIntoProject;

impl Optimization for FoldRenameIntoProject {
    fn pattern(&self) -> Pattern {
        pattern!(Rename Project)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let renames_vec = match &steps[0] {
            WorkflowStep::Rename(renames) => renames,
            _ => return OptimizationResult::Unchanged,
        };
        let project_fields = match &steps[1] {
            WorkflowStep::Project(fields) => fields,
            _ => return OptimizationResult::Unchanged,
        };

        let renames: BTreeMap<Field, Field> = renames_vec
            .iter()
            .map(|(from, to)| (to.clone(), from.clone()))
            .collect();
        let literals: BTreeMap<Field, Value> = BTreeMap::new();

        let expr_subst = ExprSubstitute::new(&renames, &literals);

        let merged_fields: Vec<ProjectField> = project_fields
            .iter()
            .map(|pf| ProjectField {
                from: expr_subst.substitute(pf.from.clone()),
                to: pf.to.clone(),
            })
            .collect();

        OptimizationResult::Changed(vec![WorkflowStep::Project(merged_fields)])
    }
}
