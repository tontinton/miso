use std::collections::BTreeMap;

use miso_workflow::WorkflowStep;
use miso_workflow_types::{expr::Expr, field::Field, project::ProjectField, value::Value};

use crate::{
    Group, Optimization, OptimizationResult, Pattern, expr_substitude::ExprSubstitute, pattern,
    project_propagation::categorize_fields,
};

pub struct MergeConsecutiveProjects;

impl Optimization for MergeConsecutiveProjects {
    fn pattern(&self) -> Pattern {
        pattern!(Project Project)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let first_fields = match &steps[0] {
            WorkflowStep::Project(fields) => fields,
            _ => return OptimizationResult::Unchanged,
        };
        let second_fields = match &steps[1] {
            WorkflowStep::Project(fields) => fields,
            _ => return OptimizationResult::Unchanged,
        };

        let mut renames: BTreeMap<Field, Field> = BTreeMap::new();
        let mut literals: BTreeMap<Field, Value> = BTreeMap::new();
        let mut exprs: BTreeMap<Field, Expr> = BTreeMap::new();

        categorize_fields(first_fields, &mut renames, &mut literals, &mut exprs);

        let expr_subst = ExprSubstitute::with_exprs(&renames, &literals, &exprs);

        let merged_fields: Vec<ProjectField> = second_fields
            .iter()
            .map(|pf| ProjectField {
                from: expr_subst.substitute(pf.from.clone()),
                to: pf.to.clone(),
            })
            .collect();

        OptimizationResult::Changed(vec![WorkflowStep::Project(merged_fields)])
    }
}
