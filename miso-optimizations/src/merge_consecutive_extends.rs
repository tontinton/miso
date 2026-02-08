use std::collections::{BTreeMap, BTreeSet};

use miso_workflow::WorkflowStep;
use miso_workflow_types::{
    expr::Expr, expr_visitor::ExprTransformer, field::Field, project::ProjectField,
};

use crate::{Group, Optimization, OptimizationResult, Pattern, pattern};

pub struct MergeConsecutiveExtends;

struct SingleLevelSubstitute<'a> {
    map: &'a BTreeMap<Field, Expr>,
}

impl ExprTransformer for SingleLevelSubstitute<'_> {
    fn transform_field(&self, field: Field) -> Expr {
        if let Some(expr) = self.map.get(&field) {
            expr.clone()
        } else {
            Expr::Field(field)
        }
    }
}

impl Optimization for MergeConsecutiveExtends {
    fn pattern(&self) -> Pattern {
        pattern!(Extend Extend)
    }

    fn apply(&self, steps: &[WorkflowStep], _groups: &[Group]) -> OptimizationResult {
        let first_fields = match &steps[0] {
            WorkflowStep::Extend(fields) => fields,
            _ => return OptimizationResult::Unchanged,
        };
        let second_fields = match &steps[1] {
            WorkflowStep::Extend(fields) => fields,
            _ => return OptimizationResult::Unchanged,
        };

        let first_map: BTreeMap<Field, Expr> = first_fields
            .iter()
            .map(|pf| (pf.to.clone(), pf.from.clone()))
            .collect();

        let subst = SingleLevelSubstitute { map: &first_map };

        let second_targets: BTreeSet<Field> =
            second_fields.iter().map(|pf| pf.to.clone()).collect();

        let mut merged_fields: Vec<ProjectField> = first_fields
            .iter()
            .filter(|pf| !second_targets.contains(&pf.to))
            .cloned()
            .collect();

        merged_fields.extend(second_fields.iter().map(|pf| ProjectField {
            from: subst.transform(pf.from.clone()),
            to: pf.to.clone(),
        }));

        OptimizationResult::Changed(vec![WorkflowStep::Extend(merged_fields)])
    }
}
