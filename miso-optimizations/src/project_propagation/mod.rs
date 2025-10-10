mod expr_substitude;

use hashbrown::{HashMap, HashSet};
use miso_workflow::WorkflowStep;
use miso_workflow_types::{
    expr::Expr,
    expr_visitor::ExprTransformer,
    field::Field,
    project::ProjectField,
    sort::Sort,
    summarize::{Aggregation, Summarize},
    value::Value,
};

use crate::{
    Group, Optimization, Pattern, pattern, project_propagation::expr_substitude::ExprSubstitute,
};

/// Propagate renames and literal assignments into later steps, to move the project step
/// later if possible (and make it run on less logs).
///
/// Example with filter:
///   project a = b, c = 50 | where a == c
///  ->
///   where b == 50 | project a = b, c = 50
pub struct ProjectPropagationWithoutEnd;

/// Example with summarize:
///   project a = b, c = 50 | summarize d = sum(a) by c
///  ->
///   summarize d = sum(b) | extend c = 50
pub struct ProjectPropagationWithEnd;

impl Optimization for ProjectPropagationWithoutEnd {
    fn pattern(&self) -> Pattern {
        pattern!([Project Extend Rename] ([Filter Sort TopN Limit Extend Rename Expand]+))
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        apply(steps, groups[0], false)
    }
}

impl Optimization for ProjectPropagationWithEnd {
    fn pattern(&self) -> Pattern {
        pattern!([Project Extend Rename] ([Filter Sort TopN Limit Extend Rename Expand]*?) [Project Summarize MuxSummarize])
    }

    fn apply(&self, steps: &[WorkflowStep], groups: &[Group]) -> Option<Vec<WorkflowStep>> {
        apply(steps, groups[0], true)
    }
}

enum StepType {
    Project,
    Extend,
    Rename,
}

fn apply(
    steps: &[WorkflowStep],
    middle_group: Group,
    with_end_step: bool,
) -> Option<Vec<WorkflowStep>> {
    if steps.len() < 2 {
        return None;
    }

    if matches!(
        &steps[1],
        WorkflowStep::Project(..) | WorkflowStep::Extend(..) | WorkflowStep::Rename(..)
    ) {
        // Switching between project, extend, and rename will cause infinite loop, as it will keep getting into
        // this optimization and keep switching positions between them.
        return None;
    }

    let mut renames: HashMap<Field, Field> = HashMap::new(); // a = b
    let mut literals: HashMap<Field, Value> = HashMap::new(); // c = 50
    let mut unhandled_projects = Vec::new();

    let (project_fields, step_type) = match &steps[0] {
        WorkflowStep::Project(fields) => (fields, StepType::Project),
        WorkflowStep::Extend(fields) => (fields, StepType::Extend),
        WorkflowStep::Rename(renames_to_track) => {
            for (from, to) in renames_to_track {
                renames.insert(to.clone(), from.clone());
            }
            (&vec![], StepType::Rename)
        }
        _ => return None,
    };

    for pf in project_fields.iter().cloned() {
        match pf.from {
            Expr::Field(src) => {
                renames.insert(pf.to, src);
            }
            Expr::Literal(val) => {
                literals.insert(pf.to, val);
            }
            from => {
                unhandled_projects.push(ProjectField { from, to: pf.to });
            }
        }
    }

    if renames.is_empty() && literals.is_empty() {
        return None;
    }

    let mut out = Vec::new();
    if !unhandled_projects.is_empty() {
        let step = match step_type {
            StepType::Project => WorkflowStep::Project(unhandled_projects),
            StepType::Extend => WorkflowStep::Extend(unhandled_projects),
            StepType::Rename => unreachable!("rename cannot include unhandled projects"),
        };
        out.push(step);
    }

    {
        let expr_subst = ExprSubstitute::new(&renames, &literals);

        let (middle_start, middle_end) = middle_group;
        for step in steps[middle_start..middle_end].iter().cloned() {
            let new = match step {
                WorkflowStep::Filter(e) => WorkflowStep::Filter(expr_subst.substitute(e)),

                WorkflowStep::Sort(sorts) => {
                    let new_sorts = rewrite_sorts(sorts, &renames, &literals);
                    if new_sorts.is_empty() {
                        continue;
                    }
                    WorkflowStep::Sort(new_sorts)
                }

                WorkflowStep::TopN(sorts, n) => {
                    let new_sorts = rewrite_sorts(sorts, &renames, &literals);
                    if new_sorts.is_empty() {
                        continue;
                    }
                    WorkflowStep::TopN(new_sorts, n)
                }

                WorkflowStep::Limit(n) => WorkflowStep::Limit(n),

                WorkflowStep::Extend(fields) => {
                    let new_fields = rewrite_project_fields(fields, &renames, &literals);
                    if new_fields.is_empty() {
                        continue;
                    }
                    WorkflowStep::Extend(new_fields)
                }

                WorkflowStep::Expand(mut expand) => {
                    expand.fields = rewrite_expand(expand.fields, &renames, &literals);
                    if expand.fields.is_empty() {
                        continue;
                    }
                    WorkflowStep::Expand(expand)
                }

                _ => unreachable!("not in middle pattern"),
            };

            out.push(new);
        }
    }

    if !with_end_step {
        let step = match step_type {
            StepType::Project => {
                WorkflowStep::Project(renames_and_literals_to_project_fields(renames, literals))
            }
            StepType::Extend => {
                WorkflowStep::Extend(renames_and_literals_to_project_fields(renames, literals))
            }
            StepType::Rename => {
                WorkflowStep::Rename(renames.into_iter().map(|(to, from)| (from, to)).collect())
            }
        };
        out.push(step);
        return Some(out);
    }

    let tail = steps.last().unwrap();
    match tail {
        WorkflowStep::Summarize(sum) | WorkflowStep::MuxSummarize(sum) => {
            let (new_sum, post_project_fields) =
                rewrite_summarize(sum.clone(), &renames, &literals)?;

            if !new_sum.is_empty() {
                let step = match tail {
                    WorkflowStep::MuxSummarize(_) => WorkflowStep::MuxSummarize(new_sum),
                    WorkflowStep::Summarize(_) => WorkflowStep::Summarize(new_sum),
                    _ => unreachable!(),
                };
                out.push(step);
            }

            if !post_project_fields.is_empty() {
                out.push(WorkflowStep::Project(post_project_fields));
            }
        }
        WorkflowStep::Project(fields) => {
            let new_fields = rewrite_project_fields(fields.to_vec(), &renames, &literals);
            if !new_fields.is_empty() {
                out.push(WorkflowStep::Project(new_fields));
            }
        }
        _ => unreachable!("not in end pattern"),
    }

    Some(out)
}

fn rewrite_sorts(
    mut sorts: Vec<Sort>,
    renames: &HashMap<Field, Field>,
    literals: &HashMap<Field, Value>,
) -> Vec<Sort> {
    sorts.retain(|sort| !literals.contains_key(&sort.by));
    for sort in &mut sorts {
        if let Some(new_by) = renames.get(&sort.by) {
            sort.by = new_by.clone();
        }
    }
    sorts
}

fn rewrite_project_fields(
    fields: Vec<ProjectField>,
    renames: &HashMap<Field, Field>,
    literals: &HashMap<Field, Value>,
) -> Vec<ProjectField> {
    let expr_subst = ExprSubstitute::new(renames, literals);
    fields
        .into_iter()
        .map(|pf| ProjectField {
            from: expr_subst.substitute(pf.from),
            to: pf.to,
        })
        .collect()
}

fn rewrite_expand(
    fields: Vec<Field>,
    renames: &HashMap<Field, Field>,
    literals: &HashMap<Field, Value>,
) -> Vec<Field> {
    let expr_subst = ExprSubstitute::new(renames, literals);
    fields
        .into_iter()
        .filter_map(|f| {
            if let Expr::Field(field) = expr_subst.transform_field(f) {
                Some(field)
            } else {
                None
            }
        })
        .collect()
}

fn rewrite_summarize(
    sum: Summarize,
    renames: &HashMap<Field, Field>,
    literals: &HashMap<Field, Value>,
) -> Option<(Summarize, Vec<ProjectField>)> {
    let mut project_fields = Vec::new();
    let mut summarize_output_fields = HashSet::new();

    let new_by = {
        let expr_subst = ExprSubstitute::new(renames, literals)
            .with_literal_hook(|f, v| {
                project_fields.push(ProjectField {
                    from: Expr::Literal(v.clone()),
                    to: f,
                });
            })
            .with_rename_hook(|_, from| {
                summarize_output_fields.insert(from.clone());
            });

        sum.by
            .into_iter()
            .map(|e| expr_subst.substitute(e))
            .collect::<Vec<_>>()
    };

    for e in &new_by {
        summarize_output_fields.extend(e.fields());
    }

    let expr_subst = ExprSubstitute::new(renames, literals);
    let mut new_aggs = HashMap::new();

    for (k, agg) in sum.aggs {
        summarize_output_fields.insert(k.clone());

        match &agg {
            Aggregation::Count => {
                new_aggs.insert(k, agg);
            }
            Aggregation::Countif(e) => {
                new_aggs.insert(k, Aggregation::Countif(expr_subst.substitute(e.clone())));
            }
            Aggregation::DCount(f) => {
                if literals.contains_key(f) {
                    // dcount(literal) is always 1.
                    project_fields.push(ProjectField {
                        from: Expr::Literal(Value::Int(1)),
                        to: k,
                    });
                } else if let Some(new_f) = renames.get(f) {
                    new_aggs.insert(k, Aggregation::DCount(new_f.clone()));
                } else {
                    new_aggs.insert(k, agg);
                }
            }
            Aggregation::Sum(f) => {
                if let Some(lit) = literals.get(f) {
                    new_aggs.insert(k.clone(), Aggregation::Count);
                    project_fields.push(ProjectField {
                        from: Expr::Mul(
                            Box::new(Expr::Field(k.clone())),
                            Box::new(Expr::Literal(lit.clone())),
                        ),
                        to: k.clone(),
                    });
                } else if let Some(new_f) = renames.get(f) {
                    new_aggs.insert(k, Aggregation::Sum(new_f.clone()));
                } else {
                    new_aggs.insert(k, agg);
                }
            }
            Aggregation::Avg(f) => {
                if let Some(lit) = literals.get(f) {
                    // avg(literal) is always the literal value itself.
                    project_fields.push(ProjectField {
                        from: Expr::Literal(lit.clone()),
                        to: k,
                    });
                } else if let Some(new_f) = renames.get(f) {
                    new_aggs.insert(k, Aggregation::Avg(new_f.clone()));
                } else {
                    new_aggs.insert(k, agg);
                }
            }
            Aggregation::Min(f) | Aggregation::Max(f) => {
                if let Some(lit) = literals.get(f) {
                    project_fields.push(ProjectField {
                        from: Expr::Literal(lit.clone()),
                        to: k,
                    });
                } else if let Some(new_f) = renames.get(f) {
                    let new_agg = match agg {
                        Aggregation::Min(_) => Aggregation::Min(new_f.clone()),
                        Aggregation::Max(_) => Aggregation::Max(new_f.clone()),
                        _ => unreachable!(),
                    };
                    new_aggs.insert(k, new_agg);
                } else {
                    new_aggs.insert(k, agg);
                }
            }
        }
    }

    let summarize_step = Summarize {
        aggs: new_aggs,
        by: new_by,
    };

    summarize_output_fields.retain(|f| !project_fields.iter().any(|pf| f == &pf.to));

    for f in summarize_output_fields {
        let to = renames
            .iter()
            .find_map(|(k, v)| (v == &f).then_some(k))
            .unwrap_or(&f)
            .clone();

        project_fields.push(ProjectField {
            from: Expr::Field(f),
            to,
        });
    }

    if project_fields
        .iter()
        .all(|pf| matches!(&pf.from, Expr::Field(f) if f == &pf.to))
    {
        project_fields.clear();
    }

    Some((summarize_step, project_fields))
}

fn renames_and_literals_to_project_fields(
    renames: HashMap<Field, Field>,
    literals: HashMap<Field, Value>,
) -> Vec<ProjectField> {
    let mut project_fields = Vec::with_capacity(renames.len() + literals.len());
    for (to, from) in renames {
        project_fields.push(ProjectField {
            to,
            from: Expr::Field(from),
        });
    }
    for (to, value) in literals {
        project_fields.push(ProjectField {
            to,
            from: Expr::Literal(value),
        });
    }
    project_fields
}
