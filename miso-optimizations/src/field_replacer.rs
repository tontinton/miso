use hashbrown::HashMap;
use miso_workflow_types::{
    expand::Expand,
    expr::Expr,
    expr_visitor::ExprTransformer,
    field::Field,
    project::ProjectField,
    sort::Sort,
    summarize::{Aggregation, ByField, Summarize},
};

pub struct FieldReplacer<'a> {
    replacements: &'a HashMap<String, String>,
}

impl<'a> FieldReplacer<'a> {
    pub fn new(replacements: &'a HashMap<String, String>) -> Self {
        Self { replacements }
    }

    pub fn replace(&self, field: Field) -> Field {
        field.replace_top_level_access(self.replacements)
    }
}

impl<'a> ExprTransformer for FieldReplacer<'a> {
    fn transform_field(&self, field: Field) -> Expr {
        Expr::Field(self.replace(field))
    }

    fn transform_exists(&self, field: Field) -> Expr {
        Expr::Exists(self.replace(field))
    }
}

impl FieldReplacer<'_> {
    pub fn transform_project(&self, fields: Vec<ProjectField>) -> Vec<ProjectField> {
        fields
            .into_iter()
            .map(|pf| ProjectField {
                from: self.transform(pf.from),
                to: self.replace(pf.to),
            })
            .collect()
    }

    pub fn transform_rename(&self, fields: Vec<(Field, Field)>) -> Vec<(Field, Field)> {
        fields
            .into_iter()
            .map(|(from, to)| (self.replace(from), self.replace(to)))
            .collect()
    }

    pub fn transform_expand(&self, mut expand: Expand) -> Expand {
        expand.fields = expand.fields.into_iter().map(|f| self.replace(f)).collect();
        expand
    }

    pub fn transform_sort(&self, sorts: Vec<Sort>) -> Vec<Sort> {
        sorts
            .into_iter()
            .map(|sort| Sort {
                by: self.replace(sort.by),
                order: sort.order,
                nulls: sort.nulls,
            })
            .collect()
    }

    pub fn transform_summarize(&self, summarize: Summarize) -> Summarize {
        Summarize {
            aggs: summarize
                .aggs
                .into_iter()
                .map(|(out_f, agg)| {
                    (
                        self.replace(out_f),
                        match agg {
                            Aggregation::Count => Aggregation::Count,
                            Aggregation::Countif(e) => Aggregation::Countif(self.transform(e)),
                            Aggregation::Sum(f) => Aggregation::Sum(self.replace(f)),
                            Aggregation::Avg(f) => Aggregation::Avg(self.replace(f)),
                            Aggregation::Min(f) => Aggregation::Min(self.replace(f)),
                            Aggregation::Max(f) => Aggregation::Max(self.replace(f)),
                            Aggregation::DCount(f) => Aggregation::DCount(self.replace(f)),
                        },
                    )
                })
                .collect(),
            by: summarize
                .by
                .into_iter()
                .map(|bf| ByField {
                    name: bf.name,
                    expr: self.transform(bf.expr),
                })
                .collect(),
        }
    }
}
