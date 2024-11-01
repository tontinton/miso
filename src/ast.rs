use serde::{Deserialize, Serialize};

use crate::connector::Connector;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum FilterAst {
    Or(Vec<FilterAst>),
    And(Vec<FilterAst>),
    Term(/*field=*/ String, /*word=*/ String),
}

/// Tries to predicate pushdown the provided AST to the connector.
/// Returns the (leftovers - stuff that the connector can't predicate, predicated).
/// None means the expression was either kept as is (leftover) or taken fully (predicated).
pub fn filter_predicate_pushdown(
    ast: FilterAst,
    connector: &dyn Connector,
) -> (Option<FilterAst>, Option<FilterAst>) {
    match ast {
        FilterAst::And(exprs) => {
            let mut leftovers = Vec::new();
            let mut predicated = Vec::new();
            for expr in exprs {
                let (maybe_leftover, maybe_predicated) = filter_predicate_pushdown(expr, connector);
                if let Some(expr) = maybe_leftover {
                    leftovers.push(expr);
                }
                if let Some(expr) = maybe_predicated {
                    predicated.push(expr);
                }
            }

            (
                (!leftovers.is_empty()).then_some(FilterAst::And(leftovers)),
                (!predicated.is_empty()).then_some(FilterAst::And(predicated)),
            )
        }
        _ if connector.can_filter(&ast) => (None, Some(ast)),
        _ => (Some(ast), None),
    }
}

fn binop_to_vrl(exprs: &[FilterAst], join: &str) -> String {
    let mut result = String::new();
    if exprs.is_empty() {
        return result;
    }

    result.push('(');
    for (i, expr) in exprs.iter().enumerate() {
        result.push_str(&ast_to_vrl(expr));
        if i != exprs.len() - 1 {
            result.push_str(join);
        }
    }
    result.push(')');
    result
}

pub fn ast_to_vrl(ast: &FilterAst) -> String {
    match ast {
        FilterAst::And(exprs) => binop_to_vrl(exprs, " && "),
        FilterAst::Or(exprs) => binop_to_vrl(exprs, " || "),
        FilterAst::Term(field, word) => format!(".{field} == {word}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ast_to_vrl_sanity() -> std::io::Result<()> {
        let ast_raw = r#"{
            "and": [
                {
                    "and": [
                        { "term": ["a", "1"] },
                        {
                            "and": [
                                { "term": ["b", "2"] }
                            ]
                        }
                    ]
                },
                {
                    "or": [
                        { "term": ["c", "3"] },
                        { "term": ["d", "4"] }
                    ]
                }
            ]
        }"#;
        let result = ast_to_vrl(&serde_json::from_str(ast_raw)?);
        assert_eq!(result, "((.a == 1 && (.b == 2)) && (.c == 3 || .d == 4))");
        Ok(())
    }
}
