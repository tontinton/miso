use serde::Deserialize;

use crate::connector::Connector;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterItem {
    /// Filter items that contain the input word (2nd arg) in the input field (1st arg).
    Term(/*field=*/ String, /*word=*/ String),
    Filter(FilterTree),
}

#[derive(Debug, Deserialize)]
pub struct FilterTree {
    /// All filters must apply. Similar to ES must.
    #[serde(default)]
    and: Vec<FilterItem>,

    /// One of the filters must apply. Similar to ES should.
    #[serde(default)]
    or: Vec<FilterItem>,
}

#[derive(Debug, Deserialize)]
pub struct QueryAst {
    /// Similar to ES bool.
    filter: Option<FilterTree>,
}

pub fn predicate_pushdown(ast: &mut QueryAst, connector: &dyn Connector) {
    let Some(filter) = &mut ast.filter else {
        return;
    };

    filter.and.retain(|item| !connector.apply_filter_and(item));
    if !filter.or.is_empty() {
        if connector.apply_filter_or(&filter.or) {
            filter.or.clear();
        }
    }
}

fn filter_item_to_vrl(item: &FilterItem) -> String {
    match item {
        FilterItem::Term(field, word) => format!(".{field}={word}"),
        FilterItem::Filter(filter) => filter_tree_to_vrl(filter),
    }
}

fn filter_tree_to_vrl(filter: &FilterTree) -> String {
    let mut script = String::new();

    let wrap_parentheses = !filter.and.is_empty() && !filter.or.is_empty();
    if wrap_parentheses {
        script.push_str("(");
    }

    if !filter.and.is_empty() {
        script.push_str("(");
        for (i, item) in filter.and.iter().enumerate() {
            script.push_str(&filter_item_to_vrl(item));
            if i != filter.and.len() - 1 {
                script.push_str(" && ");
            }
        }
        script.push_str(")");
    }

    if !filter.or.is_empty() {
        if !script.is_empty() {
            script.push_str(" && ");
        }

        script.push_str("(");
        for (i, item) in filter.or.iter().enumerate() {
            script.push_str(&filter_item_to_vrl(item));
            if i != filter.or.len() - 1 {
                script.push_str(" || ");
            }
        }
        script.push_str(")");
    }

    if wrap_parentheses {
        script.push_str(")");
    }

    script
}

pub fn ast_to_vrl(ast: &QueryAst) -> String {
    let mut script = String::new();
    if let Some(filter) = &ast.filter {
        script.push_str(&filter_tree_to_vrl(filter));
    }
    script
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ast_to_vrl_sanity() -> std::io::Result<()> {
        let ast_raw = r#"{
            "filter": {
                "and": [
                    { "term": ["a", "1"] },
                    {
                        "filter": {
                            "and": [
                                { "term": ["b", "2"] }
                            ]
                        }
                    }
                ],
                "or": [
                    { "term": ["c", "3"] },
                    { "term": ["d", "4"] }
                ]
            }
        }"#;
        let result = ast_to_vrl(&serde_json::from_str(ast_raw)?);
        assert_eq!(result, "((.a=1 && (.b=2)) && (.c=3 || .d=4))");
        Ok(())
    }
}
