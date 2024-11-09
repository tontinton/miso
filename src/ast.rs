use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum FilterAst {
    Or(Vec<FilterAst>),
    And(Vec<FilterAst>),
    Contains(/*field=*/ String, /*word=*/ String),
    Eq(/*field=*/ String, /*value=*/ String),
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
        FilterAst::Eq(field, word) => format!(".{field} == {word}"),
        FilterAst::Contains(field, word) => format!("contains(string!(.{field}), \"{word}\")"),
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
                        { "eq": ["a", "1"] },
                        {
                            "and": [
                                { "eq": ["b", "2"] }
                            ]
                        }
                    ]
                },
                {
                    "or": [
                        { "eq": ["c", "3"] },
                        { "eq": ["d", "4"] }
                    ]
                }
            ]
        }"#;
        let result = ast_to_vrl(&serde_json::from_str(ast_raw)?);
        assert_eq!(result, "((.a == 1 && (.b == 2)) && (.c == 3 || .d == 4))");
        Ok(())
    }
}
