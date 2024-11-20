use async_stream::try_stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

use color_eyre::{
    eyre::{bail, Context},
    Result,
};
use tracing::info;
use vrl::{compiler::Program, core::Value};

use crate::{
    log::{Log, LogStream, LogTryStream},
    workflow::vrl_utils::{compile_pretty_print_errors, run_vrl},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum FilterAst {
    Or(Vec<FilterAst>),                                // ||
    And(Vec<FilterAst>),                               // &&
    Contains(/*field=*/ String, /*word=*/ String),     // word in field
    StartsWith(/*field=*/ String, /*prefix=*/ String), // field starts with prefix
    Eq(/*field=*/ String, /*value=*/ String),          // ==
    Ne(/*field=*/ String, /*value=*/ String),          // !=
    Gt(/*field=*/ String, /*value=*/ String),          // >
    Gte(/*field=*/ String, /*value=*/ String),         // >=
    Lt(/*field=*/ String, /*value=*/ String),          // <
    Lte(/*field=*/ String, /*value=*/ String),         // <=
}

fn binop_to_vrl(exprs: &[FilterAst], join: &str) -> String {
    let mut result = String::new();
    if exprs.is_empty() {
        return result;
    }

    result.push('(');
    for (i, expr) in exprs.iter().enumerate() {
        result.push_str(&filter_ast_to_vrl(expr));
        if i != exprs.len() - 1 {
            result.push_str(join);
        }
    }
    result.push(')');
    result
}

fn filter_ast_to_vrl(ast: &FilterAst) -> String {
    match ast {
        FilterAst::And(exprs) => binop_to_vrl(exprs, " && "),
        FilterAst::Or(exprs) => binop_to_vrl(exprs, " || "),
        FilterAst::Contains(field, word) => format!("contains(string!(.{field}), \"{word}\")"),
        FilterAst::StartsWith(field, prefix) => {
            format!("starts_with(string!(.{field}), \"{prefix}\")")
        }
        FilterAst::Eq(field, word) => format!(".{field} == {word}"),
        FilterAst::Ne(field, word) => format!(".{field} != {word}"),
        FilterAst::Gt(field, word) => format!(".{field} > {word}"),
        FilterAst::Gte(field, word) => format!(".{field} >= {word}"),
        FilterAst::Lt(field, word) => format!(".{field} < {word}"),
        FilterAst::Lte(field, word) => format!(".{field} <= {word}"),
    }
}

fn run_vrl_filter(program: &Program, log: Log) -> Result<bool> {
    let Value::Boolean(allowed) = run_vrl(program, log)? else {
        bail!("Response of VRL script not boolean");
    };
    Ok(allowed)
}

pub fn filter_stream(ast: &FilterAst, mut input_stream: LogStream) -> Result<LogTryStream> {
    let script = filter_ast_to_vrl(ast);
    info!("Filtering: `{script}`");

    let program = compile_pretty_print_errors(&script).context("compile filter vrl")?;

    Ok(Box::pin(try_stream! {
        while let Some(log) = input_stream.next().await {
            if run_vrl_filter(&program, log.clone()).context("filter vrl")? {
                yield log;
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_ast_to_vrl_sanity() -> std::io::Result<()> {
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
        let result = filter_ast_to_vrl(&serde_json::from_str(ast_raw)?);
        assert_eq!(result, "((.a == 1 && (.b == 2)) && (.c == 3 || .d == 4))");
        Ok(())
    }
}
