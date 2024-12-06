use async_stream::try_stream;
use color_eyre::eyre::{bail, Context, Result};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::info;
use vrl::{compiler::Program, core::Value};

use crate::{
    log::{Log, LogStream, LogTryStream},
    workflow::vrl_utils::compile_pretty_print_errors,
};

use super::vrl_utils::run_vrl;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum TransformAst {
    Value(String),
    Field(String),

    #[serde(rename = "*")]
    Mul(Box<TransformAst>, Box<TransformAst>),
    #[serde(rename = "/")]
    Div(Box<TransformAst>, Box<TransformAst>),
    #[serde(rename = "+")]
    Plus(Box<TransformAst>, Box<TransformAst>),
    #[serde(rename = "-")]
    Minus(Box<TransformAst>, Box<TransformAst>),
}

// table | project x=y*2 | filter x>50

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProjectField {
    from: TransformAst,
    to: String,
}

fn transform_binop_ast_to_vrl(left: &TransformAst, right: &TransformAst, op: &str) -> String {
    format!(
        "({} {} {})",
        transform_ast_to_vrl(left, true),
        op,
        transform_ast_to_vrl(right, true)
    )
}

fn transform_ast_to_vrl(ast: &TransformAst, binop: bool) -> String {
    match ast {
        TransformAst::Value(value) => value.to_string(),
        TransformAst::Field(name) if binop => format!("to_int!(.{name})"),
        TransformAst::Field(name) => format!(".{name}"),
        TransformAst::Mul(left, right) => transform_binop_ast_to_vrl(left, right, "*"),
        TransformAst::Div(left, right) => transform_binop_ast_to_vrl(left, right, "/"),
        TransformAst::Plus(left, right) => transform_binop_ast_to_vrl(left, right, "+"),
        TransformAst::Minus(left, right) => transform_binop_ast_to_vrl(left, right, "-"),
    }
}

fn project_fields_to_vrl(fields: &[ProjectField]) -> String {
    let mut items = Vec::with_capacity(fields.len());
    for field in fields {
        items.push(format!(
            "\"{}\": {}",
            field.to,
            transform_ast_to_vrl(&field.from, false)
        ));
    }
    format!(". = {{{}}}", items.join(", "))
}

fn run_vrl_project(program: &Program, log: Log) -> Result<Log> {
    let Value::Object(map) = run_vrl(program, log)? else {
        bail!("response of VRL script not object");
    };
    Ok(map)
}

pub fn project_stream(
    fields: &[ProjectField],
    mut input_stream: LogStream,
) -> Result<LogTryStream> {
    let script = project_fields_to_vrl(fields);
    let program = compile_pretty_print_errors(&script).context("compile project vrl")?;

    info!("Projecting: `{script}`");

    Ok(Box::pin(try_stream! {
        while let Some(log) = input_stream.next().await {
            let log = run_vrl_project(&program, log).context("project vrl")?;
            yield log;
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_fields_to_vrl_sanity() -> std::io::Result<()> {
        let project_fields_raw = r#"[
            {
                "from": {
                    "*": [{"field": "name"}, {"value": "100"}]
                },
                "to": "test1"
            },
            {
                "from": {
                    "+": [{"field": "left"}, {"field": "right"}]
                },
                "to": "test2"
            }
        ]"#;
        let project_fields: Vec<ProjectField> = serde_json::from_str(project_fields_raw)?;
        let result = project_fields_to_vrl(&project_fields);
        assert_eq!(
            result,
            r#". = {"test1": (to_int!(.name) * 100), "test2": (to_int!(.left) + to_int!(.right))}"#
        );
        Ok(())
    }
}
