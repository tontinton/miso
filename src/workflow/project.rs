use async_stream::try_stream;
use color_eyre::eyre::Result;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::log::{Log, LogStream, LogTryStream};

use super::interpreter::{ident, serde_json_to_val, CastType, Val};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectAst {
    Id(String),
    Lit(serde_json::Value),
    Cast(CastType, Box<ProjectAst>),
    #[serde(rename = "*")]
    Mul(Box<ProjectAst>, Box<ProjectAst>),
    #[serde(rename = "/")]
    Div(Box<ProjectAst>, Box<ProjectAst>),
    #[serde(rename = "+")]
    Plus(Box<ProjectAst>, Box<ProjectAst>),
    #[serde(rename = "-")]
    Minus(Box<ProjectAst>, Box<ProjectAst>),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ProjectField {
    from: ProjectAst,
    to: String,
}

struct ProjectInterpreter<'a> {
    log: &'a Log,
}

impl<'a> ProjectInterpreter<'a> {
    fn new(log: &'a Log) -> Self {
        Self { log }
    }
}

impl ProjectInterpreter<'_> {
    fn eval(&self, ast: &ProjectAst) -> Result<Val> {
        Ok(match ast {
            ProjectAst::Id(name) => ident(self.log, name)?,
            ProjectAst::Lit(value) => serde_json_to_val(value)?,
            ProjectAst::Cast(ty, expr) => self.eval(expr)?.cast(*ty)?,
            ProjectAst::Mul(l, r) => self.eval(l)?.mul(&self.eval(r)?)?,
            ProjectAst::Div(l, r) => self.eval(l)?.div(&self.eval(r)?)?,
            ProjectAst::Plus(l, r) => self.eval(l)?.add(&self.eval(r)?)?,
            ProjectAst::Minus(l, r) => self.eval(l)?.sub(&self.eval(r)?)?,
        })
    }
}

pub async fn project_stream(
    project_fields: Vec<ProjectField>,
    mut input_stream: LogStream,
) -> Result<LogTryStream> {
    Ok(Box::pin(try_stream! {
        'logs_loop: while let Some(log) = input_stream.next().await {
            let interpreter = ProjectInterpreter::new(&log);

            let mut output = Log::new();

            for field in &project_fields {
                match interpreter.eval(&field.from) {
                    Ok(Val::NotExist) => {} // Skip.
                    Ok(v) => {
                        output.insert(field.to.clone().into(), v.to_vrl());
                    }
                    Err(e) => {
                        error!("Project failed: {e}");
                        continue 'logs_loop;
                    }
                };
            }

            yield output;
        }
    }))
}
