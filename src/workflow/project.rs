use std::borrow::Cow;

use async_stream::stream;
use color_eyre::eyre::Result;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::log::{Log, LogStream};

use super::interpreter::{ident, CastType, Val};

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
    pub from: ProjectAst,
    pub to: String,
}

struct ProjectInterpreter<'a> {
    log: &'a Log,
}

impl<'a> ProjectInterpreter<'a> {
    fn new(log: &'a Log) -> Self {
        Self { log }
    }
}

impl<'a> ProjectInterpreter<'a> {
    fn eval(&self, ast: &'a ProjectAst) -> Result<Val<'a>> {
        Ok(match ast {
            ProjectAst::Id(name) => ident(self.log, name)?,
            ProjectAst::Lit(value) => Val::borrowed(value),
            ProjectAst::Cast(ty, expr) => self.eval(expr)?.cast(*ty)?,
            ProjectAst::Mul(l, r) => self.eval(l)?.mul(&self.eval(r)?)?.into(),
            ProjectAst::Div(l, r) => self.eval(l)?.div(&self.eval(r)?)?.into(),
            ProjectAst::Plus(l, r) => self.eval(l)?.add(&self.eval(r)?)?.into(),
            ProjectAst::Minus(l, r) => self.eval(l)?.sub(&self.eval(r)?)?.into(),
        })
    }
}

async fn project_stream_ex(
    project_fields: Vec<ProjectField>,
    mut input_stream: LogStream,
    extend: bool,
) -> LogStream {
    Box::pin(stream! {
        while let Some(mut log) = input_stream.next().await {
            let mut output = Log::new();

            {
                let interpreter = ProjectInterpreter::new(&log);

                for field in &project_fields {
                    match interpreter.eval(&field.from) {
                        Ok(Val(None)) => {} // Skip.
                        Ok(v) => {
                            let owned = match v.0.unwrap() {
                                Cow::Borrowed(borrowed) => borrowed.clone(),
                                Cow::Owned(owned) => owned,
                            };
                            output.insert(field.to.clone(), owned);
                        }
                        Err(e) => {
                            warn!("Project failed: {e}");
                            continue;
                        }
                    };
                }
            }

            if extend {
                log.extend(output);
                yield log;
            } else {
                yield output;
            }
        }
    })
}

pub async fn project_stream(
    project_fields: Vec<ProjectField>,
    input_stream: LogStream,
) -> LogStream {
    project_stream_ex(project_fields, input_stream, false).await
}

pub async fn extend_stream(
    project_fields: Vec<ProjectField>,
    input_stream: LogStream,
) -> LogStream {
    project_stream_ex(project_fields, input_stream, true).await
}
