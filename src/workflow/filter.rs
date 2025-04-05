use async_stream::try_stream;
use color_eyre::Result;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::log::{Log, LogStream, LogTryStream};

use super::interpreter::{ident, serde_json_to_val, Val};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FilterAst {
    Id(String),                                 // field
    Lit(serde_json::Value),                     // a json literal
    Exists(String),                             // field exists
    Or(Vec<FilterAst>),                         // ||
    And(Vec<FilterAst>),                        // &&
    Not(Box<FilterAst>),                        // !
    Contains(Box<FilterAst>, Box<FilterAst>),   // right in left
    StartsWith(Box<FilterAst>, Box<FilterAst>), // left starts with right
    EndsWith(Box<FilterAst>, Box<FilterAst>),   // left ends with right
    Eq(Box<FilterAst>, Box<FilterAst>),         // ==
    Ne(Box<FilterAst>, Box<FilterAst>),         // !=
    Gt(Box<FilterAst>, Box<FilterAst>),         // >
    Gte(Box<FilterAst>, Box<FilterAst>),        // >=
    Lt(Box<FilterAst>, Box<FilterAst>),         // <
    Lte(Box<FilterAst>, Box<FilterAst>),        // <=
}

struct FilterInterpreter {
    log: Log,
}

impl FilterInterpreter {
    fn new(log: Log) -> Self {
        Self { log }
    }

    fn eval(&self, ast: &FilterAst) -> Result<Val> {
        Ok(match &ast {
            FilterAst::Id(name) => ident(&self.log, name)?,
            FilterAst::Lit(value) => serde_json_to_val(value)?,
            FilterAst::Exists(name) => Val::Bool(!matches!(ident(&self.log, name)?, Val::NotExist)),
            FilterAst::Or(exprs) => {
                let mut result = false;
                for expr in exprs {
                    if self.eval(expr)?.to_bool() {
                        result = true;
                        break;
                    }
                }
                Val::Bool(result)
            }
            FilterAst::And(exprs) => {
                let mut result = true;
                for expr in exprs {
                    if !self.eval(expr)?.to_bool() {
                        result = false;
                        break;
                    }
                }
                Val::Bool(result)
            }
            FilterAst::Not(expr) => Val::Bool(!self.eval(expr)?.to_bool()),
            FilterAst::Contains(lhs, rhs) => {
                self.eval(lhs)?.contains(&self.eval(rhs)?).map(Val::Bool)?
            }
            FilterAst::StartsWith(lhs, rhs) => self
                .eval(lhs)?
                .starts_with(&self.eval(rhs)?)
                .map(Val::Bool)?,
            FilterAst::EndsWith(lhs, rhs) => {
                self.eval(lhs)?.ends_with(&self.eval(rhs)?).map(Val::Bool)?
            }
            FilterAst::Eq(l, r) => self.eval(l)?.eq(&self.eval(r)?).map(Val::Bool)?,
            FilterAst::Ne(l, r) => self.eval(l)?.ne(&self.eval(r)?).map(Val::Bool)?,
            FilterAst::Gt(l, r) => self.eval(l)?.gt(&self.eval(r)?).map(Val::Bool)?,
            FilterAst::Gte(l, r) => self.eval(l)?.gte(&self.eval(r)?).map(Val::Bool)?,
            FilterAst::Lt(l, r) => self.eval(l)?.lt(&self.eval(r)?).map(Val::Bool)?,
            FilterAst::Lte(l, r) => self.eval(l)?.lte(&self.eval(r)?).map(Val::Bool)?,
        })
    }
}

pub fn filter_stream(ast: FilterAst, mut input_stream: LogStream) -> Result<LogTryStream> {
    Ok(Box::pin(try_stream! {
        while let Some(log) = input_stream.next().await {
            let interpreter = FilterInterpreter::new(log);

            let keep = match interpreter.eval(&ast) {
                Ok(v) => v.to_bool(),
                Err(e) => {
                    error!("Filter error: {e}");
                    false
                },
            };

            if keep {
                yield interpreter.log;
            }
        }
    }))
}
