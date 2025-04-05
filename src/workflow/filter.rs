use async_stream::try_stream;
use color_eyre::Result;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::warn;

use crate::log::{Log, LogStream, LogTryStream};

use super::interpreter::{ident, Val};

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

    fn eval<'a>(&'a self, ast: &'a FilterAst) -> Result<Val<'a>> {
        Ok(match &ast {
            FilterAst::Id(name) => ident(&self.log, name)?,
            FilterAst::Lit(value) => Val::borrowed(value),
            FilterAst::Exists(name) => Val::owned(Value::Bool(ident(&self.log, name)?.0.is_some())),
            FilterAst::Or(exprs) => {
                let mut result = false;
                for expr in exprs {
                    if self.eval(expr)?.to_bool() {
                        result = true;
                        break;
                    }
                }
                Val::bool(result)
            }
            FilterAst::And(exprs) => {
                let mut result = true;
                for expr in exprs {
                    if !self.eval(expr)?.to_bool() {
                        result = false;
                        break;
                    }
                }
                Val::bool(result)
            }
            FilterAst::Not(expr) => Val::bool(!self.eval(expr)?.to_bool()),
            FilterAst::Contains(lhs, rhs) => self.eval(lhs)?.contains(self.eval(rhs)?)?,
            FilterAst::StartsWith(lhs, rhs) => self.eval(lhs)?.starts_with(self.eval(rhs)?)?,
            FilterAst::EndsWith(lhs, rhs) => self.eval(lhs)?.ends_with(self.eval(rhs)?)?,
            FilterAst::Eq(l, r) => self.eval(l)?.eq(self.eval(r)?)?,
            FilterAst::Ne(l, r) => self.eval(l)?.ne(self.eval(r)?)?,
            FilterAst::Gt(l, r) => self.eval(l)?.gt(self.eval(r)?)?,
            FilterAst::Gte(l, r) => self.eval(l)?.gte(self.eval(r)?)?,
            FilterAst::Lt(l, r) => self.eval(l)?.lt(self.eval(r)?)?,
            FilterAst::Lte(l, r) => self.eval(l)?.lte(self.eval(r)?)?,
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
                    warn!("Filter failed: {e}");
                    false
                },
            };

            if keep {
                yield interpreter.log;
            }
        }
    }))
}
