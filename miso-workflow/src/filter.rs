use color_eyre::Result;
use miso_workflow_types::{
    filter::FilterAst,
    log::{Log, LogItem, LogIter},
};
use tracing::warn;

use super::{
    interpreter::{Val, ident},
    try_next_with_partial_passthrough,
};

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
            FilterAst::Exists(name) => Val::bool(ident(&self.log, name)?.0.is_some()),
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
            FilterAst::In(lhs, rhs) => self
                .eval(lhs)?
                .is_in(
                    &rhs.iter()
                        .map(|e| self.eval(e))
                        .collect::<Result<Vec<_>>>()?,
                )?
                .into(),
            FilterAst::Contains(lhs, rhs) => self.eval(lhs)?.contains(&self.eval(rhs)?)?.into(),
            FilterAst::StartsWith(lhs, rhs) => {
                self.eval(lhs)?.starts_with(&self.eval(rhs)?)?.into()
            }
            FilterAst::EndsWith(lhs, rhs) => self.eval(lhs)?.ends_with(&self.eval(rhs)?)?.into(),
            FilterAst::Has(lhs, rhs) => self.eval(lhs)?.has(&self.eval(rhs)?)?.into(),
            FilterAst::HasCs(lhs, rhs) => self.eval(lhs)?.has_cs(&self.eval(rhs)?)?.into(),
            FilterAst::Eq(l, r) => self.eval(l)?.eq(&self.eval(r)?)?.into(),
            FilterAst::Ne(l, r) => self.eval(l)?.ne(&self.eval(r)?)?.into(),
            FilterAst::Gt(l, r) => self.eval(l)?.gt(&self.eval(r)?)?.into(),
            FilterAst::Gte(l, r) => self.eval(l)?.gte(&self.eval(r)?)?.into(),
            FilterAst::Lt(l, r) => self.eval(l)?.lt(&self.eval(r)?)?.into(),
            FilterAst::Lte(l, r) => self.eval(l)?.lte(&self.eval(r)?)?.into(),
            FilterAst::Mul(l, r) => self.eval(l)?.mul(&self.eval(r)?)?.into(),
            FilterAst::Div(l, r) => self.eval(l)?.div(&self.eval(r)?)?.into(),
            FilterAst::Plus(l, r) => self.eval(l)?.add(&self.eval(r)?)?.into(),
            FilterAst::Minus(l, r) => self.eval(l)?.sub(&self.eval(r)?)?.into(),
        })
    }
}

pub struct FilterIter {
    input: LogIter,
    ast: FilterAst,
}

impl FilterIter {
    pub fn new(input: LogIter, ast: FilterAst) -> Self {
        Self { input, ast }
    }
}

impl Iterator for FilterIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(log) = try_next_with_partial_passthrough!(self.input) {
            let interpreter = FilterInterpreter::new(log);
            let keep = match interpreter.eval(&self.ast) {
                Ok(v) => v.to_bool(),
                Err(e) => {
                    warn!("Filter failed: {e}");
                    false
                }
            };

            if !keep {
                continue;
            }

            return Some(LogItem::Log(interpreter.log));
        }
        None
    }
}
