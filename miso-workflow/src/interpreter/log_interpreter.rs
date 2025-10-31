use color_eyre::Result;
use miso_workflow_types::{expr::Expr, field::Field, log::Log};

use crate::interpreter::ExprEvaluator;

use super::{Val, get_field_value, ident};

pub struct LogInterpreter<'a> {
    pub log: &'a Log,
}

impl<'a> LogInterpreter<'a> {
    pub fn eval(&self, expr: &'a Expr) -> Result<Val<'a>> {
        ExprEvaluator::eval(self, expr)
    }
}

impl<'a> ExprEvaluator<'a> for LogInterpreter<'a> {
    fn eval_field(&self, field: &'a Field) -> Result<Val<'a>> {
        Ok(ident(self.log, field))
    }

    fn eval_exists(&self, field: &'a Field) -> Result<Val<'a>> {
        Ok(Val::bool(get_field_value(self.log, field).is_some()))
    }
}
