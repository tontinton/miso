use color_eyre::Result;
use miso_workflow_types::{expr::Expr, log::Log};

use super::{Val, get_field_value, ident};

/// Eval left and right and run some function like so: left(right).
macro_rules! eval_lr {
    ($self:ident, $func:ident, $l:expr, $r:expr) => {
        $self.eval($l)?.$func(&$self.eval($r)?)?.into()
    };
}

pub struct LogInterpreter<'a> {
    pub log: &'a Log,
}

impl<'a> LogInterpreter<'a> {
    pub fn eval(&self, expr: &'a Expr) -> Result<Val<'a>> {
        Ok(match &expr {
            Expr::Field(field) => ident(self.log, field),
            Expr::Literal(value) => Val::borrowed(value),
            Expr::Exists(field) => Val::bool(get_field_value(self.log, field).is_some()),
            Expr::Cast(ty, expr) => self.eval(expr)?.cast(*ty)?,
            Expr::Or(left, right) => {
                Val::bool(self.eval(left)?.to_bool() || self.eval(right)?.to_bool())
            }
            Expr::And(left, right) => {
                Val::bool(self.eval(left)?.to_bool() && self.eval(right)?.to_bool())
            }
            Expr::Not(expr) => Val::bool(!self.eval(expr)?.to_bool()),
            Expr::Bin(expr, by) => self.eval(expr)?.bin(&self.eval(by)?)?.into(),
            Expr::In(l, r) => self
                .eval(l)?
                .is_in(&r.iter().map(|e| self.eval(e)).collect::<Result<Vec<_>>>()?)?
                .into(),
            Expr::Contains(l, r) => eval_lr!(self, contains, l, r),
            Expr::StartsWith(l, r) => eval_lr!(self, starts_with, l, r),
            Expr::EndsWith(l, r) => eval_lr!(self, ends_with, l, r),
            Expr::Has(l, r) => eval_lr!(self, has, l, r),
            Expr::HasCs(l, r) => eval_lr!(self, has_cs, l, r),
            Expr::Eq(l, r) => eval_lr!(self, eq, l, r),
            Expr::Ne(l, r) => eval_lr!(self, ne, l, r),
            Expr::Gt(l, r) => eval_lr!(self, gt, l, r),
            Expr::Gte(l, r) => eval_lr!(self, gte, l, r),
            Expr::Lt(l, r) => eval_lr!(self, lt, l, r),
            Expr::Lte(l, r) => eval_lr!(self, lte, l, r),
            Expr::Mul(l, r) => eval_lr!(self, mul, l, r),
            Expr::Div(l, r) => eval_lr!(self, div, l, r),
            Expr::Plus(l, r) => eval_lr!(self, add, l, r),
            Expr::Minus(l, r) => eval_lr!(self, sub, l, r),
        })
    }
}
