use color_eyre::Result;
use miso_workflow_types::{expr::Expr, field::Field};

use crate::interpreter::Val;

/// Eval left and right and run some function like so: func(left, right).
macro_rules! eval_lr {
    ($self:ident, $func:ident, $l:expr, $r:expr) => {
        $self.eval($l)?.$func(&$self.eval($r)?)?.into()
    };
}

macro_rules! eval_to_bool {
    ($self:ident, $expr:expr) => {
        match $self.eval_to_bool($expr)? {
            Some(value) => value,
            None => return Ok(Val::not_exist()),
        }
    };
}

pub trait ExprEvaluator<'a> {
    fn eval_field(&self, field: &'a Field) -> Result<Val<'a>>;
    fn eval_exists(&self, field: &'a Field) -> Result<Val<'a>>;

    fn eval_to_bool(&self, expr: &'a Expr) -> Result<Option<bool>> {
        Ok(Some(self.eval(expr)?.to_bool()))
    }

    fn eval(&self, expr: &'a Expr) -> Result<Val<'a>> {
        Ok(match expr {
            Expr::Field(field) => self.eval_field(field)?,
            Expr::Exists(field) => self.eval_exists(field)?,
            Expr::Literal(value) => Val::borrowed(value),
            Expr::Cast(ty, inner) => self.eval(inner)?.cast(*ty)?,
            Expr::Or(left, right) => {
                Val::bool(eval_to_bool!(self, left) || eval_to_bool!(self, right))
            }
            Expr::And(left, right) => {
                Val::bool(eval_to_bool!(self, left) && eval_to_bool!(self, right))
            }
            Expr::Not(inner) => Val::bool(!eval_to_bool!(self, inner)),
            Expr::Bin(expr, by) => self.eval(expr)?.bin(&self.eval(by)?)?.into(),
            Expr::Extract(regex, group, source) => {
                self.eval(source)?
                    .extract(&self.eval(regex)?, &self.eval(group)?)?
            }
            Expr::Case(predicates, default) => {
                for (predicate, then) in predicates {
                    if eval_to_bool!(self, predicate) {
                        return self.eval(then);
                    }
                }
                self.eval(default)?
            }
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
