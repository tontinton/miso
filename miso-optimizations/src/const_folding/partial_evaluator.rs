use color_eyre::Result;
use miso_workflow::interpreter::{ExprEvaluator, Val};
use miso_workflow_types::{expr::Expr, field::Field};

/// Partial eval a binop.
macro_rules! partial_eval_lr {
    ($variant:ident, $l:expr, $r:expr) => {{
        let left_opt = partial_eval($l)?;
        let right_opt = partial_eval($r)?;
        Expr::$variant(Box::new(left_opt), Box::new(right_opt))
    }};
}

/// Const evaluation (for expressions without fields).
pub struct ConstEvaluator;

impl<'a> ExprEvaluator<'a> for ConstEvaluator {
    fn eval_field(&self, _field: &'a Field) -> Result<Val<'a>> {
        Ok(Val::not_exist())
    }

    fn eval_exists(&self, _field: &'a Field) -> Result<Val<'a>> {
        Ok(Val::not_exist())
    }

    fn eval_to_bool(&self, expr: &'a Expr) -> Result<Option<bool>> {
        let val = self.eval(expr)?;
        if !val.is_exist() {
            return Ok(None);
        }
        Ok(Some(val.to_bool()))
    }
}

/// Partially evaluate an expression, const-folding where possible.
pub fn partial_eval(expr: &Expr) -> Result<Expr> {
    // Try to evaluate into a constant literal expression.
    // If unable, try to partially evaluate the inner expressions.
    match ConstEvaluator.eval(expr) {
        Ok(val) if val.is_exist() => {
            return Ok(Expr::Literal(val.0.unwrap().into_owned()));
        }
        _ => {}
    }

    Ok(match expr {
        Expr::Literal(_) | Expr::Field(_) | Expr::Exists(_) => expr.clone(),

        Expr::Cast(ty, inner) => Expr::Cast(*ty, Box::new(partial_eval(inner)?)),
        Expr::Not(inner) => Expr::Not(Box::new(partial_eval(inner)?)),
        Expr::In(l, r) => Expr::In(
            Box::new(partial_eval(l)?),
            r.iter().map(partial_eval).collect::<Result<Vec<_>>>()?,
        ),
        Expr::Case(predicates, default) => Expr::Case(
            predicates
                .iter()
                .map(|(p, t)| Ok((partial_eval(p)?, partial_eval(t)?)))
                .collect::<Result<Vec<_>>>()?,
            Box::new(partial_eval(default)?),
        ),

        Expr::Or(l, r) => partial_eval_lr!(Or, l, r),
        Expr::And(l, r) => partial_eval_lr!(And, l, r),
        Expr::Plus(l, r) => partial_eval_lr!(Plus, l, r),
        Expr::Minus(l, r) => partial_eval_lr!(Minus, l, r),
        Expr::Mul(l, r) => partial_eval_lr!(Mul, l, r),
        Expr::Div(l, r) => partial_eval_lr!(Div, l, r),
        Expr::Eq(l, r) => partial_eval_lr!(Eq, l, r),
        Expr::Ne(l, r) => partial_eval_lr!(Ne, l, r),
        Expr::Gt(l, r) => partial_eval_lr!(Gt, l, r),
        Expr::Gte(l, r) => partial_eval_lr!(Gte, l, r),
        Expr::Lt(l, r) => partial_eval_lr!(Lt, l, r),
        Expr::Lte(l, r) => partial_eval_lr!(Lte, l, r),
        Expr::Contains(l, r) => partial_eval_lr!(Contains, l, r),
        Expr::StartsWith(l, r) => partial_eval_lr!(StartsWith, l, r),
        Expr::EndsWith(l, r) => partial_eval_lr!(EndsWith, l, r),
        Expr::Has(l, r) => partial_eval_lr!(Has, l, r),
        Expr::HasCs(l, r) => partial_eval_lr!(HasCs, l, r),
        Expr::Bin(l, r) => partial_eval_lr!(Bin, l, r),
    })
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use miso_workflow_types::{field::Field, field_unwrap, value::Value};

    use super::*;

    #[test]
    fn test_partial_eval_const_folding() {
        // 50 * 2 -> 100.
        let expr = Expr::Mul(
            Box::new(Expr::Literal(Value::from(50))),
            Box::new(Expr::Literal(Value::from(2))),
        );

        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Literal(Value::Int(100)) => (),
            _ => panic!("Expected 100, got {:?}", result),
        }
    }

    #[test]
    fn test_partial_eval_with_field() {
        // x > 50 * 2 -> x > 100.
        let expr = Expr::Gt(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Mul(
                Box::new(Expr::Literal(Value::from(50))),
                Box::new(Expr::Literal(Value::from(2))),
            )),
        );

        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Gt(left, right) => {
                assert!(matches!(*left, Expr::Field(_)));
                assert!(matches!(*right, Expr::Literal(Value::Int(100))));
            }
            _ => panic!("Expected x > 100, got {:?}", result),
        }
    }

    #[test]
    fn test_partial_eval_case_with_constants_and_field() {
        // CASE
        //   WHEN 1 + 1 = 2 THEN "yes"
        //   WHEN x > 10 THEN "maybe"
        //   ELSE "no"
        // =>
        // "yes"

        let expr = Expr::Case(
            vec![
                (
                    Expr::Eq(
                        Box::new(Expr::Plus(
                            Box::new(Expr::Literal(Value::Int(1))),
                            Box::new(Expr::Literal(Value::Int(1))),
                        )),
                        Box::new(Expr::Literal(Value::Int(2))),
                    ),
                    Expr::Literal(Value::from("yes")),
                ),
                (
                    Expr::Gt(
                        Box::new(Expr::Field(field_unwrap!("x"))),
                        Box::new(Expr::Literal(Value::Int(10))),
                    ),
                    Expr::Literal(Value::from("maybe")),
                ),
            ],
            Box::new(Expr::Literal(Value::from("no"))),
        );

        let result = partial_eval(&expr).unwrap();

        match result {
            Expr::Literal(Value::String(s)) => {
                assert_eq!(s, "yes");
            }
            other => panic!("Expected \"yes\" literal, got {:?}", other),
        }
    }
}
