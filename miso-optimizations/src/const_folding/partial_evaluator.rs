use color_eyre::Result;
use miso_workflow::interpreter::Val;
use miso_workflow_types::expr::Expr;

/// Eval left and right and run some function like so: func(left, right).
macro_rules! eval_lr {
    ($func:ident, $l:expr, $r:expr) => {
        eval($l)?.$func(&eval($r)?)?.into()
    };
}

/// Eval expression and check if it exists, return not_exist if it doesn't.
macro_rules! eval_exist {
    ($eval:expr, $expr:expr) => {{
        let val = $eval($expr)?;
        if !val.is_exist() {
            return Ok(val);
        }
        val
    }};
}

/// Eval expression, check existence, and convert to bool.
macro_rules! eval_to_bool {
    ($eval:expr, $expr:expr) => {
        eval_exist!($eval, $expr).to_bool()
    };
}

/// Partial eval a binop.
macro_rules! partial_eval_lr {
    ($variant:ident, $l:expr, $r:expr) => {{
        let left_opt = partial_eval($l)?;
        let right_opt = partial_eval($r)?;
        Expr::$variant(Box::new(left_opt), Box::new(right_opt))
    }};
}

/// Const evaluation (for expressions without fields)
fn eval(expr: &Expr) -> Result<Val<'static>> {
    Ok(match expr {
        Expr::Literal(value) => Val::owned(value.clone()),
        Expr::Field(_) | Expr::Exists(_) => Val::not_exist(),
        Expr::Cast(ty, expr) => eval(expr)?.cast(*ty)?,
        Expr::Or(left, right) => Val::bool(eval_to_bool!(eval, left) || eval_to_bool!(eval, right)),
        Expr::And(left, right) => {
            Val::bool(eval_to_bool!(eval, left) && eval_to_bool!(eval, right))
        }
        Expr::Not(expr) => Val::bool(!eval_to_bool!(eval, expr)),
        Expr::Bin(expr, by) => eval(expr)?.bin(&eval(by)?)?.into(),
        Expr::In(l, r) => eval(l)?
            .is_in(&r.iter().map(eval).collect::<Result<Vec<_>>>()?)?
            .into(),
        Expr::Plus(l, r) => eval_lr!(add, l, r),
        Expr::Minus(l, r) => eval_lr!(sub, l, r),
        Expr::Mul(l, r) => eval_lr!(mul, l, r),
        Expr::Div(l, r) => eval_lr!(div, l, r),
        Expr::Eq(l, r) => eval_lr!(eq, l, r),
        Expr::Ne(l, r) => eval_lr!(ne, l, r),
        Expr::Gt(l, r) => eval_lr!(gt, l, r),
        Expr::Gte(l, r) => eval_lr!(gte, l, r),
        Expr::Lt(l, r) => eval_lr!(lt, l, r),
        Expr::Lte(l, r) => eval_lr!(lte, l, r),
        Expr::Contains(l, r) => eval_lr!(contains, l, r),
        Expr::StartsWith(l, r) => eval_lr!(starts_with, l, r),
        Expr::EndsWith(l, r) => eval_lr!(ends_with, l, r),
        Expr::Has(l, r) => eval_lr!(has, l, r),
        Expr::HasCs(l, r) => eval_lr!(has_cs, l, r),
    })
}

/// Partially evaluate an expression, const-folding where possible.
pub fn partial_eval(expr: &Expr) -> Result<Expr> {
    // Try to evaluate into a constant literal expression.
    // If unable, try to partially evaluate the inner expressions.
    match eval(expr) {
        Ok(val) if val.is_exist() => {
            return Ok(Expr::Literal(val.0.unwrap().into_owned()));
        }
        _ => {}
    }

    Ok(match expr {
        Expr::Literal(_) | Expr::Field(_) | Expr::Exists(_) => expr.clone(),

        Expr::Cast(ty, inner) => Expr::Cast(*ty, Box::new(partial_eval(inner)?)),
        Expr::Not(inner) => Expr::Not(Box::new(partial_eval(inner)?)),
        Expr::In(l, r) => {
            let left_opt = partial_eval(l)?;
            let right_opt: Result<Vec<_>> = r.iter().map(partial_eval).collect();
            Expr::In(Box::new(left_opt), right_opt?)
        }

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
}
