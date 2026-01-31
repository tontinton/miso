use color_eyre::Result;
use miso_workflow::interpreter::{ExprEvaluator, Val};
use miso_workflow_types::{expr::Expr, field::Field, value::Value};

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
        Expr::Not(inner) => {
            let inner_eval = partial_eval(inner)?;
            match inner_eval {
                // SimplifyStackedNot: NOT(NOT(x)) -> x
                Expr::Not(inner_inner) => return Ok(*inner_inner),

                // NOT comparison inversion
                Expr::Eq(l, r) => return Ok(Expr::Ne(l, r)),
                Expr::Ne(l, r) => return Ok(Expr::Eq(l, r)),
                Expr::Gt(l, r) => return Ok(Expr::Lte(l, r)),
                Expr::Gte(l, r) => return Ok(Expr::Lt(l, r)),
                Expr::Lt(l, r) => return Ok(Expr::Gte(l, r)),
                Expr::Lte(l, r) => return Ok(Expr::Gt(l, r)),

                // De Morgan's Laws
                Expr::And(l, r) => {
                    let not_l = partial_eval(&Expr::Not(l))?;
                    let not_r = partial_eval(&Expr::Not(r))?;
                    return Ok(Expr::Or(Box::new(not_l), Box::new(not_r)));
                }
                Expr::Or(l, r) => {
                    let not_l = partial_eval(&Expr::Not(l))?;
                    let not_r = partial_eval(&Expr::Not(r))?;
                    return Ok(Expr::And(Box::new(not_l), Box::new(not_r)));
                }

                _ => Expr::Not(Box::new(inner_eval)),
            }
        }
        Expr::In(l, r) => {
            let left = partial_eval(l)?;
            let items: Vec<Expr> = r.iter().map(partial_eval).collect::<Result<Vec<_>>>()?;

            // RemoveRedundantInItems: deduplicate items
            let mut deduped = Vec::new();
            for item in items {
                if !deduped.contains(&item) {
                    deduped.push(item);
                }
            }

            // Single item: convert to equality
            if deduped.len() == 1 {
                return Ok(Expr::Eq(Box::new(left), Box::new(deduped.pop().unwrap())));
            }

            Expr::In(Box::new(left), deduped)
        }
        Expr::Case(predicates, default) => Expr::Case(
            predicates
                .iter()
                .map(|(p, t)| Ok((partial_eval(p)?, partial_eval(t)?)))
                .collect::<Result<Vec<_>>>()?,
            Box::new(partial_eval(default)?),
        ),

        Expr::And(l, r) => {
            let l = partial_eval(l)?;
            let r = partial_eval(r)?;

            match (&l, &r) {
                (Expr::Literal(Value::Bool(false)), _) | (_, Expr::Literal(Value::Bool(false))) => {
                    Expr::Literal(Value::Bool(false))
                }
                (Expr::Literal(Value::Bool(true)), _) => r,
                (_, Expr::Literal(Value::Bool(true))) => l,
                // RemoveRedundantLogicalTerms: a AND a -> a
                _ if l == r => l,
                _ => Expr::And(Box::new(l), Box::new(r)),
            }
        }

        Expr::Or(l, r) => {
            let l = partial_eval(l)?;
            let r = partial_eval(r)?;

            match (&l, &r) {
                (Expr::Literal(Value::Bool(true)), _) | (_, Expr::Literal(Value::Bool(true))) => {
                    Expr::Literal(Value::Bool(true))
                }
                (Expr::Literal(Value::Bool(false)), _) => r,
                (_, Expr::Literal(Value::Bool(false))) => l,
                // RemoveRedundantLogicalTerms: a OR a -> a
                _ if l == r => l,
                _ => Expr::Or(Box::new(l), Box::new(r)),
            }
        }

        Expr::Plus(l, r) => {
            let l = partial_eval(l)?;
            let r = partial_eval(r)?;
            match (&l, &r) {
                (Expr::Literal(a), _) if a.as_f64() == Some(0.0) => r,
                (_, Expr::Literal(b)) if b.as_f64() == Some(0.0) => l,
                _ => Expr::Plus(Box::new(l), Box::new(r)),
            }
        }

        Expr::Minus(l, r) => {
            let l = partial_eval(l)?;
            let r = partial_eval(r)?;
            match &r {
                Expr::Literal(b) if b.as_f64() == Some(0.0) => l,
                _ => Expr::Minus(Box::new(l), Box::new(r)),
            }
        }

        Expr::Mul(l, r) => {
            let l = partial_eval(l)?;
            let r = partial_eval(r)?;
            match (&l, &r) {
                (Expr::Literal(a), _) if a.as_f64() == Some(0.0) => Expr::Literal(Value::Int(0)),
                (_, Expr::Literal(b)) if b.as_f64() == Some(0.0) => Expr::Literal(Value::Int(0)),
                (Expr::Literal(a), _) if a.as_f64() == Some(1.0) => r,
                (_, Expr::Literal(b)) if b.as_f64() == Some(1.0) => l,
                _ => Expr::Mul(Box::new(l), Box::new(r)),
            }
        }

        Expr::Div(l, r) => {
            let l = partial_eval(l)?;
            let r = partial_eval(r)?;
            match (&l, &r) {
                (Expr::Literal(a), _) if a.as_f64() == Some(0.0) => {
                    Expr::Literal(Value::Float(0.0))
                }
                (_, Expr::Literal(b)) if b.as_f64() == Some(1.0) => l,
                _ => Expr::Div(Box::new(l), Box::new(r)),
            }
        }

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

    #[test]
    fn test_partial_eval_and_or_short_circuit() {
        // true AND x -> x
        let expr = Expr::And(
            Box::new(Expr::Literal(Value::Bool(true))),
            Box::new(Expr::Field(field_unwrap!("x"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
            _ => panic!("Expected field x, got {:?}", result),
        }

        // false AND x -> false
        let expr = Expr::And(
            Box::new(Expr::Literal(Value::Bool(false))),
            Box::new(Expr::Field(field_unwrap!("x"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Literal(Value::Bool(false)) => (),
            _ => panic!("Expected false, got {:?}", result),
        }

        // true OR x -> true
        let expr = Expr::Or(
            Box::new(Expr::Literal(Value::Bool(true))),
            Box::new(Expr::Field(field_unwrap!("x"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Literal(Value::Bool(true)) => (),
            _ => panic!("Expected true, got {:?}", result),
        }

        // false OR x -> x
        let expr = Expr::Or(
            Box::new(Expr::Literal(Value::Bool(false))),
            Box::new(Expr::Field(field_unwrap!("x"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
            _ => panic!("Expected field x, got {:?}", result),
        }
    }

    #[test]
    fn test_partial_eval_arithmetic_folding() {
        // x + 0 -> x
        let expr = Expr::Plus(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(0))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
            _ => panic!("Expected field x, got {:?}", result),
        }

        // 0 + x -> x
        let expr = Expr::Plus(
            Box::new(Expr::Literal(Value::Int(0))),
            Box::new(Expr::Field(field_unwrap!("x"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
            _ => panic!("Expected field x, got {:?}", result),
        }

        // x * 1 -> x
        let expr = Expr::Mul(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
            _ => panic!("Expected field x, got {:?}", result),
        }

        // 1 * x -> x
        let expr = Expr::Mul(
            Box::new(Expr::Literal(Value::Int(1))),
            Box::new(Expr::Field(field_unwrap!("x"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
            _ => panic!("Expected field x, got {:?}", result),
        }

        // x * 0 -> 0
        let expr = Expr::Mul(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(0))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Literal(Value::Int(0)) => (),
            _ => panic!("Expected 0, got {:?}", result),
        }

        // 0 / x -> 0
        let expr = Expr::Div(
            Box::new(Expr::Literal(Value::Int(0))),
            Box::new(Expr::Field(field_unwrap!("x"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Literal(Value::Float(0.0)) => (),
            _ => panic!("Expected 0, got {:?}", result),
        }

        // x / 1 -> x
        let expr = Expr::Div(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
            _ => panic!("Expected field x, got {:?}", result),
        }
    }

    #[test]
    fn test_simplify_stacked_not() {
        // NOT(NOT(NOT(x))) -> NOT(x)
        let expr = Expr::Not(Box::new(Expr::Not(Box::new(Expr::Not(Box::new(
            Expr::Field(field_unwrap!("x")),
        ))))));
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Not(inner) => match *inner {
                Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
                _ => panic!("Expected NOT(field x), got {:?}", inner),
            },
            _ => panic!("Expected NOT(x), got {:?}", result),
        }
    }

    #[test]
    fn test_remove_redundant_logical_terms() {
        // x AND x -> x
        let expr = Expr::And(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Field(field_unwrap!("x"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
            _ => panic!("Expected field x, got {:?}", result),
        }

        // x OR x -> x
        let expr = Expr::Or(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Field(field_unwrap!("x"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Field(f) => assert_eq!(&f.to_string(), "x"),
            _ => panic!("Expected field x, got {:?}", result),
        }

        // (x > 1) AND (x > 1) -> (x > 1)
        let cond = Expr::Gt(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        );
        let expr = Expr::And(Box::new(cond.clone()), Box::new(cond));
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Gt(_, _) => (),
            _ => panic!("Expected x > 1, got {:?}", result),
        }
    }

    #[test]
    fn test_remove_redundant_in_items() {
        // x IN (1, 1, 2) -> x IN (1, 2)
        let expr = Expr::In(
            Box::new(Expr::Field(field_unwrap!("x"))),
            vec![
                Expr::Literal(Value::Int(1)),
                Expr::Literal(Value::Int(1)),
                Expr::Literal(Value::Int(2)),
            ],
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::In(_, items) => {
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], Expr::Literal(Value::Int(1))));
                assert!(matches!(&items[1], Expr::Literal(Value::Int(2))));
            }
            _ => panic!("Expected IN expression, got {:?}", result),
        }

        // x IN (1) -> x == 1
        let expr = Expr::In(
            Box::new(Expr::Field(field_unwrap!("x"))),
            vec![Expr::Literal(Value::Int(1))],
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Eq(left, right) => {
                assert!(matches!(*left, Expr::Field(_)));
                assert!(matches!(*right, Expr::Literal(Value::Int(1))));
            }
            _ => panic!("Expected equality, got {:?}", result),
        }

        // x IN (1, 1) -> x == 1 (dedup then convert)
        let expr = Expr::In(
            Box::new(Expr::Field(field_unwrap!("x"))),
            vec![
                Expr::Literal(Value::Int(1)),
                Expr::Literal(Value::Int(1)),
            ],
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Eq(left, right) => {
                assert!(matches!(*left, Expr::Field(_)));
                assert!(matches!(*right, Expr::Literal(Value::Int(1))));
            }
            _ => panic!("Expected equality, got {:?}", result),
        }
    }

    #[test]
    fn test_not_comparison_inversion() {
        // NOT(x == 1) -> x != 1
        let expr = Expr::Not(Box::new(Expr::Eq(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(matches!(result, Expr::Ne(_, _)), "Expected Ne, got {:?}", result);

        // NOT(x != 1) -> x == 1
        let expr = Expr::Not(Box::new(Expr::Ne(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(matches!(result, Expr::Eq(_, _)), "Expected Eq, got {:?}", result);

        // NOT(x > 1) -> x <= 1
        let expr = Expr::Not(Box::new(Expr::Gt(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(matches!(result, Expr::Lte(_, _)), "Expected Lte, got {:?}", result);

        // NOT(x >= 1) -> x < 1
        let expr = Expr::Not(Box::new(Expr::Gte(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(matches!(result, Expr::Lt(_, _)), "Expected Lt, got {:?}", result);

        // NOT(x < 1) -> x >= 1
        let expr = Expr::Not(Box::new(Expr::Lt(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(matches!(result, Expr::Gte(_, _)), "Expected Gte, got {:?}", result);

        // NOT(x <= 1) -> x > 1
        let expr = Expr::Not(Box::new(Expr::Lte(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(matches!(result, Expr::Gt(_, _)), "Expected Gt, got {:?}", result);
    }

    #[test]
    fn test_de_morgan_and() {
        // NOT(a AND b) -> (NOT a) OR (NOT b)
        let expr = Expr::Not(Box::new(Expr::And(
            Box::new(Expr::Field(field_unwrap!("a"))),
            Box::new(Expr::Field(field_unwrap!("b"))),
        )));
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Or(l, r) => {
                assert!(matches!(*l, Expr::Not(_)), "Expected NOT on left, got {:?}", l);
                assert!(matches!(*r, Expr::Not(_)), "Expected NOT on right, got {:?}", r);
            }
            _ => panic!("Expected OR, got {:?}", result),
        }
    }

    #[test]
    fn test_de_morgan_or() {
        // NOT(a OR b) -> (NOT a) AND (NOT b)
        let expr = Expr::Not(Box::new(Expr::Or(
            Box::new(Expr::Field(field_unwrap!("a"))),
            Box::new(Expr::Field(field_unwrap!("b"))),
        )));
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::And(l, r) => {
                assert!(matches!(*l, Expr::Not(_)), "Expected NOT on left, got {:?}", l);
                assert!(matches!(*r, Expr::Not(_)), "Expected NOT on right, got {:?}", r);
            }
            _ => panic!("Expected AND, got {:?}", result),
        }
    }

    #[test]
    fn test_combined_not_optimizations() {
        // NOT(NOT(x > 1)) -> x > 1
        let expr = Expr::Not(Box::new(Expr::Not(Box::new(Expr::Gt(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )))));
        let result = partial_eval(&expr).unwrap();
        assert!(matches!(result, Expr::Gt(_, _)), "Expected Gt, got {:?}", result);

        // NOT(a AND NOT(b)) -> (NOT a) OR b
        let expr = Expr::Not(Box::new(Expr::And(
            Box::new(Expr::Field(field_unwrap!("a"))),
            Box::new(Expr::Not(Box::new(Expr::Field(field_unwrap!("b"))))),
        )));
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Or(l, r) => {
                assert!(matches!(*l, Expr::Not(_)), "Expected NOT on left, got {:?}", l);
                assert!(matches!(*r, Expr::Field(_)), "Expected field on right, got {:?}", r);
            }
            _ => panic!("Expected OR, got {:?}", result),
        }

        // NOT(x == 1 AND y > 2) -> x != 1 OR y <= 2
        let expr = Expr::Not(Box::new(Expr::And(
            Box::new(Expr::Eq(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(1))),
            )),
            Box::new(Expr::Gt(
                Box::new(Expr::Field(field_unwrap!("y"))),
                Box::new(Expr::Literal(Value::Int(2))),
            )),
        )));
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Or(l, r) => {
                assert!(matches!(*l, Expr::Ne(_, _)), "Expected Ne on left, got {:?}", l);
                assert!(matches!(*r, Expr::Lte(_, _)), "Expected Lte on right, got {:?}", r);
            }
            _ => panic!("Expected OR, got {:?}", result),
        }
    }
}
