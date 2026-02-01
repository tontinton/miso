use color_eyre::Result;
use miso_workflow::interpreter::{ExprEvaluator, Val};
use miso_workflow_types::{expr::Expr, field::Field, value::Value};

/// Extract (field, literal) from field == literal expression
fn extract_field_eq_literal(expr: &Expr) -> Option<(&Field, &Value)> {
    match expr {
        Expr::Eq(l, r) => match (l.as_ref(), r.as_ref()) {
            (Expr::Field(f), Expr::Literal(v)) | (Expr::Literal(v), Expr::Field(f)) => Some((f, v)),
            _ => None,
        },
        _ => None,
    }
}

/// Extract field from equality or IN expression (for OR-to-IN normalization)
fn extract_or_in_field(expr: &Expr) -> Option<&Field> {
    match expr {
        Expr::Eq(l, r) => match (l.as_ref(), r.as_ref()) {
            (Expr::Field(f), Expr::Literal(_)) | (Expr::Literal(_), Expr::Field(f)) => Some(f),
            _ => None,
        },
        Expr::In(inner, _) => match inner.as_ref() {
            Expr::Field(f) => Some(f),
            _ => None,
        },
        _ => None,
    }
}

/// Collect all values from OR chain of field == literal or IN expressions
fn collect_or_equalities(expr: &Expr, field: &Field, values: &mut Vec<Value>) -> bool {
    match expr {
        Expr::Or(l, r) => {
            collect_or_equalities(l, field, values) && collect_or_equalities(r, field, values)
        }
        Expr::In(inner_field, items) => {
            if let Expr::Field(f) = inner_field.as_ref()
                && f == field
            {
                for item in items {
                    if let Expr::Literal(v) = item {
                        values.push(v.clone());
                    } else {
                        return false;
                    }
                }
                return true;
            }
            false
        }
        _ => {
            if let Some((f, v)) = extract_field_eq_literal(expr)
                && f == field
            {
                values.push(v.clone());
                return true;
            }
            false
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum CmpOp {
    Gt,
    Gte,
    Lt,
    Lte,
}

fn extract_field_cmp(expr: &Expr) -> Option<(&Field, CmpOp, &Value)> {
    use CmpOp::*;
    match expr {
        Expr::Gt(l, r) => match (l.as_ref(), r.as_ref()) {
            (Expr::Field(f), Expr::Literal(v)) => Some((f, Gt, v)),
            (Expr::Literal(v), Expr::Field(f)) => Some((f, Lt, v)),
            _ => None,
        },
        Expr::Gte(l, r) => match (l.as_ref(), r.as_ref()) {
            (Expr::Field(f), Expr::Literal(v)) => Some((f, Gte, v)),
            (Expr::Literal(v), Expr::Field(f)) => Some((f, Lte, v)),
            _ => None,
        },
        Expr::Lt(l, r) => match (l.as_ref(), r.as_ref()) {
            (Expr::Field(f), Expr::Literal(v)) => Some((f, Lt, v)),
            (Expr::Literal(v), Expr::Field(f)) => Some((f, Gt, v)),
            _ => None,
        },
        Expr::Lte(l, r) => match (l.as_ref(), r.as_ref()) {
            (Expr::Field(f), Expr::Literal(v)) => Some((f, Lte, v)),
            (Expr::Literal(v), Expr::Field(f)) => Some((f, Gte, v)),
            _ => None,
        },
        _ => None,
    }
}

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

    fn field_exists(&self, _field: &'a Field) -> Option<bool> {
        None
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
        Expr::Literal(_) | Expr::Field(_) => expr.clone(),
        Expr::Exists(inner) => Expr::Exists(Box::new(partial_eval(inner)?)),

        Expr::Cast(ty, inner) => Expr::Cast(*ty, Box::new(partial_eval(inner)?)),
        Expr::Not(inner) => {
            let inner_eval = partial_eval(inner)?;
            match inner_eval {
                Expr::Not(inner_inner) => return Ok(*inner_inner),
                Expr::Gt(l, r) => return Ok(Expr::Lte(l, r)),
                Expr::Gte(l, r) => return Ok(Expr::Lt(l, r)),
                Expr::Lt(l, r) => return Ok(Expr::Gte(l, r)),
                Expr::Lte(l, r) => return Ok(Expr::Gt(l, r)),
                _ => Expr::Not(Box::new(inner_eval)),
            }
        }
        Expr::In(l, r) => {
            let left = partial_eval(l)?;
            let items: Vec<Expr> = r.iter().map(partial_eval).collect::<Result<Vec<_>>>()?;

            let mut deduped = Vec::new();
            for item in items {
                if !deduped.contains(&item) {
                    deduped.push(item);
                }
            }

            if deduped.len() == 1 {
                return Ok(Expr::Eq(Box::new(left), Box::new(deduped.pop().unwrap())));
            }

            Expr::In(Box::new(left), deduped)
        }
        Expr::Case(predicates, default) => {
            let mut new_predicates = Vec::new();

            for (pred, then_expr) in predicates {
                let pred_eval = partial_eval(pred)?;
                let then_eval = partial_eval(then_expr)?;

                match &pred_eval {
                    Expr::Literal(Value::Bool(true)) => return Ok(then_eval),
                    Expr::Literal(Value::Bool(false)) => continue,
                    _ => new_predicates.push((pred_eval, then_eval)),
                }
            }

            let default_eval = partial_eval(default)?;

            if new_predicates.is_empty() {
                return Ok(default_eval);
            }

            if new_predicates.len() == 1 {
                let (pred, then_val) = &new_predicates[0];
                match (then_val, &default_eval) {
                    (Expr::Literal(Value::Bool(true)), Expr::Literal(Value::Bool(false))) => {
                        return Ok(pred.clone());
                    }
                    (Expr::Literal(Value::Bool(false)), Expr::Literal(Value::Bool(true))) => {
                        return partial_eval(&Expr::Not(Box::new(pred.clone())));
                    }
                    _ => {}
                }
            }

            Expr::Case(new_predicates, Box::new(default_eval))
        }

        Expr::And(l, r) => {
            let l = partial_eval(l)?;
            let r = partial_eval(r)?;

            match (&l, &r) {
                (Expr::Literal(Value::Bool(false)), _) | (_, Expr::Literal(Value::Bool(false))) => {
                    Expr::Literal(Value::Bool(false))
                }
                (Expr::Literal(Value::Bool(true)), _) => r,
                (_, Expr::Literal(Value::Bool(true))) => l,
                _ if l == r => l,
                (a, Expr::Or(or_l, or_r)) if a == or_l.as_ref() || a == or_r.as_ref() => l,
                (Expr::Or(or_l, or_r), a) if a == or_l.as_ref() || a == or_r.as_ref() => r,
                _ => {
                    use CmpOp::*;
                    if let (Some((f1, op1, v1)), Some((f2, op2, v2))) =
                        (extract_field_cmp(&l), extract_field_cmp(&r))
                        && f1 == f2
                        && let (Some(n1), Some(n2)) = (v1.as_f64(), v2.as_f64())
                    {
                        let is_contradiction = match (op1, op2) {
                            (Gt, Lt) | (Gt, Lte) | (Gte, Lt) => n1 >= n2,
                            (Gte, Lte) => n1 > n2,
                            (Lt, Gt) | (Lt, Gte) | (Lte, Gt) => n2 >= n1,
                            (Lte, Gte) => n2 > n1,
                            _ => false,
                        };
                        if is_contradiction {
                            return Ok(Expr::Literal(Value::Bool(false)));
                        }

                        match (op1, op2) {
                            (Gt, Gt) | (Gte, Gte) | (Gt, Gte) if n1 >= n2 => return Ok(l),
                            (Gt, Gt) | (Gte, Gte) | (Gte, Gt) if n2 >= n1 => return Ok(r),
                            (Lt, Lt) | (Lte, Lte) | (Lt, Lte) if n1 <= n2 => return Ok(l),
                            (Lt, Lt) | (Lte, Lte) | (Lte, Lt) if n2 <= n1 => return Ok(r),
                            _ => {}
                        }
                    }
                    Expr::And(Box::new(l), Box::new(r))
                }
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
                _ if l == r => l,
                (a, Expr::And(and_l, and_r)) if a == and_l.as_ref() || a == and_r.as_ref() => l,
                (Expr::And(and_l, and_r), a) if a == and_l.as_ref() || a == and_r.as_ref() => r,
                _ => {
                    if let (Some(f1), Some(f2)) = (extract_or_in_field(&l), extract_or_in_field(&r))
                        && f1 == f2
                    {
                        let mut values = vec![];
                        let reconstructed = Expr::Or(Box::new(l.clone()), Box::new(r.clone()));
                        if collect_or_equalities(&reconstructed, f1, &mut values)
                            && values.len() >= 2
                        {
                            let mut deduped = Vec::new();
                            for v in values {
                                if !deduped.contains(&v) {
                                    deduped.push(v);
                                }
                            }
                            return Ok(Expr::In(
                                Box::new(Expr::Field(f1.clone())),
                                deduped.into_iter().map(Expr::Literal).collect(),
                            ));
                        }
                    }
                    Expr::Or(Box::new(l), Box::new(r))
                }
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
        Expr::Extract(regex, group, source) => Expr::Extract(
            Box::new(partial_eval(regex)?),
            Box::new(partial_eval(group)?),
            Box::new(partial_eval(source)?),
        ),
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
            vec![Expr::Literal(Value::Int(1)), Expr::Literal(Value::Int(1))],
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
        // NOT(x > 1) -> x <= 1
        let expr = Expr::Not(Box::new(Expr::Gt(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Lte(_, _)),
            "Expected Lte, got {:?}",
            result
        );

        // NOT(x >= 1) -> x < 1
        let expr = Expr::Not(Box::new(Expr::Gte(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Lt(_, _)),
            "Expected Lt, got {:?}",
            result
        );

        // NOT(x < 1) -> x >= 1
        let expr = Expr::Not(Box::new(Expr::Lt(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Gte(_, _)),
            "Expected Gte, got {:?}",
            result
        );

        // NOT(x <= 1) -> x > 1
        let expr = Expr::Not(Box::new(Expr::Lte(
            Box::new(Expr::Field(field_unwrap!("x"))),
            Box::new(Expr::Literal(Value::Int(1))),
        )));
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Gt(_, _)),
            "Expected Gt, got {:?}",
            result
        );
    }

    #[test]
    fn test_case_true_condition_returns_then() {
        // CASE WHEN true THEN "yes" WHEN x > 1 THEN "maybe" ELSE "no" -> "yes"
        let expr = Expr::Case(
            vec![
                (
                    Expr::Literal(Value::Bool(true)),
                    Expr::Literal(Value::from("yes")),
                ),
                (
                    Expr::Gt(
                        Box::new(Expr::Field(field_unwrap!("x"))),
                        Box::new(Expr::Literal(Value::Int(1))),
                    ),
                    Expr::Literal(Value::from("maybe")),
                ),
            ],
            Box::new(Expr::Literal(Value::from("no"))),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Literal(Value::String(ref s)) if s == "yes"),
            "Expected 'yes', got {:?}",
            result
        );
    }

    #[test]
    fn test_case_false_condition_skipped() {
        // CASE WHEN false THEN "no" WHEN x > 1 THEN "yes" ELSE "default" -> CASE WHEN x > 1 THEN "yes" ELSE "default"
        let expr = Expr::Case(
            vec![
                (
                    Expr::Literal(Value::Bool(false)),
                    Expr::Literal(Value::from("no")),
                ),
                (
                    Expr::Gt(
                        Box::new(Expr::Field(field_unwrap!("x"))),
                        Box::new(Expr::Literal(Value::Int(1))),
                    ),
                    Expr::Literal(Value::from("yes")),
                ),
            ],
            Box::new(Expr::Literal(Value::from("default"))),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Case(preds, _) => {
                assert_eq!(preds.len(), 1, "Expected 1 predicate, got {:?}", preds);
                assert!(matches!(&preds[0].0, Expr::Gt(_, _)));
            }
            _ => panic!("Expected Case, got {:?}", result),
        }
    }

    #[test]
    fn test_case_empty_returns_default() {
        // CASE WHEN false THEN "no" ELSE "default" -> "default"
        let expr = Expr::Case(
            vec![(
                Expr::Literal(Value::Bool(false)),
                Expr::Literal(Value::from("no")),
            )],
            Box::new(Expr::Literal(Value::from("default"))),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Literal(Value::String(ref s)) if s == "default"),
            "Expected 'default', got {:?}",
            result
        );
    }

    #[test]
    fn test_case_bool_true_false_to_predicate() {
        // CASE WHEN x > 1 THEN true ELSE false -> x > 1
        let expr = Expr::Case(
            vec![(
                Expr::Gt(
                    Box::new(Expr::Field(field_unwrap!("x"))),
                    Box::new(Expr::Literal(Value::Int(1))),
                ),
                Expr::Literal(Value::Bool(true)),
            )],
            Box::new(Expr::Literal(Value::Bool(false))),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Gt(_, _)),
            "Expected Gt, got {:?}",
            result
        );
    }

    #[test]
    fn test_case_bool_false_true_to_not_predicate() {
        // CASE WHEN x > 1 THEN false ELSE true -> NOT(x > 1) -> x <= 1
        let expr = Expr::Case(
            vec![(
                Expr::Gt(
                    Box::new(Expr::Field(field_unwrap!("x"))),
                    Box::new(Expr::Literal(Value::Int(1))),
                ),
                Expr::Literal(Value::Bool(false)),
            )],
            Box::new(Expr::Literal(Value::Bool(true))),
        );
        let result = partial_eval(&expr).unwrap();
        // NOT(x > 1) gets simplified to x <= 1
        assert!(
            matches!(result, Expr::Lte(_, _)),
            "Expected Lte, got {:?}",
            result
        );
    }

    #[test]
    fn test_absorption_and_or() {
        // a AND (a OR b) -> a
        let a = Expr::Field(field_unwrap!("a"));
        let b = Expr::Field(field_unwrap!("b"));
        let expr = Expr::And(
            Box::new(a.clone()),
            Box::new(Expr::Or(Box::new(a.clone()), Box::new(b))),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Field(ref f) if f.to_string() == "a"),
            "Expected field a, got {:?}",
            result
        );
    }

    #[test]
    fn test_absorption_or_and() {
        // a OR (a AND b) -> a
        let a = Expr::Field(field_unwrap!("a"));
        let b = Expr::Field(field_unwrap!("b"));
        let expr = Expr::Or(
            Box::new(a.clone()),
            Box::new(Expr::And(Box::new(a.clone()), Box::new(b))),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Field(ref f) if f.to_string() == "a"),
            "Expected field a, got {:?}",
            result
        );
    }

    #[test]
    fn test_absorption_commutative() {
        // (a OR b) AND a -> a
        let a = Expr::Field(field_unwrap!("a"));
        let b = Expr::Field(field_unwrap!("b"));
        let expr = Expr::And(
            Box::new(Expr::Or(Box::new(a.clone()), Box::new(b.clone()))),
            Box::new(a.clone()),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Field(ref f) if f.to_string() == "a"),
            "Expected field a, got {:?}",
            result
        );

        // (a AND b) OR a -> a
        let expr = Expr::Or(
            Box::new(Expr::And(Box::new(a.clone()), Box::new(b))),
            Box::new(a),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Field(ref f) if f.to_string() == "a"),
            "Expected field a, got {:?}",
            result
        );
    }

    #[test]
    fn test_or_equality_to_in() {
        // x == 1 OR x == 2 -> x IN (1, 2)
        let expr = Expr::Or(
            Box::new(Expr::Eq(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(1))),
            )),
            Box::new(Expr::Eq(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(2))),
            )),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::In(field, items) => {
                assert!(matches!(*field, Expr::Field(_)));
                assert_eq!(items.len(), 2);
                assert!(matches!(&items[0], Expr::Literal(Value::Int(1))));
                assert!(matches!(&items[1], Expr::Literal(Value::Int(2))));
            }
            _ => panic!("Expected IN expression, got {:?}", result),
        }
    }

    #[test]
    fn test_or_equality_chain_to_in() {
        // x == 1 OR x == 2 OR x == 3 -> x IN (1, 2, 3)
        let expr = Expr::Or(
            Box::new(Expr::Or(
                Box::new(Expr::Eq(
                    Box::new(Expr::Field(field_unwrap!("x"))),
                    Box::new(Expr::Literal(Value::Int(1))),
                )),
                Box::new(Expr::Eq(
                    Box::new(Expr::Field(field_unwrap!("x"))),
                    Box::new(Expr::Literal(Value::Int(2))),
                )),
            )),
            Box::new(Expr::Eq(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(3))),
            )),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::In(field, items) => {
                assert!(matches!(*field, Expr::Field(_)));
                assert_eq!(items.len(), 3);
            }
            _ => panic!("Expected IN expression, got {:?}", result),
        }
    }

    #[test]
    fn test_or_different_fields_unchanged() {
        // x == 1 OR y == 2 -> unchanged
        let expr = Expr::Or(
            Box::new(Expr::Eq(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(1))),
            )),
            Box::new(Expr::Eq(
                Box::new(Expr::Field(field_unwrap!("y"))),
                Box::new(Expr::Literal(Value::Int(2))),
            )),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Or(_, _)),
            "Expected Or unchanged, got {:?}",
            result
        );
    }

    #[test]
    fn test_range_contradiction() {
        // x > 5 AND x < 3 -> false
        let expr = Expr::And(
            Box::new(Expr::Gt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(5))),
            )),
            Box::new(Expr::Lt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(3))),
            )),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Literal(Value::Bool(false))),
            "Expected false, got {:?}",
            result
        );
    }

    #[test]
    fn test_range_subsumption_gt() {
        // x > 5 AND x > 3 -> x > 5
        let expr = Expr::And(
            Box::new(Expr::Gt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(5))),
            )),
            Box::new(Expr::Gt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(3))),
            )),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Gt(_, r) => {
                assert!(
                    matches!(*r, Expr::Literal(Value::Int(5))),
                    "Expected > 5, got {:?}",
                    r
                );
            }
            _ => panic!("Expected Gt, got {:?}", result),
        }
    }

    #[test]
    fn test_range_subsumption_lt() {
        // x < 3 AND x < 5 -> x < 3
        let expr = Expr::And(
            Box::new(Expr::Lt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(3))),
            )),
            Box::new(Expr::Lt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(5))),
            )),
        );
        let result = partial_eval(&expr).unwrap();
        match result {
            Expr::Lt(_, r) => {
                assert!(
                    matches!(*r, Expr::Literal(Value::Int(3))),
                    "Expected < 3, got {:?}",
                    r
                );
            }
            _ => panic!("Expected Lt, got {:?}", result),
        }
    }

    #[test]
    fn test_range_valid_unchanged() {
        // x > 3 AND x < 5 -> unchanged (valid range)
        let expr = Expr::And(
            Box::new(Expr::Gt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(3))),
            )),
            Box::new(Expr::Lt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(5))),
            )),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::And(_, _)),
            "Expected And unchanged, got {:?}",
            result
        );
    }

    #[test]
    fn test_range_subsumption_mixed_gte_gt() {
        // x >= 5 AND x > 5 -> x > 5 (gt is stricter)
        let expr = Expr::And(
            Box::new(Expr::Gte(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(5))),
            )),
            Box::new(Expr::Gt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(5))),
            )),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Gt(_, _)),
            "Expected Gt, got {:?}",
            result
        );
    }

    #[test]
    fn test_range_subsumption_mixed_lte_lt() {
        // x <= 5 AND x < 5 -> x < 5 (lt is stricter)
        let expr = Expr::And(
            Box::new(Expr::Lte(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(5))),
            )),
            Box::new(Expr::Lt(
                Box::new(Expr::Field(field_unwrap!("x"))),
                Box::new(Expr::Literal(Value::Int(5))),
            )),
        );
        let result = partial_eval(&expr).unwrap();
        assert!(
            matches!(result, Expr::Lt(_, _)),
            "Expected Lt, got {:?}",
            result
        );
    }
}
