use miso_workflow_types::{expr::Expr, expr_visitor::ExprTransformer};

/// Replaces case into ORs (better chance of being pushdown).
///
/// Example:
///   case(id > 20, 100, id > 10, 50, 0) * 5 < 10
/// =>
///   (id > 20 and (100 * 5 < 10))
///   or (not(id > 20) and (id > 10 and (50 * 5 < 10)))
///   or (not(id > 20) and not(id > 10) and (0 * 5 < 10))
pub(crate) fn case_transform(expr: Expr) -> Expr {
    let lifted = LiftContextIntoCaseTransformer.transform(expr);
    CaseToOrTransformer.transform(lifted)
}

/// Pass 1: push context into CASE branches.
struct LiftContextIntoCaseTransformer;

impl ExprTransformer for LiftContextIntoCaseTransformer {
    fn transform_binop<F>(&self, left: Expr, right: Expr, rebuild: F) -> Expr
    where
        F: Fn(Expr, Expr) -> Expr,
    {
        let left = self.transform(left);
        let right = self.transform(right);

        match (left, right) {
            (Expr::Case(preds, default), right) => Expr::Case(
                preds
                    .iter()
                    .map(|(p, v)| (p.clone(), self.transform(rebuild(v.clone(), right.clone()))))
                    .collect(),
                Box::new(self.transform(rebuild(*default, right))),
            ),
            (left, Expr::Case(preds, default)) => Expr::Case(
                preds
                    .iter()
                    .map(|(p, v)| (p.clone(), self.transform(rebuild(left.clone(), v.clone()))))
                    .collect(),
                Box::new(self.transform(rebuild(left, *default))),
            ),
            (left, right) => rebuild(left, right),
        }
    }
}

/// Pass 2: flatten CASE -> OR/AND structure.
struct CaseToOrTransformer;

impl ExprTransformer for CaseToOrTransformer {
    fn transform_case(&self, predicates: Vec<(Expr, Expr)>, default: Expr) -> Expr {
        self.case_to_or(
            predicates
                .into_iter()
                .map(|(pred, then)| (self.transform(pred), self.transform(then)))
                .collect(),
            self.transform(default),
        )
    }
}

impl CaseToOrTransformer {
    fn case_to_or(&self, predicates: Vec<(Expr, Expr)>, default: Expr) -> Expr {
        let mut result = None;
        let mut negated_predicates: Vec<Expr> = Vec::new();

        for (predicate, value) in predicates {
            let pred_transformed = self.transform(predicate);
            let value_transformed = self.transform(value);

            let mut condition = pred_transformed.clone();
            for neg_pred in &negated_predicates {
                condition = Expr::And(Box::new(neg_pred.clone()), Box::new(condition));
            }

            let branch = Expr::And(Box::new(condition), Box::new(value_transformed));

            result = Some(match result {
                None => branch,
                Some(prev) => Expr::Or(Box::new(prev), Box::new(branch)),
            });

            negated_predicates.push(Expr::Not(Box::new(pred_transformed)));
        }

        let mut default_condition = self.transform(default);
        for neg_pred in &negated_predicates {
            default_condition = Expr::And(Box::new(neg_pred.clone()), Box::new(default_condition));
        }

        match result {
            None => default_condition,
            Some(prev) => Expr::Or(Box::new(prev), Box::new(default_condition)),
        }
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use super::*;
    use crate::test_utils::{and, case, field_expr as field, gt, lit, lt, mul, not, or};

    #[test_case(
        // case(id > 10, 100, 50)
        case(vec![(gt(field("id"), lit(10)), lit(100))], lit(50)),
        or(
            and(gt(field("id"), lit(10)), lit(100)),
            and(not(gt(field("id"), lit(10))), lit(50)),
        );
        "simple_case_to_or"
    )]
    #[test_case(
        // case(id > 10, 100, 50) * 2
        mul(case(vec![(gt(field("id"), lit(10)), lit(100))], lit(50)), lit(2)),
        or(
            and(gt(field("id"), lit(10)), mul(lit(100), lit(2))),
            and(not(gt(field("id"), lit(10))), mul(lit(50), lit(2))),
        );
        "case_with_operation"
    )]
    #[test_case(
        // case(id > 20, 100, id > 10, 50, 0) * 5 < 10
        lt(
            mul(
                case(
                    vec![
                        (gt(field("id"), lit(20)), lit(100)),
                        (gt(field("id"), lit(10)), lit(50)),
                    ],
                    lit(0),
                ),
                lit(5),
            ),
            lit(10),
        ),
        or(
            or(
                and(gt(field("id"), lit(20)), lt(mul(lit(100), lit(5)), lit(10))),
                and(
                    and(not(gt(field("id"), lit(20))), gt(field("id"), lit(10))),
                    lt(mul(lit(50), lit(5)), lit(10)),
                ),
            ),
            and(
                not(gt(field("id"), lit(10))),
                and(not(gt(field("id"), lit(20))), lt(mul(lit(0), lit(5)), lit(10))),
            ),
        );
        "example_from_comment"
    )]
    #[test_case(
        // case(x > 5, 10, x > 3, 20, x > 1, 30, 40)
        case(
            vec![
                (gt(field("x"), lit(5)), lit(10)),
                (gt(field("x"), lit(3)), lit(20)),
                (gt(field("x"), lit(1)), lit(30)),
            ],
            lit(40),
        ),
        or(
            or(
                or(
                    and(gt(field("x"), lit(5)), lit(10)),
                    and(and(not(gt(field("x"), lit(5))), gt(field("x"), lit(3))), lit(20)),
                ),
                and(
                    and(
                        not(gt(field("x"), lit(3))),
                        and(not(gt(field("x"), lit(5))), gt(field("x"), lit(1))),
                    ),
                    lit(30),
                ),
            ),
            and(
                not(gt(field("x"), lit(1))),
                and(
                    not(gt(field("x"), lit(3))), and(not(gt(field("x"), lit(5))),
                    lit(40))
                ),
            ),
        );
        "multiple_predicates"
    )]
    #[test_case(
        // case(x > 5, case(y > 3, 10, 20), 30)
        case(
            vec![(
                gt(field("x"), lit(5)),
                case(vec![(gt(field("y"), lit(3)), lit(10))], lit(20)),
            )],
            lit(30),
        ),
        {
            let inner_case = or(
                and(gt(field("y"), lit(3)), lit(10)),
                and(not(gt(field("y"), lit(3))), lit(20)),
            );
            or(
                and(gt(field("x"), lit(5)), inner_case),
                and(not(gt(field("x"), lit(5))), lit(30)),
            )
        };
        "nested_case"
    )]
    #[test_case(
        // 5 * case(id > 10, 100, 50)
        mul(lit(5), case(vec![(gt(field("id"), lit(10)), lit(100))], lit(50))),
        or(
            and(gt(field("id"), lit(10)), mul(lit(5), lit(100))),
            and(not(gt(field("id"), lit(10))), mul(lit(5), lit(50))),
        );
        "case_on_right_side"
    )]
    #[test_case(
        // case(id > 20, 100, id > 10, 50, 0)
        case(
            vec![
                (gt(field("id"), lit(20)), lit(100)),
                (gt(field("id"), lit(10)), lit(50)),
            ],
            lit(0),
        ),
        or(
            or(
                and(gt(field("id"), lit(20)), lit(100)),
                and(
                    and(not(gt(field("id"), lit(20))), gt(field("id"), lit(10))),
                    lit(50),
                ),
            ),
            and(
                not(gt(field("id"), lit(10))),
                and(not(gt(field("id"), lit(20))), lit(0)),
            ),
        );
        "case_without_operations"
    )]
    fn case_transformer_tests(input: Expr, expected: Expr) {
        println!("input:\t\t{input}");
        println!("expected:\t{expected}");
        let result = case_transform(input);
        println!("result:\t\t{result}");
        assert_eq!(result, expected);
    }
}
