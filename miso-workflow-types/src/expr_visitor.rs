use crate::{expr::Expr, field::Field, value::Value};

macro_rules! transform_binop {
    ($binop:expr, $self:expr, $l:expr, $r:expr) => {
        $self.transform_binop(*$l, *$r, |a, b| $binop(Box::new(a), Box::new(b)))
    };
}

/// Transform expressions using the visitor pattern.
pub trait ExprTransformer {
    fn transform_field(&self, field: Field) -> Expr {
        Expr::Field(field)
    }

    fn transform_exists(&self, field: Field) -> Expr {
        Expr::Exists(field)
    }

    fn transform_literal(&self, value: Value) -> Expr {
        Expr::Literal(value)
    }

    fn transform_in(&self, expr: Expr, arr: Vec<Expr>) -> Expr {
        Expr::In(
            Box::new(self.transform(expr)),
            arr.into_iter().map(|a| self.transform(a)).collect(),
        )
    }

    fn transform_case(&self, predicates: Vec<(Expr, Expr)>, default: Expr) -> Expr {
        Expr::Case(
            predicates
                .into_iter()
                .map(|(p, t)| (self.transform(p), self.transform(t)))
                .collect(),
            Box::new(self.transform(default)),
        )
    }

    fn transform_binop<F>(&self, left: Expr, right: Expr, rebuild: F) -> Expr
    where
        F: Fn(Expr, Expr) -> Expr,
    {
        rebuild(self.transform(left), self.transform(right))
    }

    fn transform(&self, expr: Expr) -> Expr {
        match expr {
            Expr::Field(f) => self.transform_field(f),
            Expr::Literal(v) => self.transform_literal(v),
            Expr::Exists(f) => self.transform_exists(f),
            Expr::In(e, arr) => self.transform_in(*e, arr),
            Expr::Case(predicates, default) => self.transform_case(predicates, *default),

            Expr::Not(e) => Expr::Not(Box::new(self.transform(*e))),
            Expr::Cast(ty, e) => Expr::Cast(ty, Box::new(self.transform(*e))),

            Expr::Bin(l, r) => transform_binop!(Expr::Bin, self, l, r),
            Expr::Or(l, r) => transform_binop!(Expr::Or, self, l, r),
            Expr::And(l, r) => transform_binop!(Expr::And, self, l, r),
            Expr::Contains(l, r) => transform_binop!(Expr::Contains, self, l, r),
            Expr::StartsWith(l, r) => transform_binop!(Expr::StartsWith, self, l, r),
            Expr::EndsWith(l, r) => transform_binop!(Expr::EndsWith, self, l, r),
            Expr::Has(l, r) => transform_binop!(Expr::Has, self, l, r),
            Expr::HasCs(l, r) => transform_binop!(Expr::HasCs, self, l, r),
            Expr::Eq(l, r) => transform_binop!(Expr::Eq, self, l, r),
            Expr::Ne(l, r) => transform_binop!(Expr::Ne, self, l, r),
            Expr::Gt(l, r) => transform_binop!(Expr::Gt, self, l, r),
            Expr::Gte(l, r) => transform_binop!(Expr::Gte, self, l, r),
            Expr::Lt(l, r) => transform_binop!(Expr::Lt, self, l, r),
            Expr::Lte(l, r) => transform_binop!(Expr::Lte, self, l, r),
            Expr::Mul(l, r) => transform_binop!(Expr::Mul, self, l, r),
            Expr::Div(l, r) => transform_binop!(Expr::Div, self, l, r),
            Expr::Plus(l, r) => transform_binop!(Expr::Plus, self, l, r),
            Expr::Minus(l, r) => transform_binop!(Expr::Minus, self, l, r),
        }
    }
}
