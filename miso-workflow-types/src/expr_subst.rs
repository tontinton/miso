use std::cell::RefCell;

use hashbrown::HashMap;

use crate::{expr::Expr, field::Field, value::Value};

macro_rules! subst_binop {
    ($binop:expr, $self:expr, $l:expr, $r:expr) => {
        $binop(Box::new($self.subst(*$l)), Box::new($self.subst(*$r)))
    };
}

type RenameHook<'a> = RefCell<Box<dyn FnMut(&Field, &Field) + 'a>>;
type LiteralHook<'a> = RefCell<Box<dyn FnMut(&Field, &Value) + 'a>>;

pub struct SubstituteExpr<'a> {
    renames: &'a HashMap<Field, Field>,
    literals: &'a HashMap<Field, Value>,
    rename_hook: Option<RenameHook<'a>>,
    literal_hook: Option<LiteralHook<'a>>,
}

impl<'a> SubstituteExpr<'a> {
    pub fn new(renames: &'a HashMap<Field, Field>, literals: &'a HashMap<Field, Value>) -> Self {
        Self {
            renames,
            literals,
            rename_hook: None,
            literal_hook: None,
        }
    }

    /// Add a hook that will be called whenever a field is renamed.
    /// The hook receives (original_field, new_field).
    pub fn with_rename_hook<F>(mut self, hook: F) -> Self
    where
        F: FnMut(&Field, &Field) + 'a,
    {
        self.rename_hook = Some(RefCell::new(Box::new(hook)));
        self
    }

    /// Add a hook that will be called whenever a field is replaced with a literal.
    /// The hook receives (original_field, literal_value).
    pub fn with_literal_hook<F>(mut self, hook: F) -> Self
    where
        F: FnMut(&Field, &Value) + 'a,
    {
        self.literal_hook = Some(RefCell::new(Box::new(hook)));
        self
    }

    /// Perform the substitution on the given expression.
    pub fn substitute(&self, expr: Expr) -> Expr {
        self.subst(expr)
    }

    fn subst(&self, expr: Expr) -> Expr {
        match expr {
            Expr::Field(ref f) => {
                if let Some(v) = self.literals.get(f) {
                    if let Some(ref hook) = self.literal_hook {
                        hook.borrow_mut()(f, v);
                    }
                    Expr::Literal(v.clone())
                } else if let Some(to) = self.renames.get(f) {
                    if let Some(ref hook) = self.rename_hook {
                        hook.borrow_mut()(f, to);
                    }
                    Expr::Field(to.clone())
                } else {
                    expr
                }
            }
            Expr::Literal(_) => expr,
            Expr::Exists(ref f) => {
                if self.literals.contains_key(f) {
                    // exists(literal) is always true.
                    let v = Value::Bool(true);
                    if let Some(ref hook) = self.literal_hook {
                        hook.borrow_mut()(f, &v);
                    }
                    Expr::Literal(v)
                } else if let Some(to) = self.renames.get(f) {
                    if let Some(ref hook) = self.rename_hook {
                        hook.borrow_mut()(f, to);
                    }
                    Expr::Exists(to.clone())
                } else {
                    expr
                }
            }

            Expr::Not(e) => Expr::Not(Box::new(self.subst(*e))),
            Expr::Cast(ty, e) => Expr::Cast(ty, Box::new(self.subst(*e))),

            Expr::In(e, arr) => Expr::In(
                Box::new(self.subst(*e)),
                arr.into_iter().map(|a| self.subst(a)).collect(),
            ),

            Expr::Bin(l, r) => subst_binop!(Expr::Bin, self, l, r),
            Expr::Or(l, r) => subst_binop!(Expr::Or, self, l, r),
            Expr::And(l, r) => subst_binop!(Expr::And, self, l, r),
            Expr::Contains(l, r) => subst_binop!(Expr::Contains, self, l, r),
            Expr::StartsWith(l, r) => subst_binop!(Expr::StartsWith, self, l, r),
            Expr::EndsWith(l, r) => subst_binop!(Expr::EndsWith, self, l, r),
            Expr::Has(l, r) => subst_binop!(Expr::Has, self, l, r),
            Expr::HasCs(l, r) => subst_binop!(Expr::HasCs, self, l, r),
            Expr::Eq(l, r) => subst_binop!(Expr::Eq, self, l, r),
            Expr::Ne(l, r) => subst_binop!(Expr::Ne, self, l, r),
            Expr::Gt(l, r) => subst_binop!(Expr::Gt, self, l, r),
            Expr::Gte(l, r) => subst_binop!(Expr::Gte, self, l, r),
            Expr::Lt(l, r) => subst_binop!(Expr::Lt, self, l, r),
            Expr::Lte(l, r) => subst_binop!(Expr::Lte, self, l, r),
            Expr::Mul(l, r) => subst_binop!(Expr::Mul, self, l, r),
            Expr::Div(l, r) => subst_binop!(Expr::Div, self, l, r),
            Expr::Plus(l, r) => subst_binop!(Expr::Plus, self, l, r),
            Expr::Minus(l, r) => subst_binop!(Expr::Minus, self, l, r),
        }
    }
}
