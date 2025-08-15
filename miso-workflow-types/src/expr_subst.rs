use std::cell::RefCell;

use hashbrown::HashMap;

use crate::{expr::Expr, field::Field};

type RenameHook<'a> = RefCell<Box<dyn FnMut(&Field, &Field) + 'a>>;
type LiteralHook<'a> = RefCell<Box<dyn FnMut(&Field, &serde_json::Value) + 'a>>;

pub struct SubstituteExpr<'a> {
    renames: &'a HashMap<Field, Field>,
    literals: &'a HashMap<Field, serde_json::Value>,
    rename_hook: Option<RenameHook<'a>>,
    literal_hook: Option<LiteralHook<'a>>,
}

impl<'a> SubstituteExpr<'a> {
    pub fn new(
        renames: &'a HashMap<Field, Field>,
        literals: &'a HashMap<Field, serde_json::Value>,
    ) -> Self {
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
        F: FnMut(&Field, &serde_json::Value) + 'a,
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
                    let v = serde_json::Value::Bool(true);
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

            Expr::Bin(l, r) => Expr::Bin(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Or(l, r) => Expr::Or(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::And(l, r) => Expr::And(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Contains(l, r) => {
                Expr::Contains(Box::new(self.subst(*l)), Box::new(self.subst(*r)))
            }
            Expr::StartsWith(l, r) => {
                Expr::StartsWith(Box::new(self.subst(*l)), Box::new(self.subst(*r)))
            }
            Expr::EndsWith(l, r) => {
                Expr::EndsWith(Box::new(self.subst(*l)), Box::new(self.subst(*r)))
            }
            Expr::Has(l, r) => Expr::Has(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::HasCs(l, r) => Expr::HasCs(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Eq(l, r) => Expr::Eq(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Ne(l, r) => Expr::Ne(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Gt(l, r) => Expr::Gt(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Gte(l, r) => Expr::Gte(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Lt(l, r) => Expr::Lt(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Lte(l, r) => Expr::Lte(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Mul(l, r) => Expr::Mul(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Div(l, r) => Expr::Div(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Plus(l, r) => Expr::Plus(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
            Expr::Minus(l, r) => Expr::Minus(Box::new(self.subst(*l)), Box::new(self.subst(*r))),
        }
    }
}
