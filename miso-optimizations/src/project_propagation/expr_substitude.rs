use std::cell::RefCell;

use hashbrown::HashMap;
use miso_workflow_types::{expr::Expr, expr_visitor::ExprTransformer, field::Field, value::Value};

type RenameHook<'a> = RefCell<Box<dyn FnMut(Field, &Field) + 'a>>;
type LiteralHook<'a> = RefCell<Box<dyn FnMut(Field, &Value) + 'a>>;

static EMPTY_EXPRS: std::sync::LazyLock<HashMap<Field, Expr>> =
    std::sync::LazyLock::new(HashMap::new);

pub struct ExprSubstitute<'a> {
    renames: &'a HashMap<Field, Field>,
    literals: &'a HashMap<Field, Value>,
    exprs: &'a HashMap<Field, Expr>,
    rename_hook: Option<RenameHook<'a>>,
    literal_hook: Option<LiteralHook<'a>>,
}

impl<'a> ExprSubstitute<'a> {
    pub fn new(renames: &'a HashMap<Field, Field>, literals: &'a HashMap<Field, Value>) -> Self {
        Self::with_exprs(renames, literals, &EMPTY_EXPRS)
    }

    pub fn with_exprs(
        renames: &'a HashMap<Field, Field>,
        literals: &'a HashMap<Field, Value>,
        exprs: &'a HashMap<Field, Expr>,
    ) -> Self {
        Self {
            renames,
            literals,
            exprs,
            rename_hook: None,
            literal_hook: None,
        }
    }

    /// Add a hook that will be called whenever a field is renamed.
    /// The hook receives (original_field, new_field).
    pub fn with_rename_hook<F>(mut self, hook: F) -> Self
    where
        F: FnMut(Field, &Field) + 'a,
    {
        self.rename_hook = Some(RefCell::new(Box::new(hook)));
        self
    }

    /// Add a hook that will be called whenever a field is replaced with a literal.
    /// The hook receives (original_field, literal_value).
    pub fn with_literal_hook<F>(mut self, hook: F) -> Self
    where
        F: FnMut(Field, &Value) + 'a,
    {
        self.literal_hook = Some(RefCell::new(Box::new(hook)));
        self
    }

    /// Perform the substitution on the given expression.
    pub fn substitute(&self, expr: Expr) -> Expr {
        self.transform(expr)
    }
}

impl<'a> ExprTransformer for ExprSubstitute<'a> {
    fn transform_field(&self, field: Field) -> Expr {
        if let Some(v) = self.literals.get(&field) {
            if let Some(ref hook) = self.literal_hook {
                hook.borrow_mut()(field, v);
            }
            Expr::Literal(v.clone())
        } else if let Some(to) = self.renames.get(&field) {
            if let Some(ref hook) = self.rename_hook {
                hook.borrow_mut()(field, to);
            }
            Expr::Field(to.clone())
        } else if let Some(expr) = self.exprs.get(&field) {
            self.transform(expr.clone())
        } else {
            Expr::Field(field)
        }
    }

    fn transform_exists(&self, field: Field) -> Expr {
        if self.literals.contains_key(&field) {
            // exists(literal) is always true.
            let v = Value::Bool(true);
            if let Some(ref hook) = self.literal_hook {
                hook.borrow_mut()(field, &v);
            }
            Expr::Literal(v)
        } else if let Some(to) = self.renames.get(&field) {
            if let Some(ref hook) = self.rename_hook {
                hook.borrow_mut()(field, to);
            }
            Expr::Exists(to.clone())
        } else {
            Expr::Exists(field)
        }
    }
}
