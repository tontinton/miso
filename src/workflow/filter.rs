use std::{collections::BTreeMap, sync::Arc};

use async_stream::try_stream;
use color_eyre::{
    eyre::{bail, Context},
    Result,
};
use cranelift::{
    module::{Linkage, Module},
    prelude::{AbiParam, FunctionBuilder, FunctionBuilderContext, InstBuilder, IntCC, Variable},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

use crate::{
    impl_and, impl_or,
    log::{LogStream, LogTryStream},
    thread_pool::run_on_thread_pool,
};

use super::jit::{
    build_args, contains, ends_with, jit_thread_pool, new_jit_module, run_code, starts_with, Arg,
    ArgKind, Compiler, FlattenedFields, BOOL_TYPE,
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FilterAst {
    Id(String),                                 // field
    Lit(serde_json::Value),                     // a json literal
    Exists(String),                             // field exists
    Or(Vec<FilterAst>),                         // ||
    And(Vec<FilterAst>),                        // &&
    Not(Box<FilterAst>),                        // !
    Contains(Box<FilterAst>, Box<FilterAst>),   // right in left
    StartsWith(Box<FilterAst>, Box<FilterAst>), // left starts with right
    EndsWith(Box<FilterAst>, Box<FilterAst>),   // left ends with right
    Eq(Box<FilterAst>, Box<FilterAst>),         // ==
    Ne(Box<FilterAst>, Box<FilterAst>),         // !=
    Gt(Box<FilterAst>, Box<FilterAst>),         // >
    Gte(Box<FilterAst>, Box<FilterAst>),        // >=
    Lt(Box<FilterAst>, Box<FilterAst>),         // <
    Lte(Box<FilterAst>, Box<FilterAst>),        // <=
}

impl FilterAst {
    fn _fields(&self, out: &mut Vec<String>) {
        match self {
            FilterAst::Id(f) | FilterAst::Exists(f) => out.push(f.clone()),
            FilterAst::Or(exprs) | FilterAst::And(exprs) => {
                for e in exprs {
                    e._fields(out);
                }
            }
            FilterAst::Not(e) => {
                e._fields(out);
            }
            FilterAst::Contains(lhs, rhs)
            | FilterAst::StartsWith(lhs, rhs)
            | FilterAst::EndsWith(lhs, rhs)
            | FilterAst::Eq(lhs, rhs)
            | FilterAst::Ne(lhs, rhs)
            | FilterAst::Gt(lhs, rhs)
            | FilterAst::Gte(lhs, rhs)
            | FilterAst::Lt(lhs, rhs)
            | FilterAst::Lte(lhs, rhs) => {
                lhs._fields(out);
                rhs._fields(out);
            }
            FilterAst::Lit(..) => {}
        };
    }

    fn fields(&self) -> Vec<String> {
        let mut out = Vec::new();
        self._fields(&mut out);
        out.sort();
        out.dedup();
        out
    }
}

fn filter_ast_to_jit(ast: &FilterAst) -> Result<(usize, Vec<String>)> {
    // The module, with the jit backend, which manages the JIT'd functions.
    let mut module = new_jit_module()?;

    // The main Cranelift context, which holds the state for codegen. Cranelift
    // separates this from `Module` to allow for parallel compilation, with a
    // context per thread, though we don't utilize this, as we only compile 1 function.
    let mut ctx = module.make_context();

    // The function builder context, which is reused across multiple
    // FunctionBuilder instances.
    let mut builder_context = FunctionBuilderContext::new();

    // Numbers are sized to machine's pointer size.
    let ptr_type = module.target_config().pointer_type();
    let ptr_size = module.target_config().pointer_bytes();

    // Pointer to start of args.
    ctx.func.signature.params.push(AbiParam::new(ptr_type));

    ctx.func.signature.returns.push(AbiParam::new(BOOL_TYPE));

    let mut builder = FunctionBuilder::new(&mut ctx.func, &mut builder_context);

    let entry_block = builder.create_block();

    builder.append_block_params_for_function_params(entry_block);
    builder.switch_to_block(entry_block);
    builder.seal_block(entry_block);

    let args_var = Variable::from_u32(0);
    builder.declare_var(args_var, ptr_type);
    builder.def_var(args_var, builder.block_params(entry_block)[0]);

    let fields = ast.fields();

    let mut variables = BTreeMap::new();
    for (i, name) in fields.iter().enumerate() {
        variables.insert(name.clone(), i as i64);
    }

    let mut compiler = FilterCompiler {
        c: Compiler {
            ptr_type,
            ptr_size,
            builder,
            args_var,
            variables,
            module: &mut module,
        },
    };

    let Arg::Bool(return_value) = compiler.compile(ast)? else {
        bail!("compiled filter function doesn't return bool");
    };

    compiler.c.builder.ins().return_(&[return_value]);
    compiler.c.builder.finalize();

    let id = module.declare_function("main", Linkage::Export, &ctx.func.signature)?;
    module.define_function(id, &mut ctx)?;

    module.clear_context(&mut ctx);

    module.finalize_definitions().context("failed to compile")?;

    let jit_func_addr = module.get_finalized_function(id);
    Ok((jit_func_addr as usize, fields))
}

struct FilterCompiler<'a> {
    c: Compiler<'a>,
}

impl FilterCompiler<'_> {
    fn compile(&mut self, ast: &FilterAst) -> Result<Arg> {
        match ast {
            FilterAst::Id(name) => {
                let (ident_type, ident_value) = self.c.ident(name)?;
                Ok(Arg::Unresolved(ident_type, ident_value))
            }
            FilterAst::Lit(value) => self.c.literal(value),
            FilterAst::And(exprs) => {
                impl_and!(self.c, exprs, |expr| self.compile(expr))
            }
            FilterAst::Or(exprs) => {
                impl_or!(self.c, exprs, |expr| self.compile(expr))
            }
            FilterAst::Not(expr) => {
                let arg = self.compile(expr)?;
                Ok(self.c.not_arg(arg))
            }
            FilterAst::Exists(field) => {
                let (field_type, _) = self.c.ident(field)?;
                Ok(Arg::Bool(self.c.cmp_type_to_kind(
                    IntCC::NotEqual,
                    field_type,
                    ArgKind::_NotExists,
                )))
            }
            FilterAst::Contains(lhs, rhs) => {
                self.compile_call_indirect_on_two_strings(lhs, rhs, contains)
            }
            FilterAst::StartsWith(lhs, rhs) => {
                self.compile_call_indirect_on_two_strings(lhs, rhs, starts_with)
            }
            FilterAst::EndsWith(lhs, rhs) => {
                self.compile_call_indirect_on_two_strings(lhs, rhs, ends_with)
            }
            FilterAst::Eq(l, r) => self.compile_cmp(IntCC::Equal, l, r),
            FilterAst::Ne(l, r) => self.compile_cmp(IntCC::NotEqual, l, r),
            FilterAst::Lt(l, r) => self.compile_cmp(IntCC::SignedLessThan, l, r),
            FilterAst::Lte(l, r) => self.compile_cmp(IntCC::SignedLessThanOrEqual, l, r),
            FilterAst::Gt(l, r) => self.compile_cmp(IntCC::SignedGreaterThan, l, r),
            FilterAst::Gte(l, r) => self.compile_cmp(IntCC::SignedGreaterThanOrEqual, l, r),
        }
    }

    fn compile_call_indirect_on_two_strings(
        &mut self,
        lhs_ast: &FilterAst,
        rhs_ast: &FilterAst,
        func: fn(*const u8, usize, *const u8, usize) -> bool,
    ) -> Result<Arg> {
        let lhs = self.compile(lhs_ast)?;
        let rhs = self.compile(rhs_ast)?;
        self.c.call_indirect_on_two_strings(lhs, rhs, func)
    }

    fn compile_cmp(&mut self, cmp: IntCC, lhs_ast: &FilterAst, rhs_ast: &FilterAst) -> Result<Arg> {
        let lhs = self.compile(lhs_ast)?;
        let rhs = self.compile(rhs_ast)?;
        self.c.cmp(cmp, lhs, rhs)
    }
}

pub async fn filter_stream(ast: FilterAst, mut input_stream: LogStream) -> Result<LogTryStream> {
    let ast = Arc::new(ast);
    let ast_clone = ast.clone();
    let (jit_fn_ptr, fields) =
        run_on_thread_pool(jit_thread_pool(), move || filter_ast_to_jit(&ast_clone))
            .await
            .context("run on jit thread pool")?
            .context("filter jit compile")?;

    let flattend_fields = FlattenedFields::from_nested(fields);

    Ok(Box::pin(try_stream! {
        // The generated JIT code references strings from inside the AST.
        let _ast_keepalive = ast;

        while let Some(log) = input_stream.next().await {
            let Some((args, _allocs)) = build_args(&log, flattend_fields.iter()).await else {
                continue;
            };

            let keep: bool = unsafe { run_code(jit_fn_ptr as *const (), &args) };
            if keep {
                yield log;
            }
        }
    }))
}
