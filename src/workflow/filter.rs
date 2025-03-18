#![allow(clippy::type_complexity)]

use std::{collections::BTreeMap, sync::Arc};

use async_stream::try_stream;
use color_eyre::{
    eyre::{bail, Context, ContextCompat},
    Result,
};
use cranelift::{
    jit::JITModule,
    module::{Linkage, Module},
    prelude::{
        types, AbiParam, FunctionBuilder, FunctionBuilderContext, InstBuilder, IntCC, MemFlags,
        Signature, Value, Variable,
    },
};
use futures_util::StreamExt;
use kinded::Kinded;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use vrl::value::KeyString;

use crate::log::{Log, LogStream, LogTryStream};

use super::jit::{contains, new_jit_module, starts_with};

const BOOL_TYPE: types::Type = types::I8;

#[derive(Kinded, Debug, Copy, Clone)]
pub enum Arg {
    _Null,             // doesn't exist
    Int(Value),        // I64
    Str(Value, Value), // *u8, usize
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FilterAst {
    Or(Vec<FilterAst>),                                // ||
    And(Vec<FilterAst>),                               // &&
    Exists(/*field=*/ String),                         // field exists
    Contains(/*field=*/ String, /*word=*/ String),     // word in field
    StartsWith(/*field=*/ String, /*prefix=*/ String), // field starts with prefix
    Eq(/*field=*/ String, /*value=*/ String),          // ==
    Ne(/*field=*/ String, /*value=*/ String),          // !=
    Gt(/*field=*/ String, /*value=*/ String),          // >
    Gte(/*field=*/ String, /*value=*/ String),         // >=
    Lt(/*field=*/ String, /*value=*/ String),          // <
    Lte(/*field=*/ String, /*value=*/ String),         // <=
}

impl FilterAst {
    fn _fields(&self, out: &mut Vec<String>) {
        match self {
            FilterAst::Or(exprs) | FilterAst::And(exprs) => {
                for expr in exprs {
                    expr._fields(out);
                }
            }
            FilterAst::Exists(f)
            | FilterAst::Contains(f, _)
            | FilterAst::StartsWith(f, _)
            | FilterAst::Eq(f, _)
            | FilterAst::Ne(f, _)
            | FilterAst::Gt(f, _)
            | FilterAst::Gte(f, _)
            | FilterAst::Lt(f, _)
            | FilterAst::Lte(f, _) => {
                out.push(f.clone());
            }
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

    let mut compiler = Compiler {
        ptr_type,
        ptr_size,
        builder,
        args_var,
        variables,
        module: &mut module,
    };
    let return_value = compiler.compile(ast)?;

    compiler.builder.ins().return_(&[return_value]);
    compiler.builder.finalize();

    let id = module.declare_function("main", Linkage::Export, &ctx.func.signature)?;
    module.define_function(id, &mut ctx)?;

    module.clear_context(&mut ctx);

    module.finalize_definitions().context("failed to compile")?;

    let code = module.get_finalized_function(id);
    Ok((code as usize, fields))
}

struct Compiler<'a> {
    ptr_type: types::Type,
    ptr_size: u8,
    builder: FunctionBuilder<'a>,
    args_var: Variable,
    variables: BTreeMap<String, i64>,
    module: &'a mut JITModule,
}

impl Compiler<'_> {
    fn compile(&mut self, ast: &FilterAst) -> Result<Value> {
        match ast {
            FilterAst::And(exprs) => {
                let values_fns = exprs
                    .iter()
                    .map(|expr| |c: &mut Self| c.compile(expr))
                    .collect::<Vec<_>>();
                self.and(&values_fns)
            }
            FilterAst::Or(exprs) => {
                let values_fns = exprs
                    .iter()
                    .map(|expr| Box::new(|c: &mut Self| c.compile(expr)))
                    .collect::<Vec<_>>();
                self.or(&values_fns)
            }
            FilterAst::Exists(field) => {
                let (field_type, _) = self.ident(field)?;
                Ok(self.cmp_types(IntCC::NotEqual, field_type, ArgKind::_Null))
            }
            FilterAst::Contains(field, word) => {
                let (lhs_type, lhs) = self.ident(field)?;
                let rhs_arg = self.literal(word)?;

                let fns: Vec<Box<dyn Fn(&mut Self) -> Result<Value>>> = vec![
                    Box::new(|c: &mut Self| {
                        Ok(c.cmp_types(IntCC::Equal, lhs_type, rhs_arg.kind()))
                    }),
                    Box::new(|c: &mut Self| {
                        if let Arg::Str(rhs_ptr, rhs_len) = rhs_arg {
                            let (lhs_ptr, lhs_len) = c.load_str(lhs);
                            let args = [lhs_ptr, lhs_len, rhs_ptr, rhs_len];

                            let mut sig = c.module.make_signature();
                            for _ in 0..args.len() {
                                sig.params.push(AbiParam::new(c.ptr_type));
                            }
                            sig.returns.push(AbiParam::new(BOOL_TYPE));

                            c.call_indirect(sig, contains as *const (), &args)
                        } else {
                            Ok(c.builder.ins().iconst(BOOL_TYPE, 0))
                        }
                    }),
                ];
                Ok(self.and(&fns)?)
            }
            FilterAst::StartsWith(field, prefix) => {
                let (lhs_type, lhs) = self.ident(field)?;
                let rhs_arg = self.literal(prefix)?;

                let fns: Vec<Box<dyn Fn(&mut Self) -> Result<Value>>> = vec![
                    Box::new(|c: &mut Self| {
                        Ok(c.cmp_types(IntCC::Equal, lhs_type, rhs_arg.kind()))
                    }),
                    Box::new(|c: &mut Self| {
                        if let Arg::Str(rhs_ptr, rhs_len) = rhs_arg {
                            let (lhs_ptr, lhs_len) = c.load_str(lhs);
                            let args = [lhs_ptr, lhs_len, rhs_ptr, rhs_len];

                            let mut sig = c.module.make_signature();
                            for _ in 0..args.len() {
                                sig.params.push(AbiParam::new(c.ptr_type));
                            }
                            sig.returns.push(AbiParam::new(BOOL_TYPE));

                            c.call_indirect(sig, starts_with as *const (), &args)
                        } else {
                            Ok(c.builder.ins().iconst(BOOL_TYPE, 0))
                        }
                    }),
                ];
                Ok(self.and(&fns)?)
            }
            FilterAst::Eq(f, v) => self.cmp(IntCC::Equal, f, v),
            FilterAst::Ne(f, v) => self.cmp(IntCC::NotEqual, f, v),
            FilterAst::Lt(f, v) => self.cmp(IntCC::SignedLessThan, f, v),
            FilterAst::Lte(f, v) => self.cmp(IntCC::SignedLessThanOrEqual, f, v),
            FilterAst::Gt(f, v) => self.cmp(IntCC::SignedGreaterThan, f, v),
            FilterAst::Gte(f, v) => self.cmp(IntCC::SignedGreaterThanOrEqual, f, v),
        }
    }

    fn and<F>(&mut self, value_fns: &[F]) -> Result<Value>
    where
        F: Fn(&mut Self) -> Result<Value>,
    {
        let blocks: Vec<_> = (0..value_fns.len())
            .map(|_| self.builder.create_block())
            .collect();
        let exit_block = blocks[blocks.len() - 1];

        for (i, (value_fn, next_block)) in value_fns.iter().zip(blocks).enumerate() {
            let value = value_fn(self)?;

            if i < value_fns.len() - 1 {
                // If true, test next expr by jumping to the next block.
                // If false, exit early by jumping to exit block.
                self.builder
                    .ins()
                    .brif(value, next_block, &[], exit_block, &[value]);
            } else {
                // Last expr, simply exit.
                self.builder.ins().jump(exit_block, &[value]);
            }

            self.builder.switch_to_block(next_block);
            self.builder.seal_block(next_block);
        }

        Ok(self.builder.append_block_param(exit_block, BOOL_TYPE))
    }

    fn or<F>(&mut self, value_fns: &[F]) -> Result<Value>
    where
        F: Fn(&mut Self) -> Result<Value>,
    {
        let blocks: Vec<_> = (0..value_fns.len())
            .map(|_| self.builder.create_block())
            .collect();
        let exit_block = blocks[blocks.len() - 1];

        for (i, (value_fn, next_block)) in value_fns.iter().zip(blocks).enumerate() {
            let value = value_fn(self)?;

            if i < value_fns.len() - 1 {
                // If true, exit early by jumping to exit block.
                // If false, test next expr by jumping to the next block.
                self.builder
                    .ins()
                    .brif(value, exit_block, &[value], next_block, &[]);
            } else {
                // Last expr, simply exit.
                self.builder.ins().jump(exit_block, &[value]);
            }

            self.builder.switch_to_block(next_block);
            self.builder.seal_block(next_block);
        }

        Ok(self.builder.append_block_param(exit_block, BOOL_TYPE))
    }

    fn cmp(&mut self, cmp: IntCC, field: &str, value: &str) -> Result<Value> {
        let (lhs_type, lhs) = self.ident(field)?;
        let rhs_arg = self.literal(value)?;

        let cmps: Vec<Box<dyn Fn(&mut Self) -> Result<Value>>> = vec![
            Box::new(|c: &mut Self| Ok(c.cmp_types(IntCC::Equal, lhs_type, rhs_arg.kind()))),
            Box::new(|c: &mut Self| match rhs_arg {
                Arg::_Null => Ok(c.builder.ins().iconst(BOOL_TYPE, 0)),
                Arg::Int(rhs) => Ok(c.builder.ins().icmp(cmp, lhs, rhs)),
                Arg::Str(rhs_ptr, rhs_len) => {
                    let (lhs_ptr, lhs_len) = c.load_str(lhs);
                    c.cmp_strs(cmp, lhs_ptr, lhs_len, rhs_ptr, rhs_len)
                }
            }),
        ];
        self.and(&cmps)
    }

    fn cmp_strs(
        &mut self,
        cmp: IntCC,
        lhs_ptr: Value,
        lhs_len: Value,
        rhs_ptr: Value,
        rhs_len: Value,
    ) -> Result<Value> {
        let cmps: Vec<Box<dyn Fn(&mut Self) -> Result<Value>>> = vec![
            Box::new(|c: &mut Self| Ok(c.builder.ins().icmp(IntCC::Equal, lhs_len, rhs_len))),
            Box::new(|c: &mut Self| {
                let args = [lhs_ptr, rhs_ptr, rhs_len];

                let mut sig = c.module.make_signature();
                for _ in 0..args.len() {
                    sig.params.push(AbiParam::new(c.ptr_type));
                }
                sig.returns.push(AbiParam::new(c.ptr_type));

                let result = c.call_libc(sig, "strncmp", &args)?;
                let zero = c.builder.ins().iconst(c.ptr_type, 0);
                Ok(c.builder.ins().icmp(cmp, result, zero))
            }),
        ];
        self.and(&cmps)
    }

    fn load_str(&mut self, addr: Value) -> (Value, Value) {
        let str_ptr = self
            .builder
            .ins()
            .load(self.ptr_type, MemFlags::new(), addr, 0);
        let str_len = self
            .builder
            .ins()
            .load(self.ptr_type, MemFlags::new(), addr, self.ptr_size);
        (str_ptr, str_len)
    }

    fn cmp_types(&mut self, cmp: IntCC, lhs_type: Value, rhs_type: ArgKind) -> Value {
        let type_const = self.builder.ins().iconst(types::I8, rhs_type as i64);
        self.builder.ins().icmp(cmp, lhs_type, type_const)
    }

    fn ident(&mut self, name: &str) -> Result<(Value, Value)> {
        let var = self
            .variables
            .get(name)
            .with_context(|| format!("variable '{name}' not defined"))?;
        let argv = self.builder.use_var(self.args_var);

        let offset = self
            .builder
            .ins()
            .iconst(self.ptr_type, *var * (1 + self.ptr_size as i64));

        let addr = self.builder.ins().iadd(argv, offset);
        let arg_type = self.builder.ins().load(types::I8, MemFlags::new(), addr, 0);
        let arg_value = self
            .builder
            .ins()
            .load(types::I64, MemFlags::new(), addr, 1);

        Ok((arg_type, arg_value))
    }

    fn literal(&mut self, value: &str) -> Result<Arg> {
        if value.starts_with('"') {
            if !value.ends_with('"') {
                bail!("'{value}' must begin and end with quotes to be a valid string");
            }

            Ok(Arg::Str(
                self.builder
                    .ins()
                    .iconst(self.ptr_type, (value.as_ptr() as i64) + 1),
                self.builder
                    .ins()
                    .iconst(self.ptr_type, (value.len() as i64) - 2),
            ))
        } else {
            let imm: i64 = value
                .parse()
                .with_context(|| format!("'{value}' is not an integer"))?;
            Ok(Arg::Int(self.builder.ins().iconst(self.ptr_type, imm)))
        }
    }

    fn call_libc(&mut self, sig: Signature, name: &str, args: &[Value]) -> Result<Value> {
        let callee = self.module.declare_function(name, Linkage::Import, &sig)?;
        let local_callee = self.module.declare_func_in_func(callee, self.builder.func);
        let call = self.builder.ins().call(local_callee, args);
        Ok(self.builder.inst_results(call)[0])
    }

    fn call_indirect(&mut self, sig: Signature, func: *const (), args: &[Value]) -> Result<Value> {
        let fn_ptr = self.builder.ins().iconst(self.ptr_type, func as i64);
        let sig_ref = self.builder.import_signature(sig);
        let call = self.builder.ins().call_indirect(sig_ref, fn_ptr, args);
        Ok(self.builder.inst_results(call)[0])
    }
}

unsafe fn run_code(code_ptr: *const (), args: &[u8]) -> bool {
    let code_fn: fn(*const u8) -> bool = std::mem::transmute(code_ptr);
    code_fn(args.as_ptr())
}

async fn build_args(log: &Log, fields: &[KeyString]) -> Option<(Vec<u8>, Vec<Box<[u8]>>)> {
    let mut args = Vec::new();
    let mut allocs = Vec::new();

    for field in fields {
        if let Some(value) = log.get(field) {
            match value {
                vrl::value::Value::Integer(i) => {
                    args.push(ArgKind::Int as u8);
                    args.extend((*i).to_ne_bytes());
                }
                vrl::value::Value::Bytes(b) => {
                    args.push(ArgKind::Str as u8);

                    let mut bytes = Vec::with_capacity(std::mem::size_of::<usize>() * 2);
                    bytes.extend((b.as_ref().as_ptr() as usize).to_ne_bytes());
                    bytes.extend(b.len().to_ne_bytes());

                    allocs.push(bytes.into_boxed_slice());

                    let ptr = allocs[allocs.len() - 1].as_ptr();
                    args.extend((ptr as usize).to_ne_bytes());
                }
                _ => {
                    return None;
                }
            };
        } else {
            args.push(ArgKind::_Null as u8);
            args.extend(0usize.to_ne_bytes());
        }
    }

    Some((args, allocs))
}

pub async fn filter_stream(ast: FilterAst, mut input_stream: LogStream) -> Result<LogTryStream> {
    let ast = Arc::new(ast);
    let ast_clone = ast.clone();
    let (code_ptr, fields) = spawn_blocking(move || filter_ast_to_jit(&ast_clone)).await??;

    let fields: Vec<vrl::value::KeyString> = fields.into_iter().map(|x| x.into()).collect();

    Ok(Box::pin(try_stream! {
        // The generated JIT code references strings from inside the AST.
        let _ast_keepalive = ast;

        while let Some(log) = input_stream.next().await {
            let Some((args, _allocs)) = build_args(&log, &fields).await else {
                continue;
            };

            let keep: bool = unsafe { run_code(code_ptr as *const (), &args) };
            if keep {
                yield log;
            }
        }
    }))
}
