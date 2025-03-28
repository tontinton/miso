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
        types, AbiParam, Block, FloatCC, FunctionBuilder, FunctionBuilderContext, InstBuilder,
        IntCC, JumpTableData, MemFlags, Signature, Value, Variable,
    },
};
use futures_util::StreamExt;
use kinded::Kinded;
use serde::{Deserialize, Serialize};
use tokio::task::spawn_blocking;
use vrl::value::KeyString;

use crate::log::{Log, LogStream, LogTryStream};

use super::jit::{contains, ends_with, int_cc_to_ordered_float_cc, new_jit_module, starts_with};

const BOOL_TYPE: types::Type = types::I8;

#[derive(Kinded, Debug, Copy, Clone)]
pub enum Arg {
    _Null,                    // null
    _NotExists,               // doesn't exist
    Unresolved(Value, Value), // ArgKind, Value
    Bool(Value),              // I8
    Int(Value),               // I64
    Float(Value),             // F64
    Str(Value, Value),        // *u8, usize
}

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

    let mut compiler = Compiler {
        ptr_type,
        ptr_size,
        builder,
        args_var,
        variables,
        module: &mut module,
    };
    let Arg::Bool(return_value) = compiler.compile(ast)? else {
        bail!("compiled filter function doesn't return bool");
    };

    compiler.builder.ins().return_(&[return_value]);
    compiler.builder.finalize();

    let id = module.declare_function("main", Linkage::Export, &ctx.func.signature)?;
    module.define_function(id, &mut ctx)?;

    module.clear_context(&mut ctx);

    module.finalize_definitions().context("failed to compile")?;

    let jit_func_addr = module.get_finalized_function(id);
    Ok((jit_func_addr as usize, fields))
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
    fn compile(&mut self, ast: &FilterAst) -> Result<Arg> {
        match ast {
            FilterAst::Id(name) => {
                let (ident_type, ident_value) = self.ident(name)?;
                Ok(Arg::Unresolved(ident_type, ident_value))
            }
            FilterAst::Lit(value) => self.literal(value),
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
            FilterAst::Not(expr) => {
                let arg = self.compile(expr)?;

                Ok(Arg::Bool(match arg {
                    Arg::_Null | Arg::_NotExists => self.builder.ins().iconst(BOOL_TYPE, 1),
                    Arg::Bool(value) => self.not_bool(value),
                    Arg::Int(value) | Arg::Str(_, /*len=*/ value) => self.not_int(value),
                    Arg::Float(value) => self.not_float(value),
                    Arg::Unresolved(kind, value) => {
                        let null_block = self.builder.create_block();
                        let bool_block = self.builder.create_block();
                        let int_block = self.builder.create_block();
                        let float_block = self.builder.create_block();
                        let str_block = self.builder.create_block();
                        let default_block = self.builder.create_block();
                        let return_block = self.builder.create_block();

                        let jt_data = JumpTableData::new(
                            self.builder.func.dfg.block_call(default_block, &[]),
                            &[
                                null_block,
                                null_block,    // not exists
                                default_block, // unresolved
                                bool_block,
                                int_block,
                                float_block,
                                str_block,
                            ]
                            .iter()
                            .map(|block| self.builder.func.dfg.block_call(*block, &[]))
                            .collect::<Vec<_>>(),
                        );

                        let jump_table = self.builder.create_jump_table(jt_data);
                        let kind = self.builder.ins().uextend(types::I32, kind);
                        self.builder.ins().br_table(kind, jump_table);

                        self.switch_to_block(null_block);
                        let not_null = self.builder.ins().iconst(BOOL_TYPE, 1);
                        self.builder.ins().jump(return_block, &[not_null]);

                        self.switch_to_block(bool_block);
                        let bool_value = self.cast_to_bool(value);
                        let not_bool = self.not_bool(bool_value);
                        self.builder.ins().jump(return_block, &[not_bool]);

                        self.switch_to_block(int_block);
                        let not_int = self.not_int(value);
                        self.builder.ins().jump(return_block, &[not_int]);

                        self.switch_to_block(float_block);
                        let float_value = self.cast_to_float(value);
                        let not_float = self.not_float(float_value);
                        self.builder.ins().jump(return_block, &[not_float]);

                        self.switch_to_block(str_block);
                        let len = self.load_str_len(value);
                        let not_str = self.not_int(len);
                        self.builder.ins().jump(return_block, &[not_str]);

                        self.switch_to_block(default_block);
                        let default_value = self.builder.ins().iconst(BOOL_TYPE, 0);
                        self.builder.ins().jump(return_block, &[default_value]);

                        self.switch_to_block(return_block);
                        self.builder.append_block_param(return_block, BOOL_TYPE)
                    }
                }))
            }
            FilterAst::Exists(field) => {
                let (field_type, _) = self.ident(field)?;
                Ok(Arg::Bool(self.cmp_type_to_kind(
                    IntCC::NotEqual,
                    field_type,
                    ArgKind::_NotExists,
                )))
            }
            FilterAst::Contains(lhs, rhs) => self.call_indirect_on_two_strings(lhs, rhs, contains),
            FilterAst::StartsWith(lhs, rhs) => {
                self.call_indirect_on_two_strings(lhs, rhs, starts_with)
            }
            FilterAst::EndsWith(lhs, rhs) => self.call_indirect_on_two_strings(lhs, rhs, ends_with),
            FilterAst::Eq(l, r) => self.cmp(IntCC::Equal, l, r),
            FilterAst::Ne(l, r) => self.cmp(IntCC::NotEqual, l, r),
            FilterAst::Lt(l, r) => self.cmp(IntCC::SignedLessThan, l, r),
            FilterAst::Lte(l, r) => self.cmp(IntCC::SignedLessThanOrEqual, l, r),
            FilterAst::Gt(l, r) => self.cmp(IntCC::SignedGreaterThan, l, r),
            FilterAst::Gte(l, r) => self.cmp(IntCC::SignedGreaterThanOrEqual, l, r),
        }
    }

    fn switch_to_block(&mut self, block: Block) {
        self.builder.switch_to_block(block);
        self.builder.seal_block(block);
    }

    #[must_use]
    fn not_bool(&mut self, value: Value) -> Value {
        let one = self.builder.ins().iconst(BOOL_TYPE, 1);
        self.builder.ins().bxor(value, one) // Flip 0 <-> 1
    }

    #[must_use]
    fn not_int(&mut self, value: Value) -> Value {
        let zero = self.builder.ins().iconst(self.ptr_type, 0);
        self.builder.ins().icmp(IntCC::Equal, value, zero)
    }

    #[must_use]
    fn not_float(&mut self, value: Value) -> Value {
        let zero = self.builder.ins().f64const(0.0);
        self.builder.ins().fcmp(FloatCC::Equal, value, zero)
    }

    #[must_use]
    fn cast_to_bool(&mut self, value: Value) -> Value {
        self.builder.ins().ireduce(BOOL_TYPE, value)
    }

    #[must_use]
    fn cast_to_float(&mut self, value: Value) -> Value {
        self.builder
            .ins()
            .bitcast(types::F64, MemFlags::new(), value)
    }

    #[must_use]
    fn arg_to_value_for_if(&mut self, arg: Arg) -> Value {
        match arg {
            Arg::_Null | Arg::_NotExists => self.builder.ins().iconst(BOOL_TYPE, 0),
            Arg::Bool(v) | Arg::Int(v) | Arg::Float(v) | Arg::Unresolved(_, v) => v,
            Arg::Str(_, len) => len,
        }
    }

    fn and<F>(&mut self, value_fns: &[F]) -> Result<Arg>
    where
        F: Fn(&mut Self) -> Result<Arg>,
    {
        let blocks: Vec<_> = (0..value_fns.len())
            .map(|_| self.builder.create_block())
            .collect();
        let exit_block = blocks[blocks.len() - 1];

        for (i, (value_fn, next_block)) in value_fns.iter().zip(blocks).enumerate() {
            let arg = value_fn(self)?;
            let value = self.arg_to_value_for_if(arg);

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

            self.switch_to_block(next_block);
        }

        Ok(Arg::Bool(
            self.builder.append_block_param(exit_block, BOOL_TYPE),
        ))
    }

    fn or<F>(&mut self, value_fns: &[F]) -> Result<Arg>
    where
        F: Fn(&mut Self) -> Result<Arg>,
    {
        let blocks: Vec<_> = (0..value_fns.len())
            .map(|_| self.builder.create_block())
            .collect();
        let exit_block = blocks[blocks.len() - 1];

        for (i, (value_fn, next_block)) in value_fns.iter().zip(blocks).enumerate() {
            let arg = value_fn(self)?;
            let value = self.arg_to_value_for_if(arg);

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

            self.switch_to_block(next_block);
        }

        Ok(Arg::Bool(
            self.builder.append_block_param(exit_block, BOOL_TYPE),
        ))
    }

    fn call_indirect_on_two_strings(
        &mut self,
        lhs_ast: &FilterAst,
        rhs_ast: &FilterAst,
        func: fn(*const u8, usize, *const u8, usize) -> bool,
    ) -> Result<Arg> {
        let lhs = self.compile(lhs_ast)?;
        let rhs = self.compile(rhs_ast)?;

        self.run_on_two_strings(
            lhs,
            rhs,
            &Box::new(
                |c: &mut Self, lhs_ptr: Value, lhs_len: Value, rhs_ptr: Value, rhs_len: Value| {
                    let args = [lhs_ptr, lhs_len, rhs_ptr, rhs_len];

                    let mut sig = c.module.make_signature();
                    for _ in 0..args.len() {
                        sig.params.push(AbiParam::new(c.ptr_type));
                    }
                    sig.returns.push(AbiParam::new(BOOL_TYPE));

                    Ok(Arg::Bool(c.call_indirect(sig, func as *const (), &args)?))
                },
            ),
        )
    }

    fn run_on_two_strings<F>(&mut self, lhs: Arg, rhs: Arg, callback: &F) -> Result<Arg>
    where
        F: Fn(&mut Self, Value, Value, Value, Value) -> Result<Arg>,
    {
        let mut fns: Vec<Box<dyn Fn(&mut Self) -> Result<Arg>>> = vec![];

        let load_lhs: Box<dyn Fn(&mut Self) -> (Value, Value)> = match lhs {
            Arg::Str(lhs_ptr, lhs_len) => Box::new(move |_: &mut Self| (lhs_ptr, lhs_len)),
            Arg::Unresolved(lhs_type, lhs_value) => {
                fns.push(Box::new(move |c: &mut Self| {
                    Ok(Arg::Bool(c.cmp_type_to_kind(
                        IntCC::Equal,
                        lhs_type,
                        ArgKind::Str,
                    )))
                }));
                Box::new(move |c: &mut Self| c.load_str(lhs_value))
            }
            _ => return Ok(Arg::Bool(self.builder.ins().iconst(BOOL_TYPE, 0))),
        };
        let load_rhs: Box<dyn Fn(&mut Self) -> (Value, Value)> = match rhs {
            Arg::Str(rhs_ptr, rhs_len) => Box::new(move |_: &mut Self| (rhs_ptr, rhs_len)),
            Arg::Unresolved(rhs_type, rhs_value) => {
                fns.push(Box::new(move |c: &mut Self| {
                    Ok(Arg::Bool(c.cmp_type_to_kind(
                        IntCC::Equal,
                        rhs_type,
                        ArgKind::Str,
                    )))
                }));
                Box::new(move |c: &mut Self| c.load_str(rhs_value))
            }
            _ => return Ok(Arg::Bool(self.builder.ins().iconst(BOOL_TYPE, 0))),
        };

        fns.push(Box::new(move |c: &mut Self| {
            let (lhs_ptr, lhs_len) = load_lhs(c);
            let (rhs_ptr, rhs_len) = load_rhs(c);
            callback(c, lhs_ptr, lhs_len, rhs_ptr, rhs_len)
        }));

        self.and(&fns)
    }

    fn cmp(&mut self, cmp: IntCC, lhs_ast: &FilterAst, rhs_ast: &FilterAst) -> Result<Arg> {
        let lhs = self.compile(lhs_ast)?;
        let rhs = self.compile(rhs_ast)?;

        let cmps: Vec<Box<dyn Fn(&mut Self) -> Result<Arg>>> = match (lhs, rhs) {
            (Arg::Unresolved(lhs_type, lhs_value), Arg::Unresolved(rhs_type, rhs_value)) => {
                vec![
                    Box::new(move |c: &mut Self| {
                        Ok(Arg::Bool(c.builder.ins().icmp(cmp, lhs_type, rhs_type)))
                    }),
                    Box::new(move |c: &mut Self| c.cmp_values(cmp, lhs_type, lhs_value, rhs_value)),
                ]
            }
            (Arg::Unresolved(lhs_type, lhs_value), _) => {
                vec![
                    Box::new(move |c: &mut Self| {
                        Ok(Arg::Bool(c.cmp_type_to_kind(
                            IntCC::Equal,
                            lhs_type,
                            rhs.kind(),
                        )))
                    }),
                    Box::new(move |c: &mut Self| c.cmp_arg_to_value(cmp, rhs, lhs_value)),
                ]
            }
            (_, Arg::Unresolved(rhs_type, rhs_value)) => {
                vec![
                    Box::new(move |c: &mut Self| {
                        Ok(Arg::Bool(c.cmp_type_to_kind(
                            IntCC::Equal,
                            rhs_type,
                            lhs.kind(),
                        )))
                    }),
                    Box::new(move |c: &mut Self| c.cmp_arg_to_value(cmp, lhs, rhs_value)),
                ]
            }
            _ => bail!(
                "can't compare '{}' between differing types ({} != {})",
                cmp.to_static_str(),
                lhs.kind(),
                rhs.kind()
            ),
        };

        self.and(&cmps)
    }

    fn cmp_arg_to_value(&mut self, cmp: IntCC, arg: Arg, value: Value) -> Result<Arg> {
        match arg {
            Arg::_Null => Ok(Arg::Bool(self.builder.ins().iconst(BOOL_TYPE, 1))),
            Arg::_NotExists => Ok(Arg::Bool(self.builder.ins().iconst(BOOL_TYPE, 0))),
            Arg::Bool(arg_value) => {
                let value = self.cast_to_bool(value);
                Ok(Arg::Bool(self.builder.ins().icmp(cmp, value, arg_value)))
            }
            Arg::Int(arg_value) => Ok(Arg::Bool(self.builder.ins().icmp(cmp, value, arg_value))),
            Arg::Float(arg_value) => {
                let value = self.cast_to_float(value);
                Ok(Arg::Bool(self.builder.ins().fcmp(
                    int_cc_to_ordered_float_cc(cmp),
                    value,
                    arg_value,
                )))
            }
            Arg::Str(rhs_ptr, rhs_len) => {
                let (lhs_ptr, lhs_len) = self.load_str(value);
                self.cmp_strs(cmp, lhs_ptr, lhs_len, rhs_ptr, rhs_len)
            }
            Arg::Unresolved(..) => bail!("unresolved arg in cmp?"),
        }
    }

    fn cmp_values(&mut self, cmp: IntCC, kind: Value, lhs: Value, rhs: Value) -> Result<Arg> {
        let null_block = self.builder.create_block();
        let bool_block = self.builder.create_block();
        let float_block = self.builder.create_block();
        let str_block = self.builder.create_block();
        let default_block = self.builder.create_block();
        let return_block = self.builder.create_block();

        let jt_data = JumpTableData::new(
            self.builder.func.dfg.block_call(default_block, &[]),
            &[
                null_block,    // null
                default_block, // not exists
                default_block, // unresolved
                bool_block,
                bool_block, // int
                float_block,
                str_block, // str
            ]
            .iter()
            .map(|block| self.builder.func.dfg.block_call(*block, &[]))
            .collect::<Vec<_>>(),
        );

        let jump_table = self.builder.create_jump_table(jt_data);
        let kind = self.builder.ins().uextend(types::I32, kind);
        self.builder.ins().br_table(kind, jump_table);

        self.switch_to_block(null_block);
        let null_cmp = self.builder.ins().iconst(BOOL_TYPE, 1);
        self.builder.ins().jump(return_block, &[null_cmp]);

        self.switch_to_block(bool_block);
        let lhs_bool = self.cast_to_bool(lhs);
        let rhs_bool = self.cast_to_bool(rhs);
        let bool_cmp = self.builder.ins().icmp(cmp, lhs_bool, rhs_bool);
        self.builder.ins().jump(return_block, &[bool_cmp]);

        self.switch_to_block(float_block);
        let lhs_float = self.cast_to_float(lhs);
        let rhs_float = self.cast_to_float(rhs);
        let float_cmp =
            self.builder
                .ins()
                .fcmp(int_cc_to_ordered_float_cc(cmp), lhs_float, rhs_float);
        self.builder.ins().jump(return_block, &[float_cmp]);

        self.switch_to_block(str_block);
        let (lhs_ptr, lhs_len) = self.load_str(lhs);
        let (rhs_ptr, rhs_len) = self.load_str(rhs);
        let Arg::Bool(str_cmp) = self.cmp_strs(cmp, lhs_ptr, lhs_len, rhs_ptr, rhs_len)? else {
            bail!("comparison result of strings is not a bool?");
        };
        self.builder.ins().jump(return_block, &[str_cmp]);

        self.switch_to_block(default_block);
        let default_value = self.builder.ins().iconst(BOOL_TYPE, 0);
        self.builder.ins().jump(return_block, &[default_value]);

        self.switch_to_block(return_block);
        Ok(Arg::Bool(
            self.builder.append_block_param(return_block, BOOL_TYPE),
        ))
    }

    fn cmp_strs(
        &mut self,
        cmp: IntCC,
        lhs_ptr: Value,
        lhs_len: Value,
        rhs_ptr: Value,
        rhs_len: Value,
    ) -> Result<Arg> {
        let cmps: Vec<Box<dyn Fn(&mut Self) -> Result<Arg>>> = vec![
            Box::new(|c: &mut Self| {
                Ok(Arg::Bool(c.builder.ins().icmp(
                    IntCC::Equal,
                    lhs_len,
                    rhs_len,
                )))
            }),
            Box::new(|c: &mut Self| {
                let args = [lhs_ptr, rhs_ptr, rhs_len];

                let mut sig = c.module.make_signature();
                for _ in 0..args.len() {
                    sig.params.push(AbiParam::new(c.ptr_type));
                }
                sig.returns.push(AbiParam::new(c.ptr_type));

                let result = c.call_libc(sig, "strncmp", &args)?;
                let zero = c.builder.ins().iconst(c.ptr_type, 0);
                Ok(Arg::Bool(c.builder.ins().icmp(cmp, result, zero)))
            }),
        ];
        self.and(&cmps)
    }

    #[must_use]
    fn load_str_len(&mut self, addr: Value) -> Value {
        self.builder
            .ins()
            .load(self.ptr_type, MemFlags::new(), addr, self.ptr_size)
    }

    #[must_use]
    fn load_str(&mut self, addr: Value) -> (Value, Value) {
        let str_ptr = self
            .builder
            .ins()
            .load(self.ptr_type, MemFlags::new(), addr, 0);
        let str_len = self.load_str_len(addr);
        (str_ptr, str_len)
    }

    #[must_use]
    fn cmp_type_to_kind(&mut self, cmp: IntCC, lhs_type: Value, rhs_type: ArgKind) -> Value {
        let type_const = self.builder.ins().iconst(BOOL_TYPE, rhs_type as i64);
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
            .load(self.ptr_type, MemFlags::new(), addr, 1);

        Ok((arg_type, arg_value))
    }

    fn literal(&mut self, value: &serde_json::Value) -> Result<Arg> {
        Ok(match value {
            serde_json::Value::Null => Arg::_Null,
            serde_json::Value::Number(x) => {
                if let Some(x) = x.as_i64() {
                    Arg::Int(self.builder.ins().iconst(self.ptr_type, x))
                } else if let Some(x) = x.as_f64() {
                    Arg::Float(self.builder.ins().f64const(x))
                } else {
                    bail!("'{x}' couldn't be parsed as either an i64 or a f64");
                }
            }
            serde_json::Value::String(x) => self.str_literal(x),
            serde_json::Value::Bool(x) => {
                Arg::Bool(self.builder.ins().iconst(BOOL_TYPE, if *x { 1 } else { 0 }))
            }
            serde_json::Value::Array(..) => {
                bail!("array values are currently unsupported");
            }
            serde_json::Value::Object(..) => {
                bail!("object values are currently unsupported");
            }
        })
    }

    #[must_use]
    fn str_literal(&mut self, value: &str) -> Arg {
        Arg::Str(
            self.builder
                .ins()
                .iconst(self.ptr_type, value.as_ptr() as i64),
            self.builder.ins().iconst(self.ptr_type, value.len() as i64),
        )
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

fn push_null(args: &mut Vec<u8>) {
    args.push(ArgKind::_Null as u8);
    args.extend(0usize.to_ne_bytes());
}

fn push_not_exists(args: &mut Vec<u8>) {
    args.push(ArgKind::_NotExists as u8);
    args.extend(0usize.to_ne_bytes());
}

async fn build_args(
    log: &Log,
    fields_iter: impl Iterator<Item = &[KeyString]>,
) -> Option<(Vec<u8>, Vec<Box<[u8]>>)> {
    use vrl::value::Value as V;

    let mut args = Vec::new();
    let mut allocs = Vec::new();

    'fields_loop: for field_keys in fields_iter {
        let mut obj = log;
        for field_key in &field_keys[..field_keys.len() - 1] {
            if let Some(V::Object(inner)) = obj.get(field_key) {
                obj = inner;
            } else {
                push_not_exists(&mut args);
                continue 'fields_loop;
            }
        }

        let Some(value) = obj.get(&field_keys[field_keys.len() - 1]) else {
            push_not_exists(&mut args);
            continue;
        };

        match value {
            V::Null => {
                push_null(&mut args);
            }
            V::Boolean(b) => {
                args.push(ArgKind::Bool as u8);
                let i: usize = if *b { 1 } else { 0 };
                args.extend(i.to_ne_bytes());
            }
            V::Integer(i) => {
                args.push(ArgKind::Int as u8);
                args.extend((*i).to_ne_bytes());
            }
            V::Float(i) => {
                args.push(ArgKind::Float as u8);
                args.extend((*i).to_ne_bytes());
            }
            V::Bytes(b) => {
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
    }

    Some((args, allocs))
}

/// A sequentially contiguous data structure to hold all fields and their nested keys,
/// separated by dots.
struct FlattenedFields {
    data: Vec<KeyString>,
    offsets: Vec<usize>,
}

impl FlattenedFields {
    fn from_nested(fields: Vec<String>) -> Self {
        let mut data = Vec::new();
        let mut offsets = Vec::with_capacity(fields.len());

        for field in fields {
            offsets.push(data.len());
            data.extend(field.split('.').map(|k| k.into()));
        }

        Self { data, offsets }
    }

    fn get(&self, index: usize) -> &[KeyString] {
        let start = self.offsets[index];
        let end = self
            .offsets
            .get(index + 1)
            .copied()
            .unwrap_or(self.data.len());
        &self.data[start..end]
    }

    fn iter(&self) -> impl Iterator<Item = &[KeyString]> {
        (0..self.offsets.len()).map(move |i| self.get(i))
    }
}

/// # Safety
///
/// - `fn_ptr` must be a valid function pointer to a function with the exact signature:
///   ```rust
///   fn(*const u8) -> bool
///   ```
///   Invoking a function pointer with an incorrect signature is **undefined behavior**.
/// - The function `fn_ptr` points to must **not unwind**. If it panics and unwinding is not caught,
///   this leads to **undefined behavior** since the function is called via `std::mem::transmute`.
/// - `args` must remain valid for the duration of the function call:
///   - It must be properly aligned.
///   - It must not be deallocated while the function executes.
/// - The function at `fn_ptr` must not assume `args` has a specific length unless that is externally enforced.
///   Passing an insufficiently sized buffer may cause out-of-bounds reads.
///
/// # Undefined Behavior
///
/// - If `fn_ptr` is null or not a valid function pointer, the behavior is undefined.
/// - If `fn_ptr` is a function with a mismatched calling convention, undefined behavior may occur.
/// - If the function at `fn_ptr` assumes `args` is mutable or writes to it, but `args` is immutable,
///   this may cause undefined behavior.
///
/// # Example Usage
///
/// ```rust
/// unsafe fn example_fn(ptr: *const u8) -> bool {
///     !ptr.is_null() // Example logic
/// }
///
/// let func_ptr = example_fn as *const ();
/// let args = [42u8];
///
/// let result = unsafe { run_code(func_ptr, &args) };
/// assert!(result);
/// ```
///
/// If you are unsure whether `fn_ptr` is valid, consider wrapping the function pointer in a higher-level
/// abstraction to enforce these constraints at runtime.
unsafe fn run_code(fn_ptr: *const (), args: &[u8]) -> bool {
    let code_fn: fn(*const u8) -> bool = std::mem::transmute(fn_ptr);
    code_fn(args.as_ptr())
}

pub async fn filter_stream(ast: FilterAst, mut input_stream: LogStream) -> Result<LogTryStream> {
    let ast = Arc::new(ast);
    let ast_clone = ast.clone();
    let (jit_fn_ptr, fields) = spawn_blocking(move || filter_ast_to_jit(&ast_clone)).await??;

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
