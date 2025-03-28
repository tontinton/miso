#![allow(clippy::type_complexity)]

use std::{collections::BTreeMap, slice::from_raw_parts};

use color_eyre::{
    eyre::{bail, eyre, ContextCompat},
    Result,
};
use cranelift::{
    jit::{JITBuilder, JITModule},
    module::{default_libcall_names, Linkage, Module},
    native,
    prelude::{
        settings, types, AbiParam, Block, Configurable, FloatCC, FunctionBuilder, InstBuilder,
        IntCC, JumpTableData, MemFlags, Signature, Value, Variable,
    },
};
use kinded::Kinded;
use memchr::memchr_iter;

#[macro_export]
macro_rules! impl_and {
    ($compiler:expr, $exprs:expr, $compile_fn:expr) => {{
        let blocks: Vec<_> = (0..$exprs.len())
            .map(|_| $compiler.builder.create_block())
            .collect();

        let exit_block = blocks[blocks.len() - 1];

        for (i, (expr, next_block)) in $exprs.iter().zip(blocks).enumerate() {
            let arg = $compile_fn(expr)?;
            let value = $compiler.arg_to_value_for_if(arg);

            if i < $exprs.len() - 1 {
                // If true, test next expr by jumping to the next block.
                // If false, exit early by jumping to exit block.
                $compiler
                    .builder
                    .ins()
                    .brif(value, next_block, &[], exit_block, &[value]);
            } else {
                // Last expr, simply exit.
                $compiler.builder.ins().jump(exit_block, &[value]);
            }

            $compiler.switch_to_block(next_block);
        }

        Ok(Arg::Bool(
            $compiler.builder.append_block_param(exit_block, BOOL_TYPE),
        ))
    }};
}

#[macro_export]
macro_rules! impl_or {
    ($compiler:expr, $exprs:expr, $compile_fn:expr) => {{
        let blocks: Vec<_> = (0..$exprs.len())
            .map(|_| $compiler.builder.create_block())
            .collect();

        let exit_block = blocks[blocks.len() - 1];

        for (i, (expr, next_block)) in $exprs.iter().zip(blocks).enumerate() {
            let arg = $compile_fn(expr)?;
            let value = $compiler.arg_to_value_for_if(arg);

            if i < $exprs.len() - 1 {
                // If true, exit early by jumping to exit block.
                // If false, test next expr by jumping to the next block.
                $compiler
                    .builder
                    .ins()
                    .brif(value, exit_block, &[value], next_block, &[]);
            } else {
                // Last expr, simply exit.
                $compiler.builder.ins().jump(exit_block, &[value]);
            }

            $compiler.switch_to_block(next_block);
        }

        Ok(Arg::Bool(
            $compiler.builder.append_block_param(exit_block, BOOL_TYPE),
        ))
    }};
}

pub const BOOL_TYPE: types::Type = types::I8;

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

pub fn new_jit_module() -> Result<JITModule> {
    let mut flag_builder = settings::builder();
    flag_builder.set("use_colocated_libcalls", "false")?;
    flag_builder.set("is_pic", "false")?;
    let isa_builder =
        native::builder().map_err(|msg| eyre!("host machine is not supported: {}", msg))?;
    let isa = isa_builder.finish(settings::Flags::new(flag_builder))?;
    let builder = JITBuilder::with_isa(isa, default_libcall_names());
    Ok(JITModule::new(builder))
}

pub fn int_cc_to_ordered_float_cc(cc: IntCC) -> FloatCC {
    match cc {
        IntCC::Equal => FloatCC::Equal,
        IntCC::NotEqual => FloatCC::OrderedNotEqual,
        IntCC::SignedLessThan | IntCC::UnsignedLessThan => FloatCC::LessThan,
        IntCC::SignedLessThanOrEqual | IntCC::UnsignedLessThanOrEqual => FloatCC::LessThanOrEqual,
        IntCC::SignedGreaterThan | IntCC::UnsignedGreaterThan => FloatCC::GreaterThan,
        IntCC::SignedGreaterThanOrEqual | IntCC::UnsignedGreaterThanOrEqual => {
            FloatCC::GreaterThanOrEqual
        }
    }
}

pub fn starts_with(
    hay_ptr: *const u8,
    hay_len: usize,
    needle_ptr: *const u8,
    needle_len: usize,
) -> bool {
    if needle_len > hay_len {
        return false;
    }

    // Safety: We assume that the pointers are valid and the memory is correctly aligned.
    let (hay, needle) = unsafe {
        (
            from_raw_parts(hay_ptr, hay_len),
            from_raw_parts(needle_ptr, needle_len),
        )
    };

    hay.get(0..needle_len) == Some(needle)
}

pub fn ends_with(
    hay_ptr: *const u8,
    hay_len: usize,
    needle_ptr: *const u8,
    needle_len: usize,
) -> bool {
    if needle_len > hay_len {
        return false;
    }

    // Safety: We assume that the pointers are valid and the memory is correctly aligned.
    let (hay, needle) = unsafe {
        (
            from_raw_parts(hay_ptr, hay_len),
            from_raw_parts(needle_ptr, needle_len),
        )
    };

    hay.get(hay_len - needle_len..hay_len) == Some(needle)
}

pub fn contains(
    hay_ptr: *const u8,
    hay_len: usize,
    needle_ptr: *const u8,
    needle_len: usize,
) -> bool {
    if needle_len > hay_len {
        return false;
    }

    // Safety: We assume that the pointers are valid and the memory is correctly aligned.
    let (hay, needle) = unsafe {
        (
            from_raw_parts(hay_ptr, hay_len),
            from_raw_parts(needle_ptr, needle_len),
        )
    };

    for pos in memchr_iter(needle[0], hay) {
        if hay[pos..].get(0..needle_len) == Some(needle) {
            return true;
        }
    }
    false
}

pub struct Compiler<'a> {
    pub ptr_type: types::Type,
    pub ptr_size: u8,
    pub builder: FunctionBuilder<'a>,
    pub args_var: Variable,
    pub variables: BTreeMap<String, i64>,
    pub module: &'a mut JITModule,
}

impl Compiler<'_> {
    fn and<F>(&mut self, arg_fns: &[F]) -> Result<Arg>
    where
        F: Fn(&mut Self) -> Result<Arg>,
    {
        impl_and!(self, arg_fns, |arg_fn: &F| arg_fn(self))
    }

    pub fn switch_to_block(&mut self, block: Block) {
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
    pub fn not_arg(&mut self, arg: Arg) -> Arg {
        Arg::Bool(match arg {
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
        })
    }

    #[must_use]
    pub fn arg_to_value_for_if(&mut self, arg: Arg) -> Value {
        match arg {
            Arg::_Null | Arg::_NotExists => self.builder.ins().iconst(BOOL_TYPE, 0),
            Arg::Bool(v) | Arg::Int(v) | Arg::Float(v) | Arg::Unresolved(_, v) => v,
            Arg::Str(_, len) => len,
        }
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

    pub fn cmp(&mut self, cmp: IntCC, lhs: Arg, rhs: Arg) -> Result<Arg> {
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
    pub fn cmp_type_to_kind(&mut self, cmp: IntCC, lhs_type: Value, rhs_type: ArgKind) -> Value {
        let type_const = self.builder.ins().iconst(BOOL_TYPE, rhs_type as i64);
        self.builder.ins().icmp(cmp, lhs_type, type_const)
    }

    pub fn ident(&mut self, name: &str) -> Result<(Value, Value)> {
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

    pub fn literal(&mut self, value: &serde_json::Value) -> Result<Arg> {
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
    pub fn str_literal(&mut self, value: &str) -> Arg {
        Arg::Str(
            self.builder
                .ins()
                .iconst(self.ptr_type, value.as_ptr() as i64),
            self.builder.ins().iconst(self.ptr_type, value.len() as i64),
        )
    }

    pub fn call_indirect_on_two_strings(
        &mut self,
        lhs: Arg,
        rhs: Arg,
        func: fn(*const u8, usize, *const u8, usize) -> bool,
    ) -> Result<Arg> {
        self.run_on_two_strings(
            lhs,
            rhs,
            &Box::new(
                |c: &mut Compiler,
                 lhs_ptr: Value,
                 lhs_len: Value,
                 rhs_ptr: Value,
                 rhs_len: Value| {
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
