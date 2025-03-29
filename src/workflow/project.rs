use std::{collections::BTreeMap, sync::Arc};

use async_stream::try_stream;
use color_eyre::eyre::{bail, Context, Result};
use cranelift::{
    module::{Linkage, Module},
    prelude::{AbiParam, FunctionBuilder, FunctionBuilderContext, InstBuilder, Variable},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::{
    log::{Log, LogStream, LogTryStream},
    thread_pool::run_on_thread_pool,
};

use super::jit::{
    build_args, jit_thread_pool, new_jit_module, run_code, Arg, ArgKind, Compiler, FlattenedFields,
};

// #[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
// #[serde(rename_all = "snake_case")]
// pub enum CastType {
//     Bool,
//     Float,
//     Int,
//     String,
//     // Regex is not yet supported.
// }

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ProjectAst {
    Id(String),
    Lit(serde_json::Value),
    // Cast(CastType, Box<ProjectAst>),
    #[serde(rename = "*")]
    Mul(Box<ProjectAst>, Box<ProjectAst>),
    #[serde(rename = "/")]
    Div(Box<ProjectAst>, Box<ProjectAst>),
    #[serde(rename = "+")]
    Plus(Box<ProjectAst>, Box<ProjectAst>),
    #[serde(rename = "-")]
    Minus(Box<ProjectAst>, Box<ProjectAst>),
}

impl ProjectAst {
    fn fields(&self, out: &mut Vec<String>) {
        match self {
            ProjectAst::Id(f) => out.push(f.clone()),
            ProjectAst::Mul(lhs, rhs)
            | ProjectAst::Div(lhs, rhs)
            | ProjectAst::Plus(lhs, rhs)
            | ProjectAst::Minus(lhs, rhs) => {
                lhs.fields(out);
                rhs.fields(out);
            }
            ProjectAst::Lit(..) => {}
        };
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub struct ProjectField {
    from: ProjectAst,
    to: String,
}

impl ProjectField {
    fn fields(&self) -> Vec<String> {
        let mut out = Vec::new();
        self.from.fields(&mut out);
        out.push(self.to.clone());
        out.sort();
        out.dedup();
        out
    }
}

fn project_to_jit(project: &ProjectField) -> Result<(usize, Vec<String>, Box<ArgKind>)> {
    let mut module = new_jit_module()?;
    let mut ctx = module.make_context();
    let mut builder_context = FunctionBuilderContext::new();

    let ptr_type = module.target_config().pointer_type();
    let ptr_size = module.target_config().pointer_bytes();

    ctx.func.signature.params.push(AbiParam::new(ptr_type));
    ctx.func.signature.returns.push(AbiParam::new(ptr_type));

    let mut builder = FunctionBuilder::new(&mut ctx.func, &mut builder_context);

    let entry_block = builder.create_block();

    builder.append_block_params_for_function_params(entry_block);
    builder.switch_to_block(entry_block);
    builder.seal_block(entry_block);

    let args_var = Variable::from_u32(0);
    builder.declare_var(args_var, ptr_type);
    builder.def_var(args_var, builder.block_params(entry_block)[0]);

    let fields = project.fields();

    let mut variables = BTreeMap::new();
    for (i, name) in fields.iter().enumerate() {
        variables.insert(name.clone(), i as i64);
    }

    let mut compiler = ProjectCompiler {
        c: Compiler {
            ptr_type,
            ptr_size,
            builder,
            args_var,
            variables,
            module: &mut module,
        },
    };

    let return_value_kind = Box::new(ArgKind::_Null);

    let return_arg = compiler.compile(&project.from)?;
    let return_value = compiler.c.arg_to_value(return_arg, &*return_value_kind)?;

    compiler.c.builder.ins().return_(&[return_value]);
    compiler.c.builder.finalize();

    let id = module.declare_function("main", Linkage::Export, &ctx.func.signature)?;
    module.define_function(id, &mut ctx)?;

    module.clear_context(&mut ctx);

    module.finalize_definitions().context("failed to compile")?;

    let jit_func_addr = module.get_finalized_function(id);
    Ok((jit_func_addr as usize, fields, return_value_kind))
}

struct ProjectCompiler<'a> {
    c: Compiler<'a>,
}

impl ProjectCompiler<'_> {
    fn compile(&mut self, ast: &ProjectAst) -> Result<Arg> {
        match ast {
            ProjectAst::Id(name) => {
                let (ident_type, ident_value) = self.c.ident(name)?;
                Ok(Arg::Unresolved(ident_type, ident_value))
            }
            ProjectAst::Lit(value) => self.c.literal(value),
            ProjectAst::Mul(lhs, rhs) => todo!(),
            ProjectAst::Div(lhs, rhs) => todo!(),
            ProjectAst::Plus(lhs, rhs) => todo!(),
            ProjectAst::Minus(lhs, rhs) => todo!(),
        }
    }
}

fn retval_to_vrl_value(retval: usize, retval_kind: &ArgKind) -> Result<vrl::value::Value> {
    Ok(match retval_kind {
        ArgKind::_NotExists | ArgKind::_Null => vrl::value::Value::Null,
        ArgKind::Bool => vrl::value::Value::Boolean(retval != 0),
        ArgKind::Int => vrl::value::Value::Integer(retval as i64),
        ArgKind::Float => vrl::value::Value::from_f64_or_zero(f64::from_bits(retval as u64)),
        ArgKind::Str => {
            if retval == 0 {
                bail!("Failed to allocate a string in JIT compiled project");
            }

            let str_bytes = unsafe {
                let ptr = retval as *const u8;
                let len_size = std::mem::size_of::<usize>();
                let stored_len = *(ptr as *const usize);
                let str_data_ptr = ptr.add(len_size) as *mut u8;
                Vec::from_raw_parts(str_data_ptr, stored_len, stored_len)
            };

            vrl::value::Value::Bytes(str_bytes.into())
        }
        ArgKind::Unresolved => {
            bail!("Return value from JIT compiled project doesn't have a valid type?");
        }
    })
}

pub async fn project_stream(
    project_fields: Vec<ProjectField>,
    mut input_stream: LogStream,
) -> Result<LogTryStream> {
    let project_fields = Arc::new(project_fields);
    let project_fields_clone = project_fields.clone();
    let compilation_results = run_on_thread_pool(jit_thread_pool(), move || {
        project_fields_clone
            .iter()
            .map(project_to_jit)
            .collect::<Result<Vec<_>>>()
    })
    .await
    .context("run on jit thread pool")?
    .context("project jit compile")?;

    let compilation_results: Vec<_> = project_fields
        .iter()
        .zip(compilation_results)
        .map(|(project_field, (jit_fn_ptr, fields, return_value))| {
            (
                vrl::value::KeyString::from(project_field.to.clone()),
                jit_fn_ptr,
                FlattenedFields::from_nested(&fields),
                return_value,
            )
        })
        .collect();

    Ok(Box::pin(try_stream! {
        // The generated JIT code references strings from inside the AST.
        let _project_fields_keepalive = project_fields;

        while let Some(mut log) = input_stream.next().await {
            for (to, jit_fn_ptr, flattened_fields, retval_kind) in &compilation_results {
                let Some((args, _allocs)) = build_args(&log, flattened_fields.iter()).await else {
                    continue;
                };

                let retval = unsafe { run_code::<usize>(*jit_fn_ptr as *const (), &args) };
                match retval_to_vrl_value(retval, retval_kind) {
                    Ok(v) => {
                        log.insert(to.clone(), v);
                    }
                    Err(e) => {
                        error!("Failed to insert JIT retval into log: {e}");
                    }
                }
            }

            yield log;
        }
    }))
}
