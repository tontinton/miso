use std::{cmp::Ordering, collections::BTreeMap};

use color_eyre::{eyre::bail, Result};
use vrl::{
    compiler::{compile, state::RuntimeState, Program, TargetValue, TimeZone},
    diagnostic::{DiagnosticList, Severity},
    value::{KeyString, Secrets, Value},
};

use crate::log::Log;

pub fn run_vrl(program: &Program, log: Log) -> Result<Value> {
    let mut target = TargetValue {
        value: log.into(),
        metadata: Value::Object(BTreeMap::new()),
        secrets: Secrets::default(),
    };
    let mut state = RuntimeState::default();
    let timezone = TimeZone::default();
    let mut ctx = vrl::compiler::Context::new(&mut target, &mut state, &timezone);
    Ok(program.resolve(&mut ctx)?)
}

fn severity_to_str(severity: &Severity) -> &'static str {
    match severity {
        Severity::Bug => "Bug",
        Severity::Error => "Error",
        Severity::Warning => "Warning",
        Severity::Note => "Note",
    }
}

fn pretty_print_diagnostics(diagnostics: &DiagnosticList) {
    let message = diagnostics
        .iter()
        .map(|d| {
            let labels = d
                .labels
                .iter()
                .map(|l| {
                    format!(
                        "    [{}] {} (Span: {}-{})",
                        if l.primary { "Primary" } else { "Secondary" },
                        l.message,
                        l.span.start(),
                        l.span.end(),
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");

            let notes = d
                .notes
                .iter()
                .map(|n| format!("    Note: {}", n))
                .collect::<Vec<_>>()
                .join("\n");

            format!(
                "{} [{}]: {}\n{}\n{}",
                severity_to_str(&d.severity),
                d.code,
                d.message,
                labels,
                notes
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    println!("{message}");
}

pub fn compile_pretty_print_errors(script: &str) -> Result<Program> {
    match compile(script, &vrl::stdlib::all()) {
        Ok(program) => {
            if !program.warnings.is_empty() {
                println!("Warnings:");
                pretty_print_diagnostics(&program.warnings);
            }
            Ok(program.program)
        }
        Err(diagnostics) => {
            println!("Errors:");
            pretty_print_diagnostics(&diagnostics);
            bail!("Failed to compile VRL script:\n{}", script);
        }
    }
}

// Should be replaced by: https://github.com/vectordotdev/vrl/pull/1117.
pub fn partial_cmp_values(left: &Value, right: &Value) -> Option<Ordering> {
    if std::mem::discriminant(left) != std::mem::discriminant(right) {
        return None;
    }
    match (left, right) {
        (Value::Bytes(a), Value::Bytes(b)) => a.partial_cmp(b),
        (Value::Regex(a), Value::Regex(b)) => a.partial_cmp(b),
        (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
        (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
        (Value::Object(a), Value::Object(b)) => {
            let keys_a: Vec<&KeyString> = a.keys().collect();
            let keys_b: Vec<&KeyString> = b.keys().collect();
            let mut keys_a = keys_a;
            let mut keys_b = keys_b;
            keys_a.sort();
            keys_b.sort();

            let keys_cmp = keys_a.partial_cmp(&keys_b)?;
            if keys_cmp != Ordering::Equal {
                return Some(keys_cmp);
            }

            for key in keys_a.into_iter() {
                let cmp = partial_cmp_values(&a[key], &b[key])?;
                if cmp != Ordering::Equal {
                    return Some(cmp);
                }
            }
            Some(Ordering::Equal)
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() < b.len() {
                return Some(Ordering::Less);
            }

            for (x, y) in a.iter().zip(b) {
                let cmp = partial_cmp_values(x, y)?;
                if cmp != Ordering::Equal {
                    return Some(cmp);
                }
            }
            Some(Ordering::Equal)
        }
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        _ => None,
    }
}
