//! Standalone profiling harness for filter evaluation.
//!
//! Usage:
//!   cargo build --release --bench filter_profile
//!   valgrind --tool=callgrind --callgrind-out-file=callgrind.out \
//!     target/release/deps/filter_profile-*
//!
//! Then view with:
//!   callgrind_annotate callgrind.out | head -100
//!   # or: kcachegrind callgrind.out

use std::str::FromStr;
use std::time::Instant;

use miso_workflow::interpreter::LogInterpreter;
use miso_workflow_types::{
    expr::Expr,
    field::Field,
    field_unwrap,
    log::Log,
    value::Value,
};

fn pseudo_random(seed: usize) -> usize {
    seed.wrapping_mul(2654435761) >> 16
}

fn generate_logs(count: usize, needle: &str, needle_rate: f64) -> Vec<Log> {
    let templates = [
        "Connection established from 192.168.1.{0} to port 8080",
        "User {0} authenticated successfully via SSO",
        "Request GET /api/v1/resources/{0} completed in 42ms",
        "Cache miss for key session:{0}, fetching from database",
        "Worker thread {0} processing batch job #12345",
        "Health check passed: cpu=23%, mem=67%, disk=45%",
        "Rate limit exceeded for client {0}, throttling enabled",
        "TLS handshake completed with cipher TLS_AES_256_GCM_SHA384",
        "Database query executed in {0}ms, 150 rows returned",
        "Scheduled task cleanup-{0} started at epoch 1700000000",
    ];

    (0..count)
        .map(|i| {
            let mut log = Log::new();

            let should_match = (pseudo_random(i) % 1000) < (needle_rate * 1000.0) as usize;
            let msg = if should_match {
                format!(
                    "{} {} extra context for realism",
                    templates[i % templates.len()].replace("{0}", &i.to_string()),
                    needle,
                )
            } else {
                templates[i % templates.len()].replace("{0}", &i.to_string())
            };

            log.insert("message".to_string(), Value::String(msg));
            log.insert(
                "level".to_string(),
                Value::String(["INFO", "WARN", "ERROR", "DEBUG"][i % 4].to_string()),
            );
            log.insert(
                "host".to_string(),
                Value::String(format!("server-{:03}", i % 50)),
            );
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn filter_logs(logs: &[Log], expr: &Expr) -> usize {
    logs.iter()
        .filter(|log| {
            let interpreter = LogInterpreter { log };
            match interpreter.eval(expr) {
                Ok(v) => v.to_bool(),
                Err(_) => false,
            }
        })
        .count()
}

fn main() {
    let needle = "needle-value";
    let count = 500_000;
    let logs = generate_logs(count, needle, 0.01);

    let exprs: Vec<(&str, Expr)> = vec![
        (
            "contains_cs",
            Expr::Contains(
                Box::new(Expr::Field(field_unwrap!("message"))),
                Box::new(Expr::Literal(Value::String(needle.to_string()))),
            ),
        ),
        (
            "has_cs",
            Expr::HasCs(
                Box::new(Expr::Field(field_unwrap!("message"))),
                Box::new(Expr::Literal(Value::String(needle.to_string()))),
            ),
        ),
        (
            "has_ci",
            Expr::Has(
                Box::new(Expr::Field(field_unwrap!("message"))),
                Box::new(Expr::Literal(Value::String(needle.to_string()))),
            ),
        ),
        (
            "compound_and",
            Expr::And(
                Box::new(Expr::Contains(
                    Box::new(Expr::Field(field_unwrap!("message"))),
                    Box::new(Expr::Literal(Value::String(needle.to_string()))),
                )),
                Box::new(Expr::Eq(
                    Box::new(Expr::Field(field_unwrap!("level"))),
                    Box::new(Expr::Literal(Value::String("ERROR".to_string()))),
                )),
            ),
        ),
    ];

    // Warm up
    for (_, expr) in &exprs {
        let _ = filter_logs(&logs, expr);
    }

    // Profile runs
    let iterations = 10;
    for (name, expr) in &exprs {
        let start = Instant::now();
        let mut matches = 0;
        for _ in 0..iterations {
            matches = filter_logs(&logs, expr);
        }
        let elapsed = start.elapsed();
        let per_iter = elapsed / iterations;
        let ns_per_log = per_iter.as_nanos() as f64 / count as f64;
        eprintln!(
            "{name:15} | {matches:6} matches | {per_iter:>10.2?}/iter | {ns_per_log:.1} ns/log"
        );
    }
}
