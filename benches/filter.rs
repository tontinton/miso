use std::str::FromStr;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use miso_workflow::interpreter::LogInterpreter;
use miso_workflow_types::{
    expr::Expr,
    field::Field,
    field_unwrap,
    log::Log,
    value::Value,
};

/// Deterministic pseudo-random number from seed.
fn pseudo_random(seed: usize) -> usize {
    seed.wrapping_mul(2654435761) >> 16
}

/// Generate realistic log lines with a "message" field containing varied text.
/// A fraction `needle_rate` of logs will contain the needle string.
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

            // Build message - sometimes inject the needle
            let should_match = (pseudo_random(i) % 1000) < (needle_rate * 1000.0) as usize;
            let msg = if should_match {
                format!(
                    "{} {} {}",
                    templates[i % templates.len()].replace("{0}", &i.to_string()),
                    needle,
                    "extra context for realism"
                )
            } else {
                templates[i % templates.len()].replace("{0}", &i.to_string())
            };

            log.insert("message".to_string(), Value::String(msg));
            log.insert(
                "level".to_string(),
                Value::String(
                    ["INFO", "WARN", "ERROR", "DEBUG"][i % 4].to_string(),
                ),
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

/// Filter all logs using the interpreter, returning count of matches.
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

// -- Expressions used in benchmarks --

/// `message contains_cs "needle"` (case-sensitive substring)
fn expr_contains_cs(needle: &str) -> Expr {
    Expr::Contains(
        Box::new(Expr::Field(field_unwrap!("message"))),
        Box::new(Expr::Literal(Value::String(needle.to_string()))),
    )
}

/// `message has_cs "needle"` (case-sensitive phrase/word boundary)
fn expr_has_cs(needle: &str) -> Expr {
    Expr::HasCs(
        Box::new(Expr::Field(field_unwrap!("message"))),
        Box::new(Expr::Literal(Value::String(needle.to_string()))),
    )
}

/// `message has "needle"` (case-insensitive phrase/word boundary)
fn expr_has(needle: &str) -> Expr {
    Expr::Has(
        Box::new(Expr::Field(field_unwrap!("message"))),
        Box::new(Expr::Literal(Value::String(needle.to_string()))),
    )
}

/// `message contains_cs "needle" and level == "ERROR"`
fn expr_compound(needle: &str) -> Expr {
    Expr::And(
        Box::new(expr_contains_cs(needle)),
        Box::new(Expr::Eq(
            Box::new(Expr::Field(field_unwrap!("level"))),
            Box::new(Expr::Literal(Value::String("ERROR".to_string()))),
        )),
    )
}

// -- Benchmarks --

fn bench_contains_cs(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_contains_cs");
    group.sample_size(20);

    let needle = "needle-value";
    let expr = expr_contains_cs(needle);

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_logs(size, needle, 0.01); // 1% match rate
        group.bench_with_input(BenchmarkId::new("rate_1pct", size), &logs, |b, logs| {
            b.iter(|| filter_logs(logs, &expr));
        });
    }

    group.finish();
}

fn bench_has_cs(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_has_cs");
    group.sample_size(20);

    let needle = "needle-value";
    let expr = expr_has_cs(needle);

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_logs(size, needle, 0.01);
        group.bench_with_input(BenchmarkId::new("rate_1pct", size), &logs, |b, logs| {
            b.iter(|| filter_logs(logs, &expr));
        });
    }

    group.finish();
}

fn bench_has_ci(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_has_ci");
    group.sample_size(20);

    let needle = "needle-value";
    let expr = expr_has(needle);

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_logs(size, needle, 0.01);
        group.bench_with_input(BenchmarkId::new("rate_1pct", size), &logs, |b, logs| {
            b.iter(|| filter_logs(logs, &expr));
        });
    }

    group.finish();
}

fn bench_match_rate(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_match_rate");
    group.sample_size(20);

    let needle = "needle-value";
    let expr = expr_contains_cs(needle);
    let size = 100_000;

    for rate in [0.0, 0.01, 0.10, 0.50, 1.0] {
        let logs = generate_logs(size, needle, rate);
        let label = format!("{}pct", (rate * 100.0) as usize);
        group.bench_with_input(BenchmarkId::new(label, size), &logs, |b, logs| {
            b.iter(|| filter_logs(logs, &expr));
        });
    }

    group.finish();
}

fn bench_compound_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_compound");
    group.sample_size(20);

    let needle = "needle-value";
    let expr = expr_compound(needle);

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_logs(size, needle, 0.01);
        group.bench_with_input(BenchmarkId::new("contains_and_eq", size), &logs, |b, logs| {
            b.iter(|| filter_logs(logs, &expr));
        });
    }

    group.finish();
}

fn bench_needle_length(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_needle_length");
    group.sample_size(20);

    let size = 100_000;

    for (label, needle) in [
        ("short_3", "abc"),
        ("medium_12", "needle-value"),
        ("long_50", "this-is-a-very-long-needle-string-for-benchmarking"),
    ] {
        let expr = expr_contains_cs(needle);
        let logs = generate_logs(size, needle, 0.01);
        group.bench_with_input(BenchmarkId::new(label, size), &logs, |b, logs| {
            b.iter(|| filter_logs(logs, &expr));
        });
    }

    group.finish();
}

fn bench_no_match_field_missing(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_field_missing");
    group.sample_size(20);

    // Filter on a field that doesn't exist in the logs
    let expr = Expr::Contains(
        Box::new(Expr::Field(field_unwrap!("nonexistent"))),
        Box::new(Expr::Literal(Value::String("needle".to_string()))),
    );

    for size in [1_000, 10_000, 100_000] {
        let logs = generate_logs(size, "needle", 0.0);
        group.bench_with_input(BenchmarkId::new("missing_field", size), &logs, |b, logs| {
            b.iter(|| filter_logs(logs, &expr));
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(5));
    targets =
        bench_contains_cs,
        bench_has_cs,
        bench_has_ci,
        bench_match_rate,
        bench_compound_filter,
        bench_needle_length,
        bench_no_match_field_missing,
}

criterion_main!(benches);
