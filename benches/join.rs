use std::str::FromStr;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use miso_workflow::{join::join_iter, CHANNEL_CAPACITY};
use miso_workflow_types::{
    field::Field,
    field_unwrap,
    join::{Join, JoinType},
    log::Log,
    value::Value,
};

const JOIN_KEY: &str = "join_key";

#[inline]
fn create_test_log(id: u64, field_key: &str, field_value: String) -> Log {
    let mut log = Log::new();
    log.insert("id".to_string(), Value::from(id.to_string()));
    log.insert(field_key.to_string(), Value::from(field_value));
    log
}

fn create_test_stream(
    left_size: usize,
    right_size: usize,
    field_key: &str,
) -> impl Iterator<Item = (bool, Log)> {
    let left_logs = (0..left_size)
        .map(|i| {
            let value = format!("value{}", i % (left_size / 10).max(1));
            (true, create_test_log(i as u64, field_key, value))
        })
        .collect::<Vec<_>>();
    let right_logs = (0..right_size)
        .map(|i| {
            let value = format!("value{}", i % (right_size / 10).max(1));
            (false, create_test_log(i as u64, field_key, value))
        })
        .collect::<Vec<_>>();
    left_logs.into_iter().chain(right_logs)
}

fn run_join(config: Join, iter: impl Iterator<Item = (bool, Log)>) -> usize {
    let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);
    join_iter(config, iter, tx, None).unwrap();
    let mut count = 0;
    while rx.recv().is_ok() {
        count += 1;
    }
    count
}

fn bench_join_various_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_streams_sizes");
    let field = field_unwrap!(JOIN_KEY);
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let iter = create_test_stream(size, size, JOIN_KEY);
                let config = Join {
                    type_: JoinType::Inner,
                    on: (field.clone(), field.clone()),
                    ..Default::default()
                };
                run_join(config, iter)
            });
        });
    }
    group.finish();
}

fn bench_join_different_ratios(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_streams_ratios");
    let field = field_unwrap!(JOIN_KEY);
    let base_size = 1000;
    for ratio in [1, 5, 10].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(ratio), ratio, |b, &ratio| {
            b.iter(|| {
                let iter = create_test_stream(base_size, base_size * ratio, JOIN_KEY);
                let config = Join {
                    type_: JoinType::Inner,
                    on: (field.clone(), field.clone()),
                    ..Default::default()
                };
                run_join(config, iter)
            });
        });
    }
    group.finish();
}

fn bench_join_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_streams_types");
    let field = field_unwrap!(JOIN_KEY);
    let join_types = [
        JoinType::Inner,
        JoinType::Outer,
        JoinType::Left,
        JoinType::Right,
    ];

    for join_type in join_types {
        group.bench_with_input(
            BenchmarkId::from_parameter(join_type),
            &join_type,
            |b, join_type| {
                b.iter(|| {
                    let iter = create_test_stream(1000, 1000, JOIN_KEY);
                    let config = Join {
                        type_: *join_type,
                        on: (field.clone(), field.clone()),
                        ..Default::default()
                    };
                    run_join(config, iter)
                });
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(10));
    targets = bench_join_various_sizes, bench_join_different_ratios, bench_join_types,
}
criterion_main!(benches);
