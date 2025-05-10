use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use futures_util::{stream::iter, StreamExt};
use miso::{
    log::{Log, LogStream},
    workflow::join::{join_streams, Join, JoinType},
};
use serde_json::Value;

const JOIN_KEY: &str = "join_key";

#[inline]
fn create_test_log(id: u64, field_key: &str, field_value: String) -> Log {
    let mut log = Log::new();
    log.insert("id".to_string(), Value::from(id.to_string()));
    log.insert(field_key.to_string(), Value::from(field_value));
    log
}

fn create_test_stream(size: usize, field_key: &str) -> LogStream {
    let logs = (0..size)
        .map(|i| {
            let value = format!("value{}", i % (size / 10).max(1));
            create_test_log(i as u64, field_key, value)
        })
        .collect::<Vec<_>>();
    Box::pin(iter(logs))
}

fn bench_join_various_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_streams_sizes");
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    let left_stream = create_test_stream(size, JOIN_KEY);
                    let right_stream = create_test_stream(size, JOIN_KEY);
                    let config = Join {
                        type_: JoinType::Inner,
                        on: (JOIN_KEY.to_string(), JOIN_KEY.to_string()),
                        ..Default::default()
                    };

                    let stream = join_streams(config, left_stream, right_stream).await;
                    let count = stream.fold(0, |acc, _| async move { acc + 1 }).await;
                    count
                });
        });
    }
    group.finish();
}

fn bench_join_different_ratios(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_streams_ratios");
    let base_size = 1000;
    for ratio in [1, 5, 10].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(ratio), ratio, |b, &ratio| {
            b.to_async(tokio::runtime::Runtime::new().unwrap())
                .iter(|| async {
                    let left_stream = create_test_stream(base_size, JOIN_KEY);
                    let right_stream = create_test_stream(base_size * ratio, JOIN_KEY);
                    let config = Join {
                        type_: JoinType::Inner,
                        on: (JOIN_KEY.to_string(), JOIN_KEY.to_string()),
                        ..Default::default()
                    };

                    let stream = join_streams(config, left_stream, right_stream).await;
                    let count = stream.fold(0, |acc, _| async move { acc + 1 }).await;
                    count
                });
        });
    }
    group.finish();
}

fn bench_join_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_streams_types");
    let join_types = [
        JoinType::Inner,
        JoinType::Outer,
        JoinType::Left,
        JoinType::Right,
    ];

    for join_type in join_types {
        group.bench_with_input(
            BenchmarkId::from_parameter(&join_type),
            &join_type,
            |b, join_type| {
                b.to_async(tokio::runtime::Runtime::new().unwrap())
                    .iter(|| async {
                        let left_stream = create_test_stream(1000, JOIN_KEY);
                        let right_stream = create_test_stream(1000, JOIN_KEY);
                        let config = Join {
                            type_: join_type.clone(),
                            on: (JOIN_KEY.to_string(), JOIN_KEY.to_string()),
                            ..Default::default()
                        };

                        let stream = join_streams(config, left_stream, right_stream).await;
                        let count = stream.fold(0, |acc, _| async move { acc + 1 }).await;
                        count
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
