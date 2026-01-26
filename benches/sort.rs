use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use miso_common::rand::pseudo_random;
use miso_workflow::sort::{
    cmp_logs, parallel_quicksort, parallel_sort, sort_num_threads, SortComparator,
};
use miso_workflow_types::{
    log::Log,
    sort::{NullsOrder, Sort, SortOrder},
    value::Value,
};
use rayon::{prelude::*, ThreadPoolBuilder};

fn create_logs(n: usize) -> Vec<Log> {
    (0..n)
        .map(|i| {
            let mut log = Log::new();
            log.insert("x".to_string(), Value::Int(pseudo_random(i) as i64));
            log
        })
        .collect()
}

fn bench_parallel_sort(c: &mut Criterion) {
    let comparator = SortComparator::new(vec![Sort {
        by: "x".parse().unwrap(),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }]);
    let mut group = c.benchmark_group("parallel_sort");
    for size in [10_000, 50_000, 100_000, 500_000, 1_000_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let logs = create_logs(size);
                parallel_sort(logs, &comparator, None).unwrap()
            });
        });
    }
    group.finish();
}

fn bench_parallel_quicksort(c: &mut Criterion) {
    let comparator = SortComparator::new(vec![Sort {
        by: "x".parse().unwrap(),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }]);
    let mut group = c.benchmark_group("parallel_quicksort");
    for size in [10_000, 50_000, 100_000, 500_000, 1_000_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut logs = create_logs(size);
                parallel_quicksort(&mut logs, &comparator, None);
                logs
            });
        });
    }
    group.finish();
}

fn bench_rayon_par_sort(c: &mut Criterion) {
    let comparator = SortComparator::new(vec![Sort {
        by: "x".parse().unwrap(),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }]);

    let pool = ThreadPoolBuilder::new()
        .num_threads(sort_num_threads())
        .build()
        .unwrap();

    let mut group = c.benchmark_group("rayon_par_sort");
    for size in [10_000, 50_000, 100_000, 500_000, 1_000_000] {
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.iter(|| {
                let mut logs = create_logs(size);
                pool.install(|| {
                    logs.par_sort_unstable_by(|a, b| cmp_logs(a, b, &comparator));
                });
                logs
            });
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(Duration::from_secs(10)).sample_size(25);
    targets = bench_parallel_sort, bench_parallel_quicksort, bench_rayon_par_sort,
}
criterion_main!(benches);
