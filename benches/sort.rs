use std::{cmp::Ordering, str::FromStr};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use miso_workflow::sort::columnar_sort;
use miso_workflow_types::{
    field::Field,
    field_unwrap,
    log::Log,
    sort::{NullsOrder, Sort, SortOrder},
    value::Value,
};
use rayon::slice::ParallelSliceMut;
use time::{Duration, OffsetDateTime};

fn get_field_value<'a>(log: &'a Log, field: &Field) -> Option<&'a Value> {
    if field.len() == 1 && field[0].arr_indices.is_empty() {
        log.get(&field[0].name)
    } else {
        let mut obj = log;
        for key in &field[..field.len() - 1] {
            obj = match obj.get(&key.name) {
                Some(Value::Object(map)) => map,
                _ => return None,
            };
        }
        let last = field.last().unwrap();
        obj.get(&last.name)
    }
}

struct SortComparator {
    by: Vec<Field>,
    sort_orders: Vec<SortOrder>,
    nulls_orders: Vec<NullsOrder>,
}

impl SortComparator {
    fn new(sorts: &[Sort]) -> Self {
        Self {
            by: sorts.iter().map(|s| s.by.clone()).collect(),
            sort_orders: sorts.iter().map(|s| s.order).collect(),
            nulls_orders: sorts.iter().map(|s| s.nulls).collect(),
        }
    }
}

fn cmp_logs(a: &Log, b: &Log, config: &SortComparator) -> Ordering {
    for ((key, sort_order), nulls_order) in config
        .by
        .iter()
        .zip(&config.sort_orders)
        .zip(&config.nulls_orders)
    {
        let a_val = get_field_value(a, key).unwrap_or(&Value::Null);
        let b_val = get_field_value(b, key).unwrap_or(&Value::Null);
        let mut any_null = true;
        let ordering = match (a_val, b_val, nulls_order) {
            (Value::Null, Value::Null, _) => Ordering::Equal,
            (Value::Null, _, NullsOrder::First) => Ordering::Less,
            (_, Value::Null, NullsOrder::First) => Ordering::Greater,
            (Value::Null, _, NullsOrder::Last) => Ordering::Greater,
            (_, Value::Null, NullsOrder::Last) => Ordering::Less,
            _ => {
                any_null = false;
                a_val.cmp(b_val)
            }
        };

        if ordering == Ordering::Equal {
            continue;
        }

        if any_null {
            return ordering;
        }

        return if *sort_order == SortOrder::Asc {
            ordering
        } else {
            ordering.reverse()
        };
    }

    Ordering::Equal
}

fn sort_row_based(mut logs: Vec<Log>, sorts: &[Sort]) -> Vec<Log> {
    let config = SortComparator::new(sorts);
    logs.sort_unstable_by(|a, b| cmp_logs(a, b, &config));
    logs
}

fn sort_row_based_parallel(mut logs: Vec<Log>, sorts: &[Sort]) -> Vec<Log> {
    let config = SortComparator::new(sorts);
    logs.par_sort_unstable_by(|a, b| cmp_logs(a, b, &config));
    logs
}

fn bench_all_impls(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    logs: &Vec<Log>,
    sorts: &[Sort],
    size: usize,
) {
    group.bench_with_input(BenchmarkId::new("row_based", size), logs, |b, logs| {
        b.iter(|| sort_row_based(logs.clone(), sorts));
    });
    group.bench_with_input(
        BenchmarkId::new("row_based_parallel", size),
        logs,
        |b, logs| {
            b.iter(|| sort_row_based_parallel(logs.clone(), sorts));
        },
    );
    group.bench_with_input(BenchmarkId::new("columnar", size), logs, |b, logs| {
        b.iter(|| columnar_sort(logs.clone(), sorts).unwrap());
    });
}

#[inline]
fn rand_bool(probability: f64, seed: usize) -> bool {
    if probability <= 0.0 {
        return false;
    }
    let hash = (seed.wrapping_mul(2654435761) >> 16) % 1000;
    (hash as f64) < (probability * 1000.0)
}

fn generate_timestamp_logs(count: usize, null_pct: f64) -> Vec<Log> {
    let base_time = OffsetDateTime::now_utc();
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            let offset_secs = ((i * 12345) % count) as i64;
            if rand_bool(null_pct, i) {
                log.insert("@timestamp".to_string(), Value::Null);
            } else {
                let ts = base_time + Duration::seconds(offset_secs);
                log.insert("@timestamp".to_string(), Value::Timestamp(ts));
            }
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_integer_logs(count: usize, null_pct: f64) -> Vec<Log> {
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            let value = ((i * 7919) % count) as i64;
            if rand_bool(null_pct, i) {
                log.insert("value".to_string(), Value::Null);
            } else {
                log.insert("value".to_string(), Value::Int(value));
            }
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_uint_logs(count: usize, null_pct: f64) -> Vec<Log> {
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            let value = ((i * 7919) % count) as u64;
            if rand_bool(null_pct, i) {
                log.insert("value".to_string(), Value::Null);
            } else {
                log.insert("value".to_string(), Value::UInt(value));
            }
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_float_logs(count: usize, null_pct: f64) -> Vec<Log> {
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            let value = ((i * 31337) % count) as f64 / count as f64 * 1000.0;
            if rand_bool(null_pct, i) {
                log.insert("value".to_string(), Value::Null);
            } else {
                log.insert("value".to_string(), Value::Float(value));
            }
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_bool_logs(count: usize, null_pct: f64) -> Vec<Log> {
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            if rand_bool(null_pct, i) {
                log.insert("value".to_string(), Value::Null);
            } else {
                log.insert("value".to_string(), Value::Bool(i % 2 == 0));
            }
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_string_logs(count: usize, null_pct: f64) -> Vec<Log> {
    let names: Vec<String> = (0..count)
        .map(|i| format!("user_{:08x}", (i * 2654435761) % 0xFFFFFFFF))
        .collect();

    (0..count)
        .map(|i| {
            let mut log = Log::new();
            if rand_bool(null_pct, i) {
                log.insert("value".to_string(), Value::Null);
            } else {
                log.insert("value".to_string(), Value::String(names[i].clone()));
            }
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_timespan_logs(count: usize, null_pct: f64) -> Vec<Log> {
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            let nanos = ((i * 12345) % count) as i64 * 1_000_000;
            if rand_bool(null_pct, i) {
                log.insert("value".to_string(), Value::Null);
            } else {
                log.insert(
                    "value".to_string(),
                    Value::Timespan(Duration::nanoseconds(nanos)),
                );
            }
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_array_logs(count: usize, null_pct: f64) -> Vec<Log> {
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            if rand_bool(null_pct, i) {
                log.insert("value".to_string(), Value::Null);
            } else {
                let arr = vec![
                    Value::Int((i % 10) as i64),
                    Value::String(format!("item-{}", i % 100)),
                ];
                log.insert("value".to_string(), Value::Array(arr));
            }
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_object_logs(count: usize, null_pct: f64) -> Vec<Log> {
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            if rand_bool(null_pct, i) {
                log.insert("value".to_string(), Value::Null);
            } else {
                let mut obj = Log::new();
                obj.insert("a".to_string(), Value::Int((i % 10) as i64));
                obj.insert("b".to_string(), Value::String(format!("val-{}", i % 100)));
                log.insert("value".to_string(), Value::Object(obj));
            }
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_multikey_logs(count: usize) -> Vec<Log> {
    let categories = ["A", "B", "C", "D", "E"];
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            let cat_idx = (i * 7) % categories.len();
            log.insert(
                "category".to_string(),
                Value::String(categories[cat_idx].to_string()),
            );
            let value = ((i * 12345) % count) as i64;
            log.insert("value".to_string(), Value::Int(value));
            log.insert("id".to_string(), Value::UInt(i as u64));
            log
        })
        .collect()
}

fn generate_wide_logs(count: usize) -> Vec<Log> {
    let base_time = OffsetDateTime::now_utc();
    (0..count)
        .map(|i| {
            let mut log = Log::new();
            let offset_secs = ((i * 12345) % count) as i64;
            let ts = base_time + Duration::seconds(offset_secs);
            log.insert("@timestamp".to_string(), Value::Timestamp(ts));
            log.insert("id".to_string(), Value::UInt(i as u64));
            log.insert(
                "request_id".to_string(),
                Value::String(format!("req-{:016x}", i)),
            );
            log.insert(
                "trace_id".to_string(),
                Value::String(format!("trace-{:032x}", i * 31337)),
            );
            log.insert(
                "span_id".to_string(),
                Value::String(format!("span-{:016x}", i * 7919)),
            );
            log.insert(
                "level".to_string(),
                Value::String(["INFO", "WARN", "ERROR", "DEBUG"][i % 4].to_string()),
            );
            log.insert(
                "logger".to_string(),
                Value::String(format!("com.example.service.Handler{}", i % 10)),
            );
            log.insert(
                "thread".to_string(),
                Value::String(format!("worker-{}", i % 8)),
            );
            log.insert(
                "host".to_string(),
                Value::String(format!("server-{:03}.dc1.example.com", i % 100)),
            );
            log.insert(
                "pod".to_string(),
                Value::String(format!("service-{:08x}", i % 1000)),
            );
            log.insert(
                "container".to_string(),
                Value::String(format!("container-{}", i % 5)),
            );
            log.insert(
                "namespace".to_string(),
                Value::String("production".to_string()),
            );
            log.insert(
                "cluster".to_string(),
                Value::String("us-east-1".to_string()),
            );
            log.insert(
                "service".to_string(),
                Value::String("api-gateway".to_string()),
            );
            log.insert("version".to_string(), Value::String("1.2.3".to_string()));
            log.insert(
                "method".to_string(),
                Value::String(["GET", "POST", "PUT", "DELETE"][i % 4].to_string()),
            );
            log.insert(
                "path".to_string(),
                Value::String(format!("/api/v1/resources/{}", i % 1000)),
            );
            log.insert(
                "status_code".to_string(),
                Value::Int([200, 201, 400, 404, 500][i % 5] as i64),
            );
            log.insert(
                "duration_ms".to_string(),
                Value::Float((i % 1000) as f64 + 0.5),
            );
            log.insert(
                "bytes_sent".to_string(),
                Value::UInt((i * 1024) as u64 % 1000000),
            );
            log.insert(
                "message".to_string(),
                Value::String(format!("Request processed successfully for resource {}", i)),
            );
            log
        })
        .collect()
}

fn bench_sort_by_timestamp(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_timestamp");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("@timestamp"),
        order: SortOrder::Desc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_timestamp_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_by_integer(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_int");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("value"),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_integer_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_by_uint(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_uint");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("value"),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_uint_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_by_float(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_float");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("value"),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_float_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_by_bool(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_bool");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("value"),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_bool_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_by_string(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_string");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("value"),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_string_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_by_timespan(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_timespan");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("value"),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_timespan_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_by_array(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_array");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("value"),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 100_000] {
        let logs = generate_array_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_by_object(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_object");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("value"),
        order: SortOrder::Asc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 100_000] {
        let logs = generate_object_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_with_nulls(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_with_nulls");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("@timestamp"),
        order: SortOrder::Desc,
        nulls: NullsOrder::Last,
    }];

    let size = 100_000;
    for null_pct in [0.01, 0.05, 0.10, 0.25] {
        let logs = generate_timestamp_logs(size, null_pct);
        let label = (null_pct * 100.0) as usize;
        bench_all_impls(&mut group, &logs, &sorts, label);
    }

    group.finish();
}

fn bench_sort_multikey(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_multikey");
    group.sample_size(20);

    let sorts = vec![
        Sort {
            by: field_unwrap!("category"),
            order: SortOrder::Asc,
            nulls: NullsOrder::Last,
        },
        Sort {
            by: field_unwrap!("value"),
            order: SortOrder::Desc,
            nulls: NullsOrder::Last,
        },
    ];

    for size in [1_000, 10_000, 100_000, 1_000_000] {
        let logs = generate_multikey_logs(size);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_wide_logs(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_wide_logs");
    group.sample_size(20);

    let sorts = vec![Sort {
        by: field_unwrap!("@timestamp"),
        order: SortOrder::Desc,
        nulls: NullsOrder::Last,
    }];

    for size in [1_000, 10_000, 50_000] {
        let logs = generate_wide_logs(size);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

fn bench_sort_small_datasets(c: &mut Criterion) {
    let mut group = c.benchmark_group("sort_small");
    group.sample_size(50);

    let sorts = vec![Sort {
        by: field_unwrap!("@timestamp"),
        order: SortOrder::Desc,
        nulls: NullsOrder::Last,
    }];

    for size in [10, 50, 100, 500] {
        let logs = generate_timestamp_logs(size, 0.0);
        bench_all_impls(&mut group, &logs, &sorts, size);
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().measurement_time(std::time::Duration::from_secs(5));
    targets =
        bench_sort_by_timestamp,
        bench_sort_by_integer,
        bench_sort_by_uint,
        bench_sort_by_float,
        bench_sort_by_bool,
        bench_sort_by_string,
        bench_sort_by_timespan,
        bench_sort_by_array,
        bench_sort_by_object,
        bench_sort_with_nulls,
        bench_sort_multikey,
        bench_sort_wide_logs,
        bench_sort_small_datasets,
}

criterion_main!(benches);
