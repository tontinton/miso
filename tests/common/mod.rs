use std::collections::BTreeMap;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use color_eyre::{
    eyre::{bail, WrapErr},
    Result,
};
use futures_util::{FutureExt, TryStreamExt};
use miso_kql::parse;
use miso_optimizations::Optimizer;
use miso_server::http_server::ConnectorsMap;
use miso_server::query_to_workflow::to_workflow_steps;
use miso_workflow::Workflow;
use miso_workflow_types::value::Value;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::fmt::MakeWriter;

const SLOW_TEST_LOG_DURATION: Duration = Duration::from_secs(60);

pub const BASE_PREDICATE_PUSHDOWN_TESTS: &[TestCase] = &[
    TestCase {
        query: r#"test.stack | where acceptedAnswerId == 12446"#,
        expected: r#"test.stack"#,
        count: 1,
        name: "filter_eq",
    },
    TestCase {
        query: r#"test.stack | where body has_cs "VB.NET""#,
        expected: r#"test.stack"#,
        count: 1,
        name: "filter_has_cs",
    },
    TestCase {
        query: r#"test.stack | where acceptedAnswerId in (12446, 31)"#,
        expected: r#"test.stack"#,
        count: 2,
        name: "filter_in",
    },
    TestCase {
        query: r#"test.stack | where questionId == 11 and exists(answerId)"#,
        expected: r#"test.stack"#,
        count: 1,
        name: "filter_eq_and_exists",
    },
    TestCase {
        query: r#"test.stack | where @time == datetime(2008-07-31 22:17:57)"#,
        expected: r#"test.stack"#,
        count: 1,
        name: "filter_eq_timestamp",
    },
    TestCase {
        query: r#"test.stack | project acceptedAnswerId"#,
        expected: r#"test.stack"#,
        count: 10,
        name: "project_one_field",
    },
    TestCase {
        query: r#"test.stack | project acceptedAnswerId | count"#,
        expected: r#"test.stack"#,
        count: 1,
        name: "project_count",
    },
    TestCase {
        query: r#"test.stack | project acceptedAnswerId | summarize c=count()"#,
        expected: r#"test.stack"#,
        count: 1,
        name: "project_summarize",
    },
    TestCase {
        query: r#"test.stack | summarize c=count()"#,
        expected: r#"test.stack"#,
        count: 1,
        name: "summarize_only_count",
    },
    TestCase {
        query: r#"
    test.stack
    | summarize minQuestionId=min(questionId),
                maxQuestionId=max(questionId),
                sumQuestionId=sum(questionId),
                minTimestamp=min(@time),
                maxTimestamp=max(@time),
                c=count()
      by user
    "#,
        expected: r#"test.stack"#,
        count: 5,
        name: "summarize_min_max_count",
    },
    TestCase {
        query: r#"
    test.stack
    | summarize minQuestionId=min(questionId),
                maxQuestionId=max(questionId),
                avgQuestionId=avg(questionId),
                cifQuestionId=countif(exists(questionId)),
                sumQuestionId=sum(questionId),
                c=count()
      by bin(@time, 1h)
    "#,
        expected: r#"test.stack"#,
        count: 6,
        name: "summarize_min_max_count_by_bin_timestamp",
    },
    TestCase {
        query: r#"test.stack | summarize by user"#,
        expected: r#"test.stack"#,
        count: 5,
        name: "summarize_distinct",
    },
    TestCase {
        query: r#"test.stack | distinct user"#,
        expected: r#"test.stack"#,
        count: 5,
        name: "distinct",
    },
    TestCase {
        query: r#"test.stack | distinct @time"#,
        expected: r#"test.stack"#,
        count: 10,
        name: "distinct_timestamp",
    },
    TestCase {
        query: r#"test.stack | summarize minQuestionId=min(questionId) by user | top 3 by minQuestionId"#,
        expected: r#"test.stack | top 3 by minQuestionId"#,
        count: 3,
        name: "summarize_then_topn",
    },
    TestCase {
        query: r#"test.stack | top 5 by questionId | summarize minQuestionId=min(questionId) by user"#,
        expected: r#"test.stack | summarize minQuestionId=min(questionId) by user"#,
        count: 3,
        name: "topn_then_summarize",
    },
    TestCase {
        query: r#"test.stack | count"#,
        expected: r#"test.stack"#,
        count: 1,
        name: "count",
    },
    TestCase {
        query: r#"test.stack | union (test.stack_mirror)"#,
        expected: r#"test.stack"#,
        count: 20,
        name: "union",
    },
    TestCase {
        query: r#"test.stack | union (test.hdfs)"#,
        expected: r#"test.stack | union (test.hdfs)"#,
        count: 20,
        name: "union_not_same_timestamp_field",
    },
    TestCase {
        query: r#"
    test.stack
    | union (test.stack_mirror)
    | where acceptedAnswerId < 100
    | top 1 by acceptedAnswerId
    "#,
        expected: r#"test.stack"#,
        count: 1,
        name: "union_filter_topn",
    },
    TestCase {
        query: r#"
    test.stack
    | where case(acceptedAnswerId > 50, "big", acceptedAnswerId > 10, "medium", "small") == "medium"
    "#,
        expected: r#"test.stack"#,
        count: 2,
        name: "filter_case",
    },
    TestCase {
        query: r#"test.stack | sort by @time | take 3"#,
        expected: r#"test.stack"#,
        count: 3,
        name: "top_n_timestamp",
    },
    TestCase {
        query: r#"
    test.stack
    | summarize minQuestionId=min(questionId),
                maxQuestionId=max(questionId),
                avgQuestionId=avg(questionId),
                dcountUser=dcount(user),
                cifQuestionId=countif(exists(questionId)),
                sumQuestionId=sum(questionId),
                minTimestamp=min(@time),
                maxTimestamp=max(@time),
                c=count()
      by bin(answerId, 5)
    "#,
        expected: r#"test.stack"#,
        count: 2,
        name: "summarize_min_max_count_by_bin_with_dcount",
    },
];

#[derive(Clone, Copy)]
pub struct TestCase {
    pub query: &'static str,
    pub expected: &'static str,
    pub count: usize,
    pub name: &'static str,
}

#[derive(Clone)]
struct TestLogWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl TestLogWriter {
    fn new() -> (Self, Arc<Mutex<Vec<u8>>>) {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                buffer: Arc::clone(&buffer),
            },
            buffer,
        )
    }
}

impl Write for TestLogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buffer.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.buffer.lock().unwrap().flush()
    }
}

impl<'a> MakeWriter<'a> for TestLogWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

pub async fn predicate_pushdown_same_results(
    connectors: &ConnectorsMap,
    query: &str,
    query_after_optimizations: &str,
    count: usize,
    test_name: &str,
) -> Result<()> {
    let steps = to_workflow_steps(
        connectors,
        &BTreeMap::new(),
        parse(query).expect("parse KQL"),
    )
    .expect("to workflow steps");

    let expected_after_optimizations_steps = to_workflow_steps(
        connectors,
        &BTreeMap::new(),
        parse(query_after_optimizations).expect("parse expected KQL"),
    )
    .expect("to expected workflow steps");

    let default_optimizer = Optimizer::default();
    let no_pushdown_optimizer = Optimizer::empty();

    let steps_cloned = steps.clone();
    let predicate_pushdown_steps =
        spawn_blocking(move || default_optimizer.optimize(steps_cloned)).await?;

    let pushdown_workflow = Workflow::new(predicate_pushdown_steps.clone());
    let expected_workflow = Workflow::new(expected_after_optimizations_steps.clone());
    info!("Pushdown workflow:\n{pushdown_workflow}");
    info!("Expected pushdown workflow:\n{expected_workflow}");

    assert_eq!(
        predicate_pushdown_steps, expected_after_optimizations_steps,
        "[{}] query predicates should have been equal to expected steps after optimization",
        test_name
    );

    let no_predicate_pushdown_steps =
        spawn_blocking(move || no_pushdown_optimizer.optimize(steps)).await?;
    let no_pushdown_workflow = Workflow::new(no_predicate_pushdown_steps);
    info!("No pushdown workflow:\n{no_pushdown_workflow}");

    let cancel1 = CancellationToken::new();
    let cancel2 = CancellationToken::new();

    let mut pushdown_stream = pushdown_workflow
        .execute(cancel1)
        .context("execute predicate pushdown workflow")?;
    let mut no_pushdown_stream = no_pushdown_workflow
        .execute(cancel2)
        .context("execute no predicate pushdown workflow")?;

    let mut pushdown_results = Vec::with_capacity(count);
    let mut no_pushdown_results = Vec::with_capacity(count);

    let mut pushdown_done = false;
    let mut no_pushdown_done = false;

    loop {
        if pushdown_done && no_pushdown_done {
            break;
        }

        tokio::select! {
            item = pushdown_stream.try_next(), if !pushdown_done => {
                match item.context("predicate pushdown workflow failure")? {
                    Some(log) => {
                        pushdown_results.push(Value::Object(log));
                    }
                    None => {
                        assert_eq!(
                            count,
                            pushdown_results.len(),
                            "[{}] number of logs returned in pushdown query is wrong",
                            test_name
                        );
                        pushdown_done = true;
                    }
                }
            }
            item = no_pushdown_stream.try_next(), if !no_pushdown_done => {
                match item.context("non predicate pushdown workflow failure")? {
                    Some(log) => {
                        no_pushdown_results.push(Value::Object(log));
                    }
                    None => {
                        assert_eq!(
                            count,
                            no_pushdown_results.len(),
                            "[{}] number of logs returned in non pushdown query is wrong",
                            test_name
                        );
                        no_pushdown_done = true;
                    }
                }
            }
        }
    }

    pushdown_results.sort();
    no_pushdown_results.sort();

    assert_eq!(
        pushdown_results, no_pushdown_results,
        "[{}] results of pushdown query should equal results of non pushdown query, after sorting",
        test_name
    );

    Ok(())
}

pub async fn run_predicate_pushdown_tests(
    connectors: Arc<ConnectorsMap>,
    test_case_slices: &[&[TestCase]],
) -> Result<()> {
    let test_filter = std::env::var("TEST_FILTER").ok();
    let tests_to_run: Vec<TestCase> = test_case_slices
        .iter()
        .flat_map(|slice| slice.iter())
        .filter(|tc| {
            if let Some(ref filter) = test_filter {
                tc.name.contains(filter)
            } else {
                true
            }
        })
        .copied()
        .collect();

    info!("Running {} test cases", tests_to_run.len());

    let handles: Vec<_> = tests_to_run
        .into_iter()
        .map(|tc| {
            let connectors = Arc::clone(&connectors);
            tokio::spawn(async move {
                let (writer, log_buffer) = TestLogWriter::new();
                let start = Instant::now();

                let subscriber = tracing_subscriber::fmt()
                    .with_writer(writer)
                    .with_ansi(false)
                    .finish();

                let result = tracing::dispatcher::with_default(
                    &tracing::Dispatch::new(subscriber),
                    || async move {
                        info!("Running test: {}", tc.name);

                        let test_name_for_monitor = tc.name;
                        let monitor_handle = tokio::spawn(async move {
                            sleep(SLOW_TEST_LOG_DURATION).await;
                            info!(
                                "SLOW TEST: {} is still running after 60s",
                                test_name_for_monitor
                            );
                        });

                        let result = std::panic::AssertUnwindSafe(predicate_pushdown_same_results(
                            &connectors,
                            tc.query,
                            tc.expected,
                            tc.count,
                            tc.name,
                        ))
                        .catch_unwind()
                        .await;

                        monitor_handle.abort();
                        result
                    },
                )
                .await;

                let elapsed = start.elapsed();
                let logs = String::from_utf8_lossy(&log_buffer.lock().unwrap()).to_string();

                match result {
                    Ok(Ok(())) => Ok((tc.name, elapsed, logs)),
                    Ok(Err(e)) => Err((tc.name, e, logs)),
                    Err(panic_payload) => {
                        let panic_msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "unknown panic".to_string()
                        };
                        Err((
                            tc.name,
                            color_eyre::eyre::eyre!("panic: {}", panic_msg),
                            logs,
                        ))
                    }
                }
            })
        })
        .collect();

    let mut failed_tests = Vec::new();
    let mut passed_tests = Vec::new();

    for handle in handles {
        match handle.await {
            Ok(Ok((name, duration, _logs))) => {
                passed_tests.push((name, duration));
            }
            Ok(Err((name, err, logs))) => {
                failed_tests.push((name.to_string(), err, logs));
            }
            Err(join_err) => {
                return Err(join_err.into());
            }
        }
    }

    if !failed_tests.is_empty() {
        eprintln!("\n──────────────────────────────────────────────────────────────────");
        for (test_name, err, logs) in &failed_tests {
            eprintln!("\n{}", test_name);
            eprintln!("──────────────────────────────────────────────────────────────────");
            eprintln!("Error: {:#}", err);
            if !logs.is_empty() {
                eprintln!("\nLogs:\n{}", logs);
            }
        }
        eprintln!("──────────────────────────────────────────────────────────────────");
        eprintln!("FAILED TESTS:");
        for (test_name, _, _) in &failed_tests {
            eprintln!("  ✗ {}", test_name);
        }
        bail!(
            "{} test(s) failed out of {}",
            failed_tests.len(),
            passed_tests.len() + failed_tests.len()
        );
    }

    Ok(())
}
