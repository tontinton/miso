use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use color_eyre::{
    eyre::{bail, WrapErr},
    Result,
};
use futures_util::{FutureExt, TryStreamExt};
use miso_kql::parse;
use miso_optimizations::Optimizer;
use miso_server::config::ConnectorsMap;
use miso_server::query_to_workflow::to_workflow_steps;
use miso_workflow::Workflow;
use miso_workflow_types::value::Value;
use tokio::task::spawn_blocking;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, Instrument};

use super::test_cases::TestCase;
use super::TEST_LAYER;

pub use super::test_cases::{TestConnector, BASE_PREDICATE_PUSHDOWN_TESTS as TESTS};

const SLOW_TEST_LOG_DURATION: Duration = Duration::from_secs(60);

pub const INDEXES: [(&str, &str); 3] = [
    (
        "stack",
        include_str!("../resources/stackoverflow.posts.10.json"),
    ),
    (
        "stack_mirror",
        include_str!("../resources/stackoverflow.posts.10.json"),
    ),
    ("hdfs", include_str!("../resources/hdfs.logs.10.json")),
];

async fn same_results(
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

pub async fn run_tests(
    test_connector: TestConnector,
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
        .cloned()
        .collect();

    info!("Running {} test cases", tests_to_run.len());

    let handles: Vec<_> = tests_to_run
        .into_iter()
        .map(|tc| {
            let connectors = Arc::clone(&connectors);

            let test_span = tracing::info_span!("test_execution", test_name = tc.name);

            tokio::spawn(
                async move {
                    let start = Instant::now();

                    info!("Running test: {}", tc.name);

                    let test_name_for_monitor = tc.name;
                    let monitor_handle = tokio::spawn(
                        async move {
                            sleep(SLOW_TEST_LOG_DURATION).await;
                            info!(
                                "SLOW TEST: {} is still running after 60s",
                                test_name_for_monitor
                            );
                        }
                        .in_current_span(),
                    );

                    let result = std::panic::AssertUnwindSafe(same_results(
                        &connectors,
                        tc.query,
                        tc.expected.for_connector(test_connector),
                        tc.count,
                        tc.name,
                    ))
                    .catch_unwind()
                    .await;

                    monitor_handle.abort();

                    let elapsed = start.elapsed();

                    match result {
                        Ok(Ok(())) => {
                            info!("Test passed: {}", tc.name);
                            Ok((tc.name, elapsed))
                        }
                        Ok(Err(e)) => {
                            info!("Test failed: {}", tc.name);
                            Err((tc.name, e))
                        }
                        Err(panic_payload) => {
                            let panic_msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                                s.to_string()
                            } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "unknown panic".to_string()
                            };
                            info!("Test panicked: {}", tc.name);
                            Err((tc.name, color_eyre::eyre::eyre!("panic: {}", panic_msg)))
                        }
                    }
                }
                .instrument(test_span),
            )
        })
        .collect();

    let mut failed_tests = Vec::new();
    let mut passed_tests = Vec::new();

    for handle in handles {
        match handle.await {
            Ok(Ok((name, duration))) => {
                passed_tests.push((name, duration));
            }
            Ok(Err((name, err))) => {
                failed_tests.push((name.to_string(), err));
            }
            Err(join_err) => {
                return Err(join_err.into());
            }
        }
    }

    if let Some(layer) = TEST_LAYER.get() {
        eprintln!("\n════════════════════════════════════════════════════════════════");
        eprintln!("TEST OUTPUT");
        eprintln!("════════════════════════════════════════════════════════════════\n");

        for (test_name, duration) in &passed_tests {
            let logs = layer.drain_logs(test_name);
            if !logs.is_empty() {
                eprintln!(
                    "\x1b[32m════════ Test: {} (PASSED in {:.2}s) ════════\x1b[0m",
                    test_name,
                    duration.as_secs_f64()
                );
                for log in logs {
                    eprintln!("{}", log);
                }
                eprintln!();
            }
        }

        if !passed_tests.is_empty() {
            eprintln!(
                "\x1b[32m════════════════════════════════════════════════════════════════\x1b[0m"
            );
        }

        for (test_name, err) in &failed_tests {
            let logs = layer.drain_logs(test_name);
            eprintln!(
                "\x1b[31m════════ Test: {} (FAILED) ════════\x1b[0m",
                test_name
            );
            if !logs.is_empty() {
                for log in logs {
                    eprintln!("{}", log);
                }
            }
            eprintln!("Error: {:#}", err);
            eprintln!();
        }

        if !failed_tests.is_empty() {
            eprintln!(
                "\x1b[31m════════════════════════════════════════════════════════════════\x1b[0m"
            );
        }
    }

    if !failed_tests.is_empty() {
        eprintln!("\n────────────────────────────────────────────────────────────────");
        eprintln!("FAILED TESTS:");
        for (test_name, _) in &failed_tests {
            eprintln!("  ✗ {}", test_name);
        }
        eprintln!("────────────────────────────────────────────────────────────────");
        bail!(
            "{} test(s) failed out of {}",
            failed_tests.len(),
            passed_tests.len() + failed_tests.len()
        );
    }

    Ok(())
}
