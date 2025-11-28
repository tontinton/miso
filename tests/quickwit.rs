use std::{collections::BTreeMap, sync::Arc, time::Duration};

use collection_macros::btreemap;
use color_eyre::{
    eyre::{bail, Context, OptionExt},
    Result,
};
use ctor::ctor;
use futures_util::{future::try_join_all, TryStreamExt};
use miso_connectors::{
    quickwit::{QuickwitConfig, QuickwitConnector},
    Connector, ConnectorState,
};
use miso_kql::parse;
use miso_optimizations::Optimizer;
use miso_server::{http_server::ConnectorsMap, query_to_workflow::to_workflow_steps};
use miso_workflow::Workflow;
use miso_workflow_types::value::Value;
use reqwest::{header::CONTENT_TYPE, Client, Response};
use serde::Serialize;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::task::spawn_blocking;
use tokio_retry::{strategy::FixedInterval, Retry};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[ctor]
fn init() {
    color_eyre::install().unwrap();
    tracing_subscriber::fmt::init();
}

const INDEXES: [(&str, &str); 3] = [
    (
        "stack",
        include_str!("./resources/stackoverflow.posts.10.json"),
    ),
    (
        "stack_mirror",
        include_str!("./resources/stackoverflow.posts.10.json"),
    ),
    ("hdfs", include_str!("./resources/hdfs.logs.10.json")),
];

const QUICKWIT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

struct QuickwitImage {
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

async fn run_quickwit_image() -> QuickwitImage {
    let container = GenericImage::new("quickwit/quickwit", "edge")
        .with_exposed_port(7280.tcp())
        .with_wait_for(WaitFor::message_on_stdout("REST server is ready"))
        .with_cmd(["run"])
        .start()
        .await
        .expect("quickwit container to start");
    let port = container
        .get_host_port_ipv4(7280)
        .await
        .expect("get quickwit container exposed port");
    info!("Quickwit running & listening on port: {port}");
    QuickwitImage {
        _container: container,
        port,
    }
}

async fn get_quickwit_connector_map(image: &QuickwitImage) -> Result<ConnectorsMap> {
    let url = format!("http://127.0.0.1:{}", image.port);
    let client = Client::new();

    let mut create_index_futures = Vec::with_capacity(INDEXES.len());

    for stackoverflow_index_name in ["stack", "stack_mirror"] {
        let fut = create_index(
            &client,
            &url,
            stackoverflow_index_name,
            DocMapping {
                field_mappings: vec![FieldMapping {
                    name: "creationDate".to_string(),
                    ty: "datetime".to_string(),
                    fast: Some(true),
                    fast_precision: Some("seconds".to_string()),
                    input_formats: vec!["rfc3339".to_string()],
                    ..Default::default()
                }],
                timestamp_field: "creationDate".to_string(),
                ..Default::default()
            },
        );
        create_index_futures.push(fut);
    }

    create_index_futures.push(create_index(
        &client,
        &url,
        "hdfs",
        DocMapping {
            field_mappings: vec![FieldMapping {
                name: "timestamp".to_string(),
                ty: "datetime".to_string(),
                fast: Some(true),
                fast_precision: Some("seconds".to_string()),
                input_formats: vec!["unix_timestamp".to_string()],
                ..Default::default()
            }],
            timestamp_field: "timestamp".to_string(),
            ..Default::default()
        },
    ));

    try_join_all(create_index_futures).await?;

    try_join_all(
        INDEXES
            .iter()
            .map(|(index_name, data)| write_to_index(&client, &url, index_name, data))
            .collect::<Vec<_>>(),
    )
    .await?;

    let config = QuickwitConfig::new_with_interval(url, QUICKWIT_REFRESH_INTERVAL);
    let connector = Arc::new(QuickwitConnector::new(config)) as Arc<dyn Connector>;
    let connector_state = Arc::new(ConnectorState::new(connector.clone()));

    Retry::spawn(
        FixedInterval::new(QUICKWIT_REFRESH_INTERVAL).take(3),
        || async {
            for (index_name, _) in INDEXES {
                connector
                    .does_collection_exist(index_name)
                    .then_some(())
                    .ok_or_eyre(format!(
                        "timeout waiting for '{index_name}' collection to exist"
                    ))?;
            }
            Ok::<(), color_eyre::eyre::Error>(())
        },
    )
    .await?;

    Ok(btreemap! { "test".to_string() => connector_state })
}

fn default_version() -> String {
    "0.8".to_string()
}

#[derive(Serialize, Default)]
struct FieldMapping {
    name: String,

    #[serde(rename = "type")]
    ty: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    tokenizer: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    record: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    stored: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    fast: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    fast_precision: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    input_formats: Vec<String>,
}

#[derive(Serialize)]
struct DynamicMapping {
    indexed: bool,
    stored: bool,
    tokenizer: String,
    expand_dots: bool,
    fast: bool,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    record: Option<String>,
}

impl Default for DynamicMapping {
    fn default() -> Self {
        Self {
            indexed: true,
            stored: true,
            tokenizer: "default".to_string(),
            expand_dots: true,
            fast: true,
            record: Some("position".to_string()),
        }
    }
}

#[derive(Serialize, Default)]
struct DocMapping {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mode: Option<String>,
    dynamic_mapping: DynamicMapping,
    field_mappings: Vec<FieldMapping>,
    timestamp_field: String,
}

#[derive(Serialize)]
struct CreateIndex {
    #[serde(default = "default_version")]
    version: String,
    index_id: String,
    doc_mapping: DocMapping,
}

impl Default for CreateIndex {
    fn default() -> Self {
        Self {
            version: default_version(),
            index_id: String::default(),
            doc_mapping: DocMapping::default(),
        }
    }
}

async fn void_response_to_err(url: &str, response: Response) -> Result<()> {
    let status = response.status();
    if !status.is_success() {
        if let Ok(text) = response.text().await {
            bail!("POST {} failed with status {}: {}", url, status, text);
        } else {
            bail!("POST {} failed with status {}", url, status);
        }
    }
    Ok(())
}

async fn create_index(
    client: &Client,
    base_url: &str,
    index_name: &str,
    doc_mapping: DocMapping,
) -> Result<()> {
    let url = format!("{base_url}/api/v1/indexes");
    let response = client
        .post(&url)
        .json(&CreateIndex {
            index_id: index_name.to_string(),
            doc_mapping,
            ..Default::default()
        })
        .send()
        .await
        .with_context(|| format!("POST create index: {index_name}"))?;

    void_response_to_err(&url, response).await?;

    info!("Index '{}' created successfully", index_name);
    Ok(())
}

async fn write_to_index(
    client: &Client,
    base_url: &str,
    index_name: &str,
    data: &'static str,
) -> Result<()> {
    let url = format!("{base_url}/api/v1/{index_name}/ingest");
    let response = client
        .post(&url)
        .query(&[("commit", "force")])
        .header(CONTENT_TYPE, "application/json")
        .body(data)
        .send()
        .await
        .with_context(|| format!("POST ingest stackoverflow posts into index: {index_name}"))?;

    void_response_to_err(&url, response).await?;

    info!(
        "Index '{}' ingested stackoverflow posts successfully",
        index_name
    );
    Ok(())
}

#[derive(Clone, Copy)]
struct TestCase {
    query: &'static str,
    expected: &'static str,
    count: usize,
    name: &'static str,
}

async fn predicate_pushdown_same_results(
    connectors: &ConnectorsMap,
    query: &str,
    query_after_optimizations: &str,
    count: usize,
) -> Result<()> {
    let steps = to_workflow_steps(
        &connectors,
        &BTreeMap::new(),
        parse(query).expect("parse KQL"),
    )
    .expect("to workflow steps");

    let expected_after_optimizations_steps = to_workflow_steps(
        &connectors,
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
        "query predicates should have been equal to expected steps after optimization"
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
                            "number of logs returned in pushdown query is wrong"
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
                            "number of logs returned in non pushdown query is wrong"
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
        "results of pushdown query should equal results of non pushdown query, after sorting"
    );

    Ok(())
}

const PREDICATE_PUSHDOWN_TESTS: &[TestCase] = &[
    TestCase {
        query: r#"test.stack | sort by @time | take 3"#,
        expected: r#"test.stack"#,
        count: 3,
        name: "top_n",
    },
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
        query: r#"
    test.stack
    | where case(acceptedAnswerId > 50, "big", acceptedAnswerId > 10, "medium", "small") == "medium"
    "#,
        expected: r#"test.stack"#,
        count: 2,
        name: "filter_case",
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
                avgQuestionId=max(questionId),
                cifQuestionId=countif(exists(questionId)),
                sumQuestionId=sum(questionId),
                minTimestamp=min(@time),
                maxTimestamp=max(@time),
                c=count()
      by bin(answerId, 5)
    "#,
        expected: r#"test.stack"#,
        count: 2,
        name: "summarize_min_max_count_by_bin",
    },
    TestCase {
        query: r#"
    test.stack
    | summarize minQuestionId=min(questionId),
                maxQuestionId=max(questionId),
                avgQuestionId=max(questionId),
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
];

#[tokio::test]
async fn test_quickwit_predicate_pushdown() -> Result<()> {
    let image = run_quickwit_image().await;
    let connectors = get_quickwit_connector_map(&image).await?;
    let connectors = Arc::new(connectors);

    let test_filter = std::env::var("TEST_FILTER").ok();
    let tests_to_run: Vec<_> = PREDICATE_PUSHDOWN_TESTS
        .iter()
        .filter(|tc| {
            if let Some(ref filter) = test_filter {
                tc.name.contains(filter)
            } else {
                true
            }
        })
        .collect();

    info!("Running {} test cases", tests_to_run.len());

    let handles: Vec<_> = tests_to_run
        .into_iter()
        .map(|tc| {
            let connectors = Arc::clone(&connectors);
            tokio::spawn(async move {
                info!("Running test: {}", tc.name);
                predicate_pushdown_same_results(&connectors, tc.query, tc.expected, tc.count)
                    .await
                    .map_err(|e| format!("Test '{}' failed: {}", tc.name, e))
            })
        })
        .collect();

    let mut errors = Vec::new();
    for handle in handles {
        if let Err(e) = handle.await? {
            errors.push(e);
        }
    }

    if !errors.is_empty() {
        bail!("Test failures:\n{}", errors.join("\n"));
    }

    Ok(())
}
