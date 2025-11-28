use std::{collections::BTreeMap, time::Duration};

use collection_macros::btreemap;
use color_eyre::{
    eyre::{bail, Context, OptionExt},
    Result,
};
use ctor::ctor;
use futures_util::{future::try_join_all, TryStreamExt};
use miso_connectors::{
    elasticsearch::{ElasticsearchConfig, ElasticsearchConnector},
    Connector, ConnectorState,
};
use miso_kql::parse;
use miso_optimizations::Optimizer;
use miso_server::{http_server::ConnectorsMap, query_to_workflow::to_workflow_steps};
use miso_workflow::Workflow;
use miso_workflow_types::value::Value;
use reqwest::{header::CONTENT_TYPE, Client, Response};
use serde::Serialize;
use std::sync::Arc;
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

const ELASTICSEARCH_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

struct ElasticsearchImage {
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

async fn run_elasticsearch_image() -> ElasticsearchImage {
    let container = GenericImage::new("opensearchproject/opensearch", "2.11.0")
        .with_wait_for(WaitFor::message_on_stdout("started"))
        .with_exposed_port(9200.tcp())
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms256m -Xmx256m")
        .start()
        .await
        .expect("elasticsearch container to start");

    let port = container
        .get_host_port_ipv4(9200)
        .await
        .expect("get opensearch port");

    ElasticsearchImage {
        _container: container,
        port,
    }
}

#[derive(Serialize)]
struct IndexMapping {
    mappings: IndexProperties,
}

#[derive(Serialize)]
struct IndexProperties {
    properties: serde_json::Map<String, serde_json::Value>,
}

async fn get_elasticsearch_connector_map(image: &ElasticsearchImage) -> Result<ConnectorsMap> {
    let url = format!("http://127.0.0.1:{}", image.port);
    let client = Client::new();

    let mut create_index_futures = Vec::with_capacity(INDEXES.len());

    for stackoverflow_index_name in ["stack", "stack_mirror"] {
        let mut properties = serde_json::Map::new();
        properties.insert(
            "creationDate".to_string(),
            serde_json::json!({
                "type": "date"
            }),
        );
        properties.insert(
            "user".to_string(),
            serde_json::json!({
                "type": "keyword"
            }),
        );

        let mapping = IndexMapping {
            mappings: IndexProperties { properties },
        };

        let fut = create_index(&client, &url, stackoverflow_index_name, mapping);
        create_index_futures.push(fut);
    }

    let mut properties = serde_json::Map::new();
    properties.insert(
        "timestamp".to_string(),
        serde_json::json!({
            "type": "date"
            // No explicit format - ES will auto-detect including epoch formats
        }),
    );

    let mapping = IndexMapping {
        mappings: IndexProperties { properties },
    };

    create_index_futures.push(create_index(&client, &url, "hdfs", mapping));

    try_join_all(create_index_futures).await?;

    try_join_all(
        INDEXES
            .iter()
            .map(|(index_name, data)| write_to_index(&client, &url, index_name, data))
            .collect::<Vec<_>>(),
    )
    .await?;

    let config =
        ElasticsearchConfig::new_with_interval(url.clone(), ELASTICSEARCH_REFRESH_INTERVAL);
    let connector = Arc::new(ElasticsearchConnector::new(config)) as Arc<dyn Connector>;
    let connector_state = Arc::new(ConnectorState::new(connector.clone()));

    let url_for_retry = url.clone();
    let client_for_retry = client.clone();
    Retry::spawn(
        FixedInterval::new(ELASTICSEARCH_REFRESH_INTERVAL).take(10),
        move || {
            let url = url_for_retry.clone();
            let client = client_for_retry.clone();
            let connector = connector.clone();
            async move {
                for (index_name, expected_count) in INDEXES.iter().map(|(name, data)| (name, data.lines().count())) {
                    // Check collection exists
                    connector
                        .does_collection_exist(index_name)
                        .then_some(())
                        .ok_or_eyre(format!(
                            "timeout waiting for '{index_name}' collection to exist"
                        ))?;

                    // Verify data is searchable by checking document count
                    let count_url = format!("{url}/{index_name}/_count");
                    let response = client
                        .get(&count_url)
                        .send()
                        .await
                        .with_context(|| format!("GET count from index: {index_name}"))?;

                    if response.status().is_success() {
                        let count_result: serde_json::Value = response.json().await?;
                        let actual_count = count_result["count"].as_u64().unwrap_or(0);
                        if actual_count < expected_count as u64 {
                            bail!("Index '{index_name}' has {actual_count} docs, expecting {expected_count}");
                        }
                    }
                }
                Ok::<(), color_eyre::eyre::Error>(())
            }
        },
    )
    .await?;

    Ok(btreemap! { "test".to_string() => connector_state })
}

#[derive(Clone, Copy)]
struct TestCase {
    query: &'static str,
    expected: &'static str,
    count: usize,
    name: &'static str,
}

async fn void_response_to_err(url: &str, response: Response) -> Result<()> {
    let status = response.status();
    if !status.is_success() {
        if let Ok(text) = response.text().await {
            bail!("PUT/POST {} failed with status {}: {}", url, status, text);
        } else {
            bail!("PUT/POST {} failed with status {}", url, status);
        }
    }
    Ok(())
}

async fn create_index(
    client: &Client,
    base_url: &str,
    index_name: &str,
    mapping: IndexMapping,
) -> Result<()> {
    let url = format!("{base_url}/{index_name}");
    let response = client
        .put(&url)
        .json(&mapping)
        .send()
        .await
        .with_context(|| format!("PUT create index: {index_name}"))?;

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
    let url = format!("{base_url}/{index_name}/_bulk");

    let lines: Vec<&str> = data.lines().collect();
    let mut bulk_data = String::new();
    for line in lines {
        bulk_data.push_str(r#"{"index":{}}"#);
        bulk_data.push('\n');
        bulk_data.push_str(line);
        bulk_data.push('\n');
    }

    let response = client
        .post(&url)
        .query(&[("refresh", "true")])
        .header(CONTENT_TYPE, "application/x-ndjson")
        .body(bulk_data)
        .send()
        .await
        .with_context(|| format!("POST bulk ingest into index: {index_name}"))?;

    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        bail!("Bulk ingest into '{index_name}' failed with status {status}: {text}");
    }

    // Parse bulk response to check for errors
    let bulk_response: serde_json::Value = response.json().await?;
    if let Some(errors) = bulk_response.get("errors") {
        if errors.as_bool().unwrap_or(false) {
            let response_text = serde_json::to_string_pretty(&bulk_response)?;
            bail!("Bulk ingest into '{index_name}' had errors: {response_text}");
        }
    }

    info!("Index '{}' ingested data successfully", index_name);
    Ok(())
}

async fn predicate_pushdown_same_results(
    connectors: &ConnectorsMap,
    query: &str,
    query_after_optimizations: &str,
    count: usize,
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
        query: r#"test.stack | sort by creationDate | take 3"#,
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
];

#[tokio::test]
async fn test_elasticsearch_predicate_pushdown() -> Result<()> {
    let image = run_elasticsearch_image().await;
    let connectors = get_elasticsearch_connector_map(&image).await?;
    let connectors = Arc::new(connectors);

    // Filter test cases by name if TEST_FILTER env var is set
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
