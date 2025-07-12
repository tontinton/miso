use std::{
    collections::BTreeMap,
    sync::{Arc, Weak},
    time::Duration,
};

use collection_macros::btreemap;
use color_eyre::{
    eyre::{bail, Context, OptionExt},
    Result,
};
use futures_util::{future::try_join_all, TryStreamExt};
use miso_connectors::{
    quickwit::{QuickwitConfig, QuickwitConnector},
    Connector, ConnectorState,
};
use miso_optimizations::Optimizer;
use miso_server::http_server::{to_workflow_steps, ConnectorsMap};
use miso_workflow::{sortable_value::SortableValue, Workflow};
use reqwest::{header::CONTENT_TYPE, Client, Response};
use serde::Serialize;
use test_case::test_case;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::{sync::OnceCell, task::spawn_blocking};
use tokio_retry::{strategy::FixedInterval, Retry};
use tokio_test::block_on;
use tokio_util::sync::CancellationToken;
use tracing::info;

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
static TEST_RESOURCES_WEAK: OnceCell<tokio::sync::Mutex<Weak<(QuickwitImage, ConnectorsMap)>>> =
    OnceCell::const_new();

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

async fn get_test_resources() -> Arc<(QuickwitImage, ConnectorsMap)> {
    let weak_cell = TEST_RESOURCES_WEAK
        .get_or_init(|| async { tokio::sync::Mutex::new(Weak::new()) })
        .await;

    let mut weak_lock = weak_cell.lock().await;
    if let Some(existing) = weak_lock.upgrade() {
        return existing;
    }

    let image = run_quickwit_image().await;
    let connectors = get_quickwit_connector_map(&image)
        .await
        .expect("get quickwit connector");

    let resources = Arc::new((image, connectors));
    *weak_lock = Arc::downgrade(&resources);

    resources
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

async fn predicate_pushdown_same_results(
    query: &str,
    query_after_optimizations: &str,
    count: usize,
) -> Result<()> {
    let resources = get_test_resources().await;
    let connectors = resources.1.clone();

    let steps = to_workflow_steps(
        &connectors,
        &BTreeMap::new(),
        serde_json::from_str(query).context("parse query steps from json")?,
    )
    .await
    .expect("to workflow steps");

    let expected_after_optimizations_steps = to_workflow_steps(
        &connectors,
        &BTreeMap::new(),
        serde_json::from_str(query_after_optimizations)
            .context("parse expected query steps from json")?,
    )
    .await
    .expect("to expected workflow steps");

    let default_optimizer = Optimizer::default();
    let no_pushdown_optimizer = Optimizer::empty();

    let steps_cloned = steps.clone();
    let predicate_pushdown_steps =
        spawn_blocking(move || default_optimizer.optimize(steps_cloned)).await?;

    assert_eq!(
        predicate_pushdown_steps, expected_after_optimizations_steps,
        "query predicates should have been equal to expected steps after optimization"
    );

    let pushdown_workflow = Workflow::new(predicate_pushdown_steps);

    let no_predicate_pushdown_steps =
        spawn_blocking(move || no_pushdown_optimizer.optimize(steps)).await?;
    let no_pushdown_workflow = Workflow::new(no_predicate_pushdown_steps);

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
                        pushdown_results.push(SortableValue(serde_json::Value::Object(log)));
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
                        no_pushdown_results.push(SortableValue(serde_json::Value::Object(log)));
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

#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"sort": [{"by": "creationDate"}]},
        {"limit": 3}
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    3;
    "top_n"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"filter": {"==": [{"id": "acceptedAnswerId"}, {"lit": 12446}]}}
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    1;
    "filter_eq"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"filter": {"has_cs": [{"id": "body"}, {"lit": "VB.NET"}]}}
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    1;
    "filter_has_cs"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"filter": {"in": [{"id": "acceptedAnswerId"}, [{"lit": 12446}, {"lit": 31}]]}}
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    2;
    "filter_in"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {
            "filter": {
                "and": [{
                    "==": [{"id": "questionId"}, {"lit": 11}]
                }, {
                    "exists": "answerId"
                }]
            }
        }
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    1;
    "filter_eq_and_exists"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {
          "summarize": {
            "aggs": {
              "minQuestionId": {"min": "questionId"},
              "maxQuestionId": {"max": "questionId"},
              "sumQuestionId": {"sum": "questionId"},
              "count": "count"
            },
            "by": [{"id": "user"}]
          }
        }
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    5;
    "summarize_min_max_count"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {
          "summarize": {
            "aggs": {
              "minQuestionId": {"min": "questionId"},
              "maxQuestionId": {"max": "questionId"},
              "sumQuestionId": {"sum": "questionId"},
              "count": "count"
            },
            "by": [{"bin": ["answerId", 5]}]
          }
        }
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    2;
    "summarize_min_max_count_by_bin"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {
          "summarize": {
            "aggs": {},
            "by": [{"id": "user"}]
          }
        }
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    5;
    "summarize_distinct"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"distinct": ["user"]}
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    5;
    "distinct"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {
          "summarize": {
            "aggs": {"minQuestionId": {"min": "questionId"}},
            "by": [{"id": "user"}]
          }
        },
        {"top": [[{"by": "minQuestionId"}], 3]}
    ]"#,
    r#"[
        {"scan": ["test", "stack"]},
        {"top": [[{"by": "minQuestionId"}], 3]}
    ]"#,
    3;
    "summarize_then_topn"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"top": [[{"by": "questionId"}], 5]},
        {
          "summarize": {
            "aggs": {"minQuestionId": {"min": "questionId"}},
            "by": [{"id": "user"}]
          }
        }
    ]"#,
    r#"[
        {"scan": ["test", "stack"]},
        {
          "summarize": {
            "aggs": {"minQuestionId": {"min": "questionId"}},
            "by": [{"id": "user"}]
          }
        }
    ]"#,
    3;
    "topn_then_summarize"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        "count"
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    1;
    "count"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"union": [{"scan": ["test", "stack_mirror"]}]}
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    20;
    "union"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"union": [{"scan": ["test", "hdfs"]}]}
    ]"#,
    r#"[
        {"scan": ["test", "stack"]},
        {"union": [{"scan": ["test", "hdfs"]}]}
    ]"#,
    20;
    "union_not_same_timestamp_field"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"union": [{"scan": ["test", "stack_mirror"]}]},
        {"filter": {"<": [{"id": "acceptedAnswerId"}, {"lit": 100}]}},
        {"top": [[{"by": "acceptedAnswerId"}], 1]}
    ]"#,
    r#"[{"scan": ["test", "stack"]}]"#,
    1;
    "union_filter_topn"
)]
fn quickwit_predicate_pushdown(
    query: &str,
    query_after_optimizations: &str,
    count: usize,
) -> Result<()> {
    block_on(predicate_pushdown_same_results(
        query,
        query_after_optimizations,
        count,
    ))
}
