use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use collection_macros::btreemap;
use color_eyre::{
    eyre::{bail, Context, OptionExt},
    Result,
};
use futures_util::{future::try_join_all, TryStreamExt};
use reqwest::{header::CONTENT_TYPE, Client, Response};
use serde::Serialize;
use test_case::test_case;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::sync::{watch, OnceCell};
use tokio_retry::{strategy::FixedInterval, Retry};
use tokio_test::block_on;
use tracing::info;

use crate::{
    connector::Connector,
    http_server::{to_workflow_steps, ConnectorsMap},
    optimizations::Optimizer,
    quickwit_connector::{QuickwitConfig, QuickwitConnector},
    workflow::{sortable_value::SortableValue, Scan, Workflow, WorkflowStep},
};

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

    Ok(btreemap! { "test".to_string() => connector })
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

#[derive(Serialize, Default)]
struct DocMapping {
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
    let url = format!("{}/api/v1/indexes", base_url);
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
    let url = format!("{}/api/v1/{}/ingest", base_url, index_name);
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
    count: usize,
    fully_pushdown: bool,
) -> Result<()> {
    let resources = get_test_resources().await;
    let connectors = resources.1.clone();

    let steps = to_workflow_steps(
        &connectors,
        serde_json::from_str(query).context("query from json")?,
    )
    .await
    .expect("to workflow steps");

    let default_optimizer = Optimizer::default();
    let no_pushdown_optimizer = Optimizer::no_predicate_pushdowns();

    let predicate_pushdown_steps = default_optimizer.optimize(steps.clone()).await;

    if fully_pushdown {
        assert_eq!(
            predicate_pushdown_steps,
            vec![WorkflowStep::Scan(
                Scan::from_connector(connectors["test"].clone(), "stack".to_string()).await
            )],
            "query predicates should have been fully pushdown"
        );
    } else {
        assert!(
            predicate_pushdown_steps.len() > 1,
            "query predicates should have not have been fully pushdown: {:?}",
            predicate_pushdown_steps
        );
    }

    let pushdown_workflow = Workflow::new(predicate_pushdown_steps);
    let no_pushdown_workflow = Workflow::new(no_pushdown_optimizer.optimize(steps).await);

    let (_cancel_tx1, cancel_rx1) = watch::channel(());
    let (_cancel_tx2, cancel_rx2) = watch::channel(());

    let mut pushdown_stream = pushdown_workflow
        .execute(cancel_rx1)
        .context("execute predicate pushdown workflow")?;
    let mut no_pushdown_stream = no_pushdown_workflow
        .execute(cancel_rx2)
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
    3,
    true;
    "top_n"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"filter": {"eq": [{"id": "acceptedAnswerId"}, {"lit": 12446}]}}
    ]"#,
    1,
    true;
    "filter_eq"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {
            "filter": {
                "and": [{
                    "eq": [{"id": "questionId"}, {"lit": 11}]
                }, {
                    "exists": "answerId"
                }]
            }
        }
    ]"#,
    1,
    true;
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
            "by": ["user"]
          }
        }
    ]"#,
    5,
    true;
    "summarize_min_max_count"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        "count"
    ]"#,
    1,
    true;
    "count"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"union": [{"scan": ["test", "stack_mirror"]}]}
    ]"#,
    20,
    true;
    "union"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"union": [{"scan": ["test", "hdfs"]}]}
    ]"#,
    20,
    false;
    "union_not_same_timestamp_field"
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"union": [{"scan": ["test", "stack_mirror"]}]},
        {"filter": {"lt": [{"id": "acceptedAnswerId"}, {"lit": 100}]}},
        {"limit": 1}
    ]"#,
    1,
    true;
    "union_filter_limit"
)]
fn quickwit_predicate_pushdown(query: &str, count: usize, fully_pushdown: bool) -> Result<()> {
    block_on(predicate_pushdown_same_results(
        query,
        count,
        fully_pushdown,
    ))
}
