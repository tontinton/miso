use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use collection_macros::btreemap;
use color_eyre::{
    eyre::{bail, Context, OptionExt},
    Result,
};
use futures_util::StreamExt;
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
    workflow::Workflow,
};

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
    create_index(&client, &url, "stack").await?;

    Retry::spawn(
        FixedInterval::new(Duration::from_secs(1)).take(3),
        || async { write_stackoverflow_posts(&client, &url, "stack").await },
    )
    .await?;

    let config = QuickwitConfig::new_with_interval(url, QUICKWIT_REFRESH_INTERVAL);
    let connector = Arc::new(QuickwitConnector::new(config)) as Arc<dyn Connector>;

    Retry::spawn(
        FixedInterval::new(QUICKWIT_REFRESH_INTERVAL).take(3),
        || async {
            connector
                .does_collection_exist("stack")
                .await
                .then_some(())
                .ok_or_eyre("timeout waiting for 'stack' collection to exist")
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

async fn create_index(client: &Client, base_url: &str, index_name: &str) -> Result<()> {
    let url = format!("{}/api/v1/indexes", base_url);
    let response = client
        .post(&url)
        .json(&CreateIndex {
            index_id: index_name.to_string(),
            doc_mapping: DocMapping {
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
            ..Default::default()
        })
        .send()
        .await
        .with_context(|| format!("POST create index: {index_name}"))?;

    void_response_to_err(&url, response).await?;

    info!("Index '{}' created successfully", index_name);
    Ok(())
}

async fn write_stackoverflow_posts(
    client: &Client,
    base_url: &str,
    index_name: &str,
) -> Result<()> {
    let url = format!("{}/api/v1/{}/ingest", base_url, index_name);
    let response = client
        .post(&url)
        .query(&[("commit", "force")])
        .header(CONTENT_TYPE, "application/json")
        .body(include_str!("./resources/stackoverflow.posts.10.json"))
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

async fn predicate_pushdown_same_results(query: &str, count: usize) -> Result<()> {
    let resources = get_test_resources().await;

    let steps = to_workflow_steps(
        &resources.1,
        serde_json::from_str(query).context("query from json")?,
    )
    .await
    .expect("to workflow steps");

    let default_optimizer = Optimizer::default();
    let no_pushdown_optimizer = Optimizer::no_predicate_pushdowns();

    let pushdown_workflow = Workflow::new(default_optimizer.optimize(steps.clone()).await);
    let no_pushdown_workflow = Workflow::new(no_pushdown_optimizer.optimize(steps).await);

    let (_cancel_tx1, cancel_rx1) = watch::channel(());
    let (_cancel_tx2, cancel_rx2) = watch::channel(());

    let mut pushdown_stream = pushdown_workflow
        .execute(cancel_rx1)
        .context("execute predicate pushdown workflow")?;
    let mut no_pushdown_stream = no_pushdown_workflow
        .execute(cancel_rx2)
        .context("execute no predicate pushdown workflow")?;

    let mut i = 0;
    while let Some(log1) = pushdown_stream.next().await {
        let log2 = no_pushdown_stream.next().await
            .ok_or_eyre("expected the no predicate pushdown query to have a log when the connector query streamed a log")?;
        assert_eq!(
            log1.context("predicate pushdown workflow failure")?,
            log2.context("no predicate pushdown workflow failure")?
        );
        i += 1;
    }

    assert_eq!(count, i, "number of logs returned is wrong");

    Ok(())
}

#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"sort": [{"by": "creationDate"}]},
        {"limit": 3}
    ]"#,
    3
)]
#[test_case(
    r#"[
        {"scan": ["test", "stack"]},
        {"filter": {"eq": [{"id": "acceptedAnswerId"}, {"lit": 12446}]}}
    ]"#,
    1
)]
fn quickwit_predicate_pushdown(query: &str, count: usize) -> Result<()> {
    block_on(predicate_pushdown_same_results(query, count))
}
