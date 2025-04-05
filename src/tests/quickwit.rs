use std::{sync::Arc, time::Duration};

use collection_macros::btreemap;
use color_eyre::{
    eyre::{bail, Context},
    Result,
};
use futures_util::StreamExt;
use reqwest::{header::CONTENT_TYPE, Client, Response};
use serde::Serialize;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::sync::watch;
use tokio_retry::{strategy::FixedInterval, Retry};
use tracing::info;

use crate::{
    connector::Connector,
    http_server::{to_workflow_steps, ConnectorsMap},
    optimizations::Optimizer,
    quickwit_connector::{QuickwitConfig, QuickwitConnector},
    workflow::Workflow,
};

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
    create_index(&client, &url, "stack").await?;

    Retry::spawn(
        FixedInterval::new(Duration::from_secs(1)).take(3),
        || async { write_stackoverflow_posts(&client, &url, "stack").await },
    )
    .await?;

    let config = QuickwitConfig::new_with_interval(url, QUICKWIT_REFRESH_INTERVAL);
    let connector = Arc::new(QuickwitConnector::new(config)) as Arc<dyn Connector>;
    Ok(btreemap! { "test".to_string() => connector })
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

#[tokio::test]
async fn quickwit_sanity() -> Result<()> {
    let image = run_quickwit_image().await;

    let connectors = get_quickwit_connector_map(&image)
        .await
        .context("get quickwit connector")?;

    let strategy = FixedInterval::new(QUICKWIT_REFRESH_INTERVAL).take(3);
    let steps = Retry::spawn(strategy, || async {
        to_workflow_steps(
            &connectors,
            serde_json::from_str(r#"[{"scan": ["test", "stack"]}]"#).expect("query from json"),
        )
        .await
    })
    .await
    .expect("to workflow steps");

    let optimizer = Optimizer::default();
    let workflow = Workflow::new(optimizer.optimize(steps).await);

    let (_cancel_tx, cancel_rx) = watch::channel(());
    let mut stream = workflow.execute(cancel_rx).context("execute workflow")?;

    let mut i = 0;
    while let Some(_log) = stream.next().await {
        i += 1;
    }

    assert_eq!(i, 10);

    Ok(())
}
