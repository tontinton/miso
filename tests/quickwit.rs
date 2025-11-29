mod common;

use std::{sync::Arc, time::Duration};

use collection_macros::btreemap;
use color_eyre::{
    eyre::{bail, OptionExt, WrapErr},
    Result,
};
use ctor::ctor;
use futures_util::future::try_join_all;
use miso_connectors::{
    quickwit::{QuickwitConfig, QuickwitConnector},
    Connector, ConnectorState,
};
use miso_server::http_server::ConnectorsMap;
use reqwest::{header::CONTENT_TYPE, Client, Response};
use serde::Serialize;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio_retry::{strategy::FixedInterval, Retry};
use tracing::info;

use common::{run_predicate_pushdown_tests, TestCase, BASE_PREDICATE_PUSHDOWN_TESTS, INDEXES};

const QUICKWIT_TESTS: &[TestCase] = &[
    TestCase {
        query: r#"test.stack | summarize c=count() by user | top 3 by c"#,
        expected: r#"test.stack | top 3 by c"#,
        count: 3,
        name: "quickwit_topn_after_groupby",
    },
    TestCase {
        query: r#"test.stack | top 5 by questionId | top 3 by questionId"#,
        expected: r#"test.stack | top 3 by questionId"#,
        count: 3,
        name: "quickwit_topn_after_topn",
    },
];

const QUICKWIT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

#[ctor]
fn init() {
    color_eyre::install().unwrap();
    tracing_subscriber::fmt::init();
}

struct QuickwitImage {
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

async fn run_image() -> QuickwitImage {
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

async fn setup(url: String) -> Result<ConnectorsMap> {
    let client = Client::new();

    let delete_futures: Vec<_> = INDEXES
        .iter()
        .map(|(name, _)| delete_index(&client, &url, name))
        .collect();
    try_join_all(delete_futures).await?;

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

async fn delete_index(client: &Client, base_url: &str, index_name: &str) -> Result<()> {
    let url = format!("{base_url}/api/v1/indexes/{index_name}");
    let response = client
        .delete(&url)
        .send()
        .await
        .with_context(|| format!("DELETE index: {index_name}"))?;

    let status = response.status();

    // Ignore 404 errors (index doesn't exist).
    if !status.is_success() && status != 404 {
        if let Ok(text) = response.text().await {
            bail!("DELETE {} failed with status {}: {}", url, status, text);
        } else {
            bail!("DELETE {} failed with status {}", url, status);
        }
    }

    info!("Index '{}' deleted or did not exist", index_name);
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

#[tokio::test]
async fn quickwit_predicate_pushdown() -> Result<()> {
    let (url, _image_keepalive) = match std::env::var("EXT_QW") {
        Ok(url) => (url, None),
        Err(_) => {
            let image = run_image().await;
            (format!("http://127.0.0.1:{}", image.port), Some(image))
        }
    };
    let connectors = Arc::new(setup(url).await?);
    run_predicate_pushdown_tests(connectors, &[BASE_PREDICATE_PUSHDOWN_TESTS, QUICKWIT_TESTS]).await
}
