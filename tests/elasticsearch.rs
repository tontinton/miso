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
    elasticsearch::{ElasticsearchConfig, ElasticsearchConnector},
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

const ELASTICSEARCH_TESTS: &[TestCase] = &[TestCase {
    query: r#"test.stack | union (test.hdfs) | where exists(questionId) or exists(tenant_id)"#,
    expected: r#"test.stack | union (test.hdfs)"#,
    count: 20,
    name: "elasticsearch_union_different_timestamps",
}];

#[ctor]
fn init() {
    color_eyre::install().unwrap();
    tracing_subscriber::fmt::init();
}

const ELASTICSEARCH_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

struct ElasticsearchImage {
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

async fn run_image() -> ElasticsearchImage {
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

async fn setup(url: String) -> Result<ConnectorsMap> {
    let client = Client::new();

    let mut create_index_futures = Vec::with_capacity(INDEXES.len());

    for stackoverflow_index_name in ["stack", "stack_mirror"] {
        let mut properties = serde_json::Map::new();
        properties.insert(
            "creationDate".to_string(),
            serde_json::json!({"type": "date"}),
        );
        properties.insert("user".to_string(), serde_json::json!({"type": "keyword"}));

        let mapping = IndexMapping {
            mappings: IndexProperties { properties },
        };

        let fut = create_index(&client, &url, stackoverflow_index_name, mapping);
        create_index_futures.push(fut);
    }

    let mut properties = serde_json::Map::new();
    properties.insert("timestamp".to_string(), serde_json::json!({"type": "date"}));

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
                    connector
                        .does_collection_exist(index_name)
                        .then_some(())
                        .ok_or_eyre(format!(
                            "timeout waiting for '{index_name}' collection to exist"
                        ))?;

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

#[tokio::test]
async fn elasticsearch_predicate_pushdown() -> Result<()> {
    let (url, _image_keepalive) = match std::env::var("EXT_ES") {
        Ok(url) => (url, None),
        Err(_) => {
            let image = run_image().await;
            (format!("http://127.0.0.1:{}", image.port), Some(image))
        }
    };
    let connectors = Arc::new(setup(url).await?);
    run_predicate_pushdown_tests(
        connectors,
        &[BASE_PREDICATE_PUSHDOWN_TESTS, ELASTICSEARCH_TESTS],
    )
    .await
}
