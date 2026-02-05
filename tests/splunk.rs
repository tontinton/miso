mod common;

use std::{sync::Arc, time::Duration};

use color_eyre::{
    eyre::{bail, OptionExt, WrapErr},
    Result,
};
use ctor::ctor;
use futures_util::future::try_join_all;
use miso_common::btreemap;
use miso_connectors::{
    splunk::{SplunkAuth, SplunkConfig, SplunkConnector},
    Connector, ConnectorState,
};
use miso_server::config::ConnectorsMap;
use reqwest::Client;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio_retry::{strategy::FixedInterval, Retry};
use tracing::info;

use common::init_test_tracing;
use common::predicate_pushdown::{run_tests, TestConnector, INDEXES, TESTS};

#[ctor]
fn init() {
    init_test_tracing();
}

const SPLUNK_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const SPLUNK_PASSWORD: &str = "testpassword123";

struct SplunkImage {
    _container: ContainerAsync<GenericImage>,
    port: u16,
}

async fn run_image() -> SplunkImage {
    let container = GenericImage::new("splunk/splunk", "9.2.3")
        .with_wait_for(WaitFor::message_on_stdout("Ansible playbook complete"))
        .with_exposed_port(8089.tcp())
        .with_env_var("SPLUNK_START_ARGS", "--accept-license")
        .with_env_var("SPLUNK_PASSWORD", SPLUNK_PASSWORD)
        .with_startup_timeout(Duration::from_secs(150))
        .start()
        .await
        .expect("splunk container to start");

    let port = container
        .get_host_port_ipv4(8089)
        .await
        .expect("get splunk REST API port");

    SplunkImage {
        _container: container,
        port,
    }
}

async fn setup(url: String) -> Result<ConnectorsMap> {
    let client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()?;

    let create_futures: Vec<_> = INDEXES
        .iter()
        .map(|(index_name, _)| create_index(&client, &url, index_name))
        .collect();
    let need_writing = try_join_all(create_futures).await?;

    let write_futures: Vec<_> = INDEXES
        .iter()
        .zip(need_writing)
        .filter(|&(_, need_write)| need_write)
        .map(|((index_name, data), _)| write_to_index(&client, &url, index_name, data))
        .collect();
    try_join_all(write_futures).await?;

    let config = SplunkConfig {
        url: url.clone(),
        auth: SplunkAuth::Basic {
            username: "admin".to_string(),
            password: SPLUNK_PASSWORD.to_string(),
        },
        refresh_interval: SPLUNK_REFRESH_INTERVAL,
        job_poll_interval: Duration::from_millis(500),
        job_timeout: Duration::from_secs(60),
        result_batch_size: 50000,
        accept_invalid_certs: true,
        enable_partial_stream: false,
        preview_interval: Duration::from_secs(2),
    };

    let connector = Arc::new(SplunkConnector::new(config)) as Arc<dyn Connector>;
    let connector_state = Arc::new(ConnectorState::new(connector.clone()));

    let url_for_retry = url.clone();
    let client_for_retry = client.clone();
    Retry::spawn(
        FixedInterval::new(SPLUNK_REFRESH_INTERVAL).take(30),
        move || {
            let connector = connector.clone();
            let url = url_for_retry.clone();
            let client = client_for_retry.clone();
            async move {
                for (index_name, data) in INDEXES.iter() {
                    let expected_count = data.lines().count();

                    connector
                        .does_collection_exist(index_name)
                        .then_some(())
                        .ok_or_eyre(format!(
                            "timeout waiting for '{index_name}' collection to exist"
                        ))?;

                    let actual_count =
                        get_index_count(&client, &url, index_name)
                            .await
                            .with_context(|| {
                                format!("failed to get count for index '{index_name}'")
                            })?;

                    if actual_count < expected_count {
                        bail!(
                            "Index '{index_name}' has {actual_count} docs, expecting {expected_count}"
                        );
                    }
                }
                Ok::<(), color_eyre::eyre::Error>(())
            }
        },
    )
    .await?;

    Ok(btreemap! { "test".to_string() => connector_state })
}

async fn create_index(client: &Client, base_url: &str, index_name: &str) -> Result<bool> {
    let url = format!("{base_url}/services/data/indexes");
    let form = [("name", index_name), ("datatype", "event")];
    let response = client
        .post(&url)
        .basic_auth("admin", Some(SPLUNK_PASSWORD))
        .form(&form)
        .send()
        .await
        .with_context(|| format!("POST create index: {index_name}"))?;

    let status = response.status();
    if !status.is_success() {
        if status == 409 {
            info!("Index '{}' already exists", index_name);
            Ok(false)
        } else {
            let text = response.text().await.unwrap_or_default();
            bail!("Create index '{index_name}' failed with status {status}: {text}");
        }
    } else {
        info!("Index '{}' created", index_name);
        Ok(true)
    }
}

async fn write_to_index(
    client: &Client,
    base_url: &str,
    index_name: &str,
    data: &'static str,
) -> Result<()> {
    let url = format!("{base_url}/services/receivers/simple?index={index_name}&sourcetype=json");

    for line in data.lines() {
        let response = client
            .post(&url)
            .basic_auth("admin", Some(SPLUNK_PASSWORD))
            .header("Content-Type", "application/json")
            .body(line.to_string())
            .send()
            .await
            .with_context(|| format!("POST event to index: {index_name}"))?;

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            bail!("Ingest into '{index_name}' failed with status {status}: {text}");
        }
    }

    info!("Index '{}' ingested data successfully", index_name);
    Ok(())
}

async fn get_index_count(client: &Client, base_url: &str, index_name: &str) -> Result<usize> {
    let url = format!("{base_url}/services/search/jobs");
    let search_query = format!("search (index=\"{index_name}\") | stats count");
    let form = [
        ("search", search_query.as_str()),
        ("output_mode", "json"),
        ("exec_mode", "blocking"),
    ];

    let response = client
        .post(&url)
        .basic_auth("admin", Some(SPLUNK_PASSWORD))
        .form(&form)
        .send()
        .await
        .with_context(|| format!("POST search job for count in index: {index_name}"))?;

    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        bail!("Search job for '{index_name}' failed with status {status}: {text}");
    }

    let search_response: serde_json::Value = response.json().await?;
    let sid = search_response["sid"]
        .as_str()
        .ok_or_eyre("missing sid in search response")?;

    let results_url = format!("{base_url}/services/search/jobs/{sid}/results?output_mode=json");
    let results_response = client
        .get(&results_url)
        .basic_auth("admin", Some(SPLUNK_PASSWORD))
        .send()
        .await
        .with_context(|| format!("GET search results for index: {index_name}"))?;

    let results_status = results_response.status();
    if !results_status.is_success() {
        let text = results_response.text().await.unwrap_or_default();
        bail!("Get search results for '{index_name}' failed with status {results_status}: {text}");
    }

    let results: serde_json::Value = results_response.json().await?;
    let count = results["results"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|obj| obj["count"].as_str())
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);

    info!("Index '{}' has {} documents", index_name, count);
    Ok(count)
}

#[tokio::test]
async fn splunk_predicate_pushdown() -> Result<()> {
    let (url, _image_keepalive) = match std::env::var("EXT_SPLUNK") {
        Ok(url) => (url, None),
        Err(_) => {
            let image = run_image().await;
            (format!("https://127.0.0.1:{}", image.port), Some(image))
        }
    };
    let connectors = Arc::new(setup(url).await?);
    run_tests(TestConnector::Splunk, connectors, &[TESTS]).await
}
