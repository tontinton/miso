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
    splunk::{SplunkAuth, SplunkConfig, SplunkConnector},
    Connector, ConnectorState,
};
use miso_server::http_server::ConnectorsMap;
use reqwest::Client;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio_retry::{strategy::FixedInterval, Retry};
use tracing::info;

use common::{
    init_test_tracing, run_predicate_pushdown_tests, TestConnector, BASE_PREDICATE_PUSHDOWN_TESTS,
    INDEXES,
};

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
    };

    let connector = Arc::new(SplunkConnector::new(config)) as Arc<dyn Connector>;
    let connector_state = Arc::new(ConnectorState::new(connector.clone()));

    // Wait for indexes to be available
    Retry::spawn(
        FixedInterval::new(SPLUNK_REFRESH_INTERVAL).take(30),
        move || {
            let connector = connector.clone();
            async move {
                for (index_name, _) in INDEXES.iter() {
                    connector
                        .does_collection_exist(index_name)
                        .then_some(())
                        .ok_or_eyre(format!(
                            "timeout waiting for '{index_name}' collection to exist"
                        ))?;
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
    run_predicate_pushdown_tests(
        TestConnector::Splunk,
        connectors,
        &[BASE_PREDICATE_PUSHDOWN_TESTS],
    )
    .await
}
