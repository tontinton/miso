use std::time::Duration;

use async_stream::try_stream;
use color_eyre::eyre::{Context, Result, eyre};
use miso_common::metrics::{
    CONNECTOR_SPLUNK, METRICS, OP_CREATE_JOB, OP_FETCH_RESULTS, OP_POLL_JOB,
};
use miso_workflow_types::log::{LogItem, LogItemTryStream};
use reqwest::Client;
use tracing::{debug, info, instrument};

use crate::instrumentation::record_operation_result;

use super::{
    CreateJobResponse, JobStatusResponse, ResultsResponse, SplunkAuth, SplunkConnector,
    send_request,
};

pub struct PreviewPoller {
    client: Client,
    base_url: String,
    auth: SplunkAuth,
    preview_interval: Duration,
    timeout: Duration,
    batch_size: u32,
}

impl PreviewPoller {
    pub fn new(
        client: Client,
        base_url: String,
        auth: SplunkAuth,
        preview_interval: Duration,
        timeout: Duration,
        batch_size: u32,
    ) -> Self {
        Self {
            client,
            base_url,
            auth,
            preview_interval,
            timeout,
            batch_size,
        }
    }

    #[instrument(skip(self), name = "splunk create_preview_job")]
    async fn create_preview_job(&self, spl: &str) -> Result<String> {
        let start = std::time::Instant::now();
        METRICS
            .connector_requests_total
            .with_label_values(&[CONNECTOR_SPLUNK, OP_CREATE_JOB])
            .inc();

        let url = format!("{}/services/search/jobs", self.base_url);

        let form = [
            ("search", spl),
            ("output_mode", "json"),
            ("exec_mode", "normal"),
        ];

        let req = self
            .auth
            .apply_to_request(self.client.post(&url))
            .form(&form);

        info!("running SPL with preview: `{spl}`");

        let result: Result<String> = async {
            let mut bytes = send_request(req).await?;
            let response: CreateJobResponse = simd_json::serde::from_slice(bytes.as_mut())
                .context("parse job creation response")?;
            Ok(response.sid)
        }
        .await;

        let duration = start.elapsed().as_secs_f64();
        record_operation_result(CONNECTOR_SPLUNK, OP_CREATE_JOB, &result, duration);
        result
    }

    pub fn poll_with_previews(self, spl: String) -> LogItemTryStream {
        Box::pin(try_stream! {
            let sid = self.create_preview_job(&spl).await?;
            let start = std::time::Instant::now();
            let mut partial_stream_id: usize = 0;
            let mut last_preview_count: u64 = 0;

            loop {
                if start.elapsed() > self.timeout {
                    Err(eyre!("Search job {} timed out after {:?}", sid, self.timeout))?;
                }

                METRICS
                    .connector_requests_total
                    .with_label_values(&[CONNECTOR_SPLUNK, OP_POLL_JOB])
                    .inc();

                let status_url = format!(
                    "{}/services/search/jobs/{}?output_mode=json",
                    self.base_url, sid
                );
                let req = self.auth.apply_to_request(self.client.get(&status_url));
                let mut bytes = send_request(req).await?;
                let response: JobStatusResponse =
                    simd_json::serde::from_slice(bytes.as_mut()).context("parse job status response")?;

                let Some(entry) = response.entry.first() else {
                    tokio::time::sleep(self.preview_interval).await;
                    continue;
                };

                if entry.content.dispatch_state == "FAILED" {
                    Err(eyre!("Search job {} failed", sid))?;
                }
                if entry.content.dispatch_state == "PAUSED" {
                    Err(eyre!("Search job {} paused unexpectedly", sid))?;
                }

                if entry.content.is_done {
                    debug!("Job {} done, fetching final results", sid);
                    for await item in self.fetch_final_results(&sid) {
                        yield item?;
                    }
                    return;
                }

                let current_count = entry.content.result_count;
                if current_count > last_preview_count {
                    debug!(
                        "Job {} has {} preview results (was {})",
                        sid, current_count, last_preview_count
                    );
                    last_preview_count = current_count;

                    for await item in self.fetch_preview_results(&sid, partial_stream_id) {
                        yield item?;
                    }
                    yield LogItem::PartialStreamDone(partial_stream_id);
                    partial_stream_id += 1;
                }

                tokio::time::sleep(self.preview_interval).await;
            }
        })
    }

    fn fetch_preview_results(&self, sid: &str, partial_stream_id: usize) -> LogItemTryStream {
        let url = format!(
            "{}/services/search/jobs/{}/results_preview",
            self.base_url, sid
        );
        let client = self.client.clone();
        let auth = self.auth.clone();
        let batch_size = self.batch_size;

        Box::pin(try_stream! {
            let mut offset = 0u64;

            loop {
                METRICS
                    .connector_requests_total
                    .with_label_values(&[CONNECTOR_SPLUNK, OP_FETCH_RESULTS])
                    .inc();

                let req = auth.apply_to_request(
                    client.get(&url)
                        .query(&[
                            ("output_mode", "json"),
                            ("offset", &offset.to_string()),
                            ("count", &batch_size.to_string()),
                        ])
                );

                let mut bytes = send_request(req).await?;
                let response: ResultsResponse =
                    simd_json::serde::from_slice(bytes.as_mut())
                        .context("parse preview results response")?;

                if response.results.is_empty() {
                    return;
                }

                for log in response.results {
                    let transformed = SplunkConnector::transform_log(log)?;
                    yield LogItem::PartialStreamLog(transformed, partial_stream_id);
                    offset += 1;
                }
            }
        })
    }

    fn fetch_final_results(&self, sid: &str) -> LogItemTryStream {
        let url = format!("{}/services/search/jobs/{}/results", self.base_url, sid);
        let client = self.client.clone();
        let auth = self.auth.clone();
        let batch_size = self.batch_size;

        Box::pin(try_stream! {
            let mut offset = 0u64;

            loop {
                METRICS
                    .connector_requests_total
                    .with_label_values(&[CONNECTOR_SPLUNK, OP_FETCH_RESULTS])
                    .inc();

                let req = auth.apply_to_request(
                    client.get(&url)
                        .query(&[
                            ("output_mode", "json"),
                            ("offset", &offset.to_string()),
                            ("count", &batch_size.to_string()),
                        ])
                );

                let mut bytes = send_request(req).await?;
                let response: ResultsResponse =
                    simd_json::serde::from_slice(bytes.as_mut())
                        .context("parse results response")?;

                if response.results.is_empty() {
                    return;
                }

                for log in response.results {
                    let transformed = SplunkConnector::transform_log(log)?;
                    yield LogItem::Log(transformed);
                    offset += 1;
                }
            }
        })
    }
}
