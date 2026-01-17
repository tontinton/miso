use std::time::Duration;

use async_stream::try_stream;
use color_eyre::eyre::{Context, eyre};
use miso_common::metrics::{CONNECTOR_SPLUNK, METRICS, OP_FETCH_RESULTS, OP_POLL_JOB};
use miso_workflow_types::log::{LogItem, LogItemTryStream, LogTryStream};
use reqwest::Client;
use tracing::debug;

use super::{
    JobStatusResponse, ResultsResponse, SplunkAuth, SplunkConnector, create_search_job,
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

    pub fn poll_with_previews(self, spl: String) -> LogItemTryStream {
        Box::pin(try_stream! {
            let sid = create_search_job(&self.client, &self.base_url, &spl, &self.auth).await?;
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
                    let stream = fetch_results_paged(
                        self.client.clone(),
                        format!("{}/services/search/jobs/{}/results", self.base_url, sid),
                        self.auth.clone(),
                        self.batch_size,
                    );
                    for await log in stream {
                        yield LogItem::Log(log?);
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

                    let stream = fetch_results_paged(
                        self.client.clone(),
                        format!("{}/services/search/jobs/{}/results_preview", self.base_url, sid),
                        self.auth.clone(),
                        self.batch_size,
                    );
                    for await log in stream {
                        yield LogItem::PartialStreamLog(log?, partial_stream_id);
                    }
                    yield LogItem::PartialStreamDone(partial_stream_id);
                    partial_stream_id += 1;
                }

                tokio::time::sleep(self.preview_interval).await;
            }
        })
    }
}

fn fetch_results_paged(
    client: Client,
    url: String,
    auth: SplunkAuth,
    batch_size: u32,
) -> LogTryStream {
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
                yield transformed;
                offset += 1;
            }
        }
    })
}
