use std::time::{Duration, Instant};

use async_stream::try_stream;
use color_eyre::eyre::{Context, Result, bail, eyre};
use hashbrown::HashSet;
use miso_common::metrics::{
    CONNECTOR_SPLUNK, METRICS, OP_CREATE_JOB, OP_FETCH_RESULTS, OP_POLL_JOB,
};
use miso_workflow_types::{
    log::{Log, LogItem, LogItemTryStream, LogTryStream, PartialStreamKey, next_source_id},
    value::Value,
};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::sleep;
use tracing::{info, instrument};

use super::{JobStatusResponse, ResultsResponse, SplunkAuth, SplunkConnector, send_request};
use crate::instrumentation::record_operation_result;

#[derive(Debug, Deserialize)]
struct CreateJobResponse {
    sid: String,
}

type StatsFields = (HashSet<String>, HashSet<String>);

pub struct QueryRunner {
    client: Client,
    base_url: String,
    auth: SplunkAuth,
    poll_interval: Duration,
    timeout: Duration,
    batch_size: u32,
}

impl QueryRunner {
    pub fn new(
        client: Client,
        base_url: String,
        auth: SplunkAuth,
        poll_interval: Duration,
        timeout: Duration,
        batch_size: u32,
    ) -> Self {
        Self {
            client,
            base_url,
            auth,
            poll_interval,
            timeout,
            batch_size,
        }
    }

    #[instrument(skip(self), name = "splunk run")]
    pub async fn run(self, spl: &str) -> Result<LogTryStream> {
        let sid = self.create_job(spl, false).await?;
        let _ = self.poll_until_done(&sid).await?;
        let url = format!("{}/services/search/jobs/{}/results", self.base_url, sid);
        Ok(fetch_results_stream(
            self.client,
            url,
            self.auth,
            self.batch_size,
        ))
    }

    #[instrument(skip(self), name = "splunk run_count")]
    pub async fn run_count(&self, spl: &str) -> Result<(u64, Log)> {
        let sid = self.create_job(spl, false).await?;
        let result_count = self.poll_until_done(&sid).await?;

        let results_url = format!(
            "{}/services/search/jobs/{}/results?output_mode=json&count=1",
            self.base_url, sid
        );
        let req = self.auth.apply_to_request(self.client.get(&results_url));
        let mut bytes = send_request(req).await?;
        let response: ResultsResponse =
            simd_json::serde::from_slice(bytes.as_mut()).context("parse count response")?;

        let log = response.results.into_iter().next().unwrap_or_default();
        Ok((result_count, log))
    }

    #[instrument(skip(self), name = "splunk run_stats")]
    pub async fn run_stats(
        self,
        spl: &str,
        timestamp_fields: HashSet<String>,
        numeric_fields: HashSet<String>,
    ) -> Result<LogTryStream> {
        let sid = self.create_job(spl, false).await?;
        let _ = self.poll_until_done(&sid).await?;
        let url = format!("{}/services/search/jobs/{}/results", self.base_url, sid);
        Ok(fetch_results_with_stats_transform(
            self.client,
            url,
            self.auth,
            self.batch_size,
            timestamp_fields,
            numeric_fields,
        ))
    }

    #[instrument(skip(self), name = "splunk run_with_previews")]
    pub fn run_with_previews(self, spl: String, preview_interval: Duration) -> LogItemTryStream {
        self.poll_with_previews_until_done(spl, preview_interval, None)
    }

    #[instrument(skip(self), name = "splunk run_stats_with_previews")]
    pub fn run_stats_with_previews(
        self,
        spl: String,
        preview_interval: Duration,
        timestamp_fields: HashSet<String>,
        numeric_fields: HashSet<String>,
    ) -> LogItemTryStream {
        self.poll_with_previews_until_done(
            spl,
            preview_interval,
            Some((timestamp_fields, numeric_fields)),
        )
    }

    #[instrument(skip(self), name = "splunk create_job")]
    async fn create_job(&self, spl: &str, previews: bool) -> Result<String> {
        let start = Instant::now();
        METRICS
            .connector_requests_total
            .with_label_values(&[CONNECTOR_SPLUNK, OP_CREATE_JOB])
            .inc();

        let url = format!("{}/services/search/jobs", self.base_url);

        let mut form = vec![
            ("search", spl),
            ("output_mode", "json"),
            ("exec_mode", "normal"),
        ];

        if previews {
            // value doesn't matter as long as > 0
            form.push(("status_buckets", "300"));
        }

        let req = self
            .auth
            .apply_to_request(self.client.post(&url))
            .form(&form);

        info!(previews, query = spl, "running SPL: `{spl}`");

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

    async fn fetch_job_status(&self, sid: &str) -> Result<JobStatusResponse> {
        METRICS
            .connector_requests_total
            .with_label_values(&[CONNECTOR_SPLUNK, OP_POLL_JOB])
            .inc();

        let url = format!(
            "{}/services/search/jobs/{}?output_mode=json",
            self.base_url, sid
        );
        let req = self.auth.apply_to_request(self.client.get(&url));
        let mut bytes = send_request(req).await?;
        simd_json::serde::from_slice(bytes.as_mut()).context("parse job status response")
    }

    #[instrument(skip(self), name = "splunk poll_until_done")]
    async fn poll_until_done(&self, sid: &str) -> Result<u64> {
        let start = Instant::now();
        loop {
            if start.elapsed() > self.timeout {
                bail!("Search job {} timed out after {:?}", sid, self.timeout);
            }

            let response = self.fetch_job_status(sid).await?;

            if let Some(entry) = response.entry.first() {
                if entry.content.is_done {
                    return Ok(entry.content.result_count);
                }
                match entry.content.dispatch_state.as_str() {
                    "FAILED" => bail!("Search job {} failed", sid),
                    "PAUSE" | "PAUSED" => bail!("Search job {} paused unexpectedly", sid),
                    _ => {}
                }
            }

            sleep(self.poll_interval).await;
        }
    }

    fn poll_with_previews_until_done(
        self,
        spl: String,
        preview_interval: Duration,
        stats_fields: Option<StatsFields>,
    ) -> LogItemTryStream {
        Box::pin(try_stream! {
            let sid = self.create_job(&spl, true).await?;
            let start = Instant::now();
            let source_id = next_source_id();
            let mut partial_stream_id: usize = 0;

            loop {
                if start.elapsed() > self.timeout {
                    Err(eyre!("Search job {} timed out after {:?}", sid, self.timeout))?;
                }

                let response = self.fetch_job_status(&sid).await?;

                let Some(entry) = response.entry.first() else {
                    sleep(self.poll_interval).await;
                    continue;
                };

                dbg!(entry.content.is_done);
                dbg!(entry.content.dispatch_state.as_str());
                match entry.content.dispatch_state.as_str() {
                    "FAILED" => Err(eyre!("Search job {} failed", sid))?,
                    "PAUSE" | "PAUSED" => Err(eyre!("Search job {} paused unexpectedly", sid))?,
                    "QUEUED" | "PARSING" | "FINALIZING" => {
                        sleep(self.poll_interval).await;
                        continue;
                    }
                    "RUNNING" => {
                        let key = PartialStreamKey { partial_stream_id, source_id };
                        let url = format!(
                            "{}/services/search/jobs/{}/results_preview",
                            self.base_url, sid
                        );
                        let stream = create_results_stream(
                            self.client.clone(),
                            url,
                            self.auth.clone(),
                            self.batch_size,
                            stats_fields.clone(),
                        );

                        let mut any = false;
                        for await log in stream {
                            any = true;
                            yield LogItem::PartialStreamLog(log?, key);
                        }
                        if any {
                            yield LogItem::PartialStreamDone(key);
                            partial_stream_id += 1;
                        }

                        sleep(preview_interval).await;
                        continue;
                    }
                    "DONE" => {}
                    _ => {
                        sleep(self.poll_interval).await;
                        continue;
                    }
                }

                let url = format!("{}/services/search/jobs/{}/results", self.base_url, sid);
                let stream = create_results_stream(
                    self.client.clone(),
                    url,
                    self.auth.clone(),
                    self.batch_size,
                    stats_fields.clone(),
                );

                for await log in stream {
                    yield LogItem::Log(log?);
                }
                return;
            }
        })
    }
}

fn create_results_stream(
    client: Client,
    url: String,
    auth: SplunkAuth,
    batch_size: u32,
    stats_fields: Option<StatsFields>,
) -> LogTryStream {
    match stats_fields {
        Some((ts, num)) => {
            fetch_results_with_stats_transform(client, url, auth, batch_size, ts, num)
        }
        None => fetch_results_stream(client, url, auth, batch_size),
    }
}

fn fetch_results_stream(
    client: Client,
    url: String,
    auth: SplunkAuth,
    batch_size: u32,
) -> LogTryStream {
    fetch_results_with_transform(
        client,
        url,
        auth,
        batch_size,
        SplunkConnector::transform_log,
    )
}

fn fetch_results_with_stats_transform(
    client: Client,
    url: String,
    auth: SplunkAuth,
    batch_size: u32,
    timestamp_fields: HashSet<String>,
    numeric_fields: HashSet<String>,
) -> LogTryStream {
    fetch_results_with_transform(client, url, auth, batch_size, move |mut log| {
        for key in &timestamp_fields {
            if let Some(v) = log.get_mut(key) {
                SplunkConnector::value_to_datetime(v)?;
            }
        }
        for key in &numeric_fields {
            if let Some(v) = log.get_mut(key) {
                let old = std::mem::replace(v, Value::Null);
                *v = try_parse_numeric(old);
            }
        }
        Ok(log)
    })
}

fn fetch_results_with_transform<F>(
    client: Client,
    url: String,
    auth: SplunkAuth,
    batch_size: u32,
    mut transform: F,
) -> LogTryStream
where
    F: FnMut(Log) -> Result<Log> + Send + 'static,
{
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
                yield transform(log)?;
                offset += 1;
            }
        }
    })
}

fn try_parse_numeric(value: Value) -> Value {
    if let Value::String(s) = &value {
        if let Ok(i) = s.parse::<i64>() {
            return Value::Int(i);
        }
        if let Ok(f) = s.parse::<f64>() {
            return Value::Float(f);
        }
    }
    value
}
