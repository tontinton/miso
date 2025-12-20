use std::{
    any::Any,
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};

use async_stream::try_stream;
use axum::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::BytesMut;
use color_eyre::eyre::{Context, Result, bail, eyre};
use hashbrown::{HashMap, HashSet};
use miso_common::{
    humantime_utils::{deserialize_duration, serialize_duration},
    metrics::{
        CONNECTOR_SPLUNK, METRICS, OP_CREATE_JOB, OP_FETCH_RESULTS, OP_GET_INDEXES, OP_POLL_JOB,
    },
    run_at_interval::run_at_interval,
    shutdown_future::ShutdownFuture,
};
use miso_workflow_types::{
    expr::Expr,
    log::{Log, LogTryStream},
    project::ProjectField,
    sort::Sort,
    summarize::{Aggregation, Summarize},
    value::Value,
};
use parking_lot::RwLock;
use reqwest::{Client, RequestBuilder, Response, header};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use time::OffsetDateTime;
use tracing::{debug, error, info, instrument};

use crate::{Collection, instrumentation::record_operation_result};

use super::{Connector, ConnectorError, QueryHandle, QueryResponse, Split, downcast_unwrap};

const SPLUNK_TIME_FIELD: &str = "_time";
const SPLUNK_RAW_FIELD: &str = "_raw";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[derive(Default)]
pub enum SplunkAuth {
    #[default]
    None,
    Basic {
        username: String,
        password: String,
    },
    Token {
        token: String,
    },
}

impl SplunkAuth {
    fn apply_to_request(&self, req: RequestBuilder) -> RequestBuilder {
        match self {
            SplunkAuth::None => req,
            SplunkAuth::Basic { username, password } => {
                let credentials = format!("{username}:{password}");
                let encoded = BASE64.encode(credentials);
                req.header(header::AUTHORIZATION, format!("Basic {encoded}"))
            }
            SplunkAuth::Token { token } => {
                req.header(header::AUTHORIZATION, format!("Bearer {token}"))
            }
        }
    }
}

fn default_refresh_interval() -> Duration {
    humantime::parse_duration("1m").expect("Invalid duration format")
}

fn default_job_poll_interval() -> Duration {
    humantime::parse_duration("500ms").expect("Invalid duration format")
}

fn default_job_timeout() -> Duration {
    humantime::parse_duration("5m").expect("Invalid duration format")
}

fn default_result_batch_size() -> u32 {
    50000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplunkConfig {
    pub url: String,

    #[serde(default)]
    pub auth: SplunkAuth,

    #[serde(
        default = "default_refresh_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub refresh_interval: Duration,

    #[serde(
        default = "default_job_poll_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub job_poll_interval: Duration,

    #[serde(
        default = "default_job_timeout",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub job_timeout: Duration,

    #[serde(default = "default_result_batch_size")]
    pub result_batch_size: u32,

    #[serde(default)]
    pub accept_invalid_certs: bool,
}

impl SplunkConfig {
    pub fn new(url: String) -> Self {
        Self {
            url,
            auth: SplunkAuth::None,
            refresh_interval: default_refresh_interval(),
            job_poll_interval: default_job_poll_interval(),
            job_timeout: default_job_timeout(),
            result_batch_size: default_result_batch_size(),
            accept_invalid_certs: false,
        }
    }

    pub fn new_with_interval(url: String, refresh_interval: Duration) -> Self {
        Self {
            url,
            auth: SplunkAuth::None,
            refresh_interval,
            job_poll_interval: default_job_poll_interval(),
            job_timeout: default_job_timeout(),
            result_batch_size: default_result_batch_size(),
            accept_invalid_certs: false,
        }
    }
}

/// A single stage in the SPL pipeline
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum SplunkOp {
    /// Search filter: `field=value` or complex boolean expressions
    Search(String),
    /// Sort: `| sort -field1, +field2`
    Sort(Vec<(String, bool)>), // (field, descending)
    /// Head/limit: `| head N`
    Head(u64),
    /// Stats aggregation: `| stats count, sum(x) by y`
    Stats {
        aggs: String,
        by: Vec<String>,
        timestamp_agg_fields: HashSet<String>,
        numeric_agg_fields: HashSet<String>,
    },
    /// Count: `| stats count`
    Count,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SplunkHandle {
    /// Additional indexes for union operations
    indexes: Vec<String>,
    pipeline: Vec<SplunkOp>,
}

#[typetag::serde]
impl QueryHandle for SplunkHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl SplunkHandle {
    fn push(&self, op: SplunkOp) -> Self {
        let mut handle = self.clone();
        handle.pipeline.push(op);
        handle
    }

    fn with_union(&self, index: &str) -> Self {
        let mut handle = self.clone();
        handle.indexes.push(index.to_string());
        handle
    }

    fn build_spl(&self, collection: &str) -> String {
        let mut spl = String::new();

        let mut indexes = vec![collection.to_string()];
        indexes.extend(self.indexes.iter().cloned());
        indexes.sort();
        indexes.dedup();

        let index_clause = indexes
            .iter()
            .map(|idx| format!("index=\"{}\"", idx))
            .collect::<Vec<_>>()
            .join(" OR ");

        spl.push_str(&format!("search ({})", index_clause));

        for op in &self.pipeline {
            match op {
                SplunkOp::Search(term) => {
                    spl.push_str(" | where ");
                    spl.push_str(term);
                }
                SplunkOp::Sort(sorts) => {
                    spl.push_str(" | sort ");
                    let sort_clause = sorts
                        .iter()
                        .map(|(field, desc)| {
                            if *desc {
                                format!("-{}", field)
                            } else {
                                format!("+{}", field)
                            }
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    spl.push_str(&sort_clause);
                }
                SplunkOp::Head(n) => {
                    spl.push_str(&format!(" | head {}", n));
                }
                SplunkOp::Stats { aggs, by, .. } => {
                    spl.push_str(" | stats ");
                    spl.push_str(aggs);
                    if !by.is_empty() {
                        spl.push_str(" by ");
                        spl.push_str(&by.join(", "));
                    }
                }
                SplunkOp::Count => {
                    spl.push_str(" | stats count");
                }
            }
        }

        spl
    }

    fn has_count(&self) -> bool {
        self.pipeline.iter().any(|op| matches!(op, SplunkOp::Count))
    }

    fn has_stats(&self) -> bool {
        self.pipeline
            .iter()
            .any(|op| matches!(op, SplunkOp::Stats { .. }))
    }

    /// Get the stats timestamp fields if present
    fn get_stats_timestamp_fields(&self) -> Option<&HashSet<String>> {
        for op in &self.pipeline {
            if let SplunkOp::Stats {
                timestamp_agg_fields,
                ..
            } = op
            {
                return Some(timestamp_agg_fields);
            }
        }
        None
    }

    /// Get the stats numeric fields if present
    fn get_stats_numeric_fields(&self) -> Option<&HashSet<String>> {
        for op in &self.pipeline {
            if let SplunkOp::Stats {
                numeric_agg_fields, ..
            } = op
            {
                return Some(numeric_agg_fields);
            }
        }
        None
    }
}

impl fmt::Display for SplunkHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut items = Vec::new();

        if !self.indexes.is_empty() {
            items.push(format!("unions=[{}]", self.indexes.join(", ")));
        }

        for op in &self.pipeline {
            match op {
                SplunkOp::Search(term) => items.push(format!("search={}", term)),
                SplunkOp::Sort(sorts) => {
                    let s = sorts
                        .iter()
                        .map(|(field, desc)| format!("{}{}", if *desc { "-" } else { "+" }, field))
                        .collect::<Vec<_>>()
                        .join(", ");
                    items.push(format!("sort={}", s));
                }
                SplunkOp::Head(n) => items.push(format!("head={}", n)),
                SplunkOp::Stats { aggs, by, .. } => {
                    if by.is_empty() {
                        items.push(format!("stats={}", aggs));
                    } else {
                        items.push(format!("stats={} by {}", aggs, by.join(", ")));
                    }
                }
                SplunkOp::Count => items.push("count".to_string()),
            }
        }

        write!(f, "{}", items.join(", "))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SplunkIndex;

type SplunkIndexes = HashMap<String, SplunkIndex>;

#[derive(Debug)]
pub struct SplunkConnector {
    config: SplunkConfig,
    indexes: Arc<RwLock<SplunkIndexes>>,
    interval_task: ShutdownFuture,
    client: Client,
}

impl Serialize for SplunkConnector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let config_json = serde_json::to_value(&self.config).map_err(serde::ser::Error::custom)?;
        let config_map = config_json
            .as_object()
            .ok_or_else(|| serde::ser::Error::custom("config must serialize to a JSON object"))?;

        let mut map = serializer.serialize_map(Some(config_map.len() + 1))?;
        for (k, v) in config_map {
            map.serialize_entry(k, v)?;
        }
        map.serialize_entry("indexes", &*self.indexes.read())?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for SplunkConnector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        SplunkConfig::deserialize(deserializer).map(SplunkConnector::new)
    }
}

// Splunk API response types
#[derive(Debug, Deserialize)]
struct CreateJobResponse {
    sid: String,
}

#[derive(Debug, Deserialize)]
struct JobStatusEntry {
    #[serde(rename = "dispatchState")]
    dispatch_state: String,

    #[serde(rename = "isDone")]
    is_done: bool,

    #[serde(rename = "resultCount", default)]
    result_count: u64,
}

#[derive(Debug, Deserialize)]
struct JobStatusResponse {
    entry: Vec<JobStatusResponseEntry>,
}

#[derive(Debug, Deserialize)]
struct JobStatusResponseEntry {
    content: JobStatusEntry,
}

#[derive(Debug, Deserialize)]
struct ResultsResponse {
    results: Vec<Log>,
}

#[derive(Debug, Deserialize)]
struct IndexListResponse {
    entry: Vec<IndexListResponseEntry>,
}

#[derive(Debug, Deserialize)]
struct IndexListResponseEntry {
    name: String,
}

async fn response_to_bytes(response: Response, connector: &str) -> Result<BytesMut> {
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        bail!(ConnectorError::ServerResp(status.as_u16(), text));
    }
    let bytes = response.bytes().await.context("bytes from response")?;
    METRICS
        .downloaded_bytes
        .with_label_values(&[connector])
        .inc_by(bytes.len() as u64);
    Ok(bytes.into())
}

#[instrument(skip_all, name = "splunk send_request")]
async fn send_request(req: RequestBuilder) -> Result<BytesMut> {
    match req.send().await {
        Ok(response) => response_to_bytes(response, CONNECTOR_SPLUNK).await,
        Err(e) => Err(eyre!(ConnectorError::Http(e))),
    }
}

fn format_spl_value(value: &Value) -> String {
    match value {
        Value::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::Timestamp(dt) => dt
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| value.to_string()),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        _ => value.to_string(),
    }
}

fn compile_filter_to_spl(expr: &Expr) -> Option<String> {
    Some(match expr {
        Expr::Or(left, right) => {
            let left_spl = compile_filter_to_spl(left)?;
            let right_spl = compile_filter_to_spl(right)?;
            format!("({} OR {})", left_spl, right_spl)
        }
        Expr::And(left, right) => {
            let left_spl = compile_filter_to_spl(left)?;
            let right_spl = compile_filter_to_spl(right)?;
            format!("({} AND {})", left_spl, right_spl)
        }
        Expr::Not(inner) => {
            let inner_spl = compile_filter_to_spl(inner)?;
            format!("NOT {}", inner_spl)
        }

        Expr::Exists(field) if !field.has_array_access() => {
            format!("isnotnull({})", field)
        }

        Expr::Eq(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            format!("{}={}", field, format_spl_value(value))
        }
        Expr::Ne(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            format!("{}!={}", field, format_spl_value(value))
        }
        Expr::Gt(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            format!("{}>{}", field, format_spl_value(value))
        }
        Expr::Gte(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            format!("{}>={}", field, format_spl_value(value))
        }
        Expr::Lt(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            format!("{}<{}", field, format_spl_value(value))
        }
        Expr::Lte(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            format!("{}<={}", field, format_spl_value(value))
        }

        Expr::In(lhs, values) => {
            let Expr::Field(field) = &**lhs else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            let formatted_values = values
                .iter()
                .map(|v| match v {
                    Expr::Literal(val) => Some(format_spl_value(val)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;

            let conditions = formatted_values
                .iter()
                .map(|v| format!("{}={}", field, v))
                .collect::<Vec<_>>()
                .join(" OR ");
            format!("({})", conditions)
        }

        Expr::StartsWith(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(prefix)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            let prefix_str = match prefix {
                Value::String(s) => s.clone(),
                _ => return None,
            };
            format!("{}={}*", field, prefix_str)
        }
        Expr::HasCs(lhs, rhs) | Expr::Has(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(phrase)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            // Use LIKE operator with wildcards for substring matching
            format!(
                "like({}, \"%{}%\")",
                field,
                match phrase {
                    Value::String(s) => s
                        .replace('\\', "\\\\")
                        .replace('"', "\\\"")
                        .replace('%', "\\%"),
                    _ => return None,
                }
            )
        }

        _ => return None,
    })
}

impl SplunkConnector {
    pub fn new(config: SplunkConfig) -> Self {
        let indexes = Arc::new(RwLock::new(HashMap::new()));
        let interval_task = ShutdownFuture::new(
            refresh_indexes_at_interval(config.clone(), Arc::downgrade(&indexes)),
            "Splunk indexes refresher",
        );

        let client = Client::builder()
            .danger_accept_invalid_certs(config.accept_invalid_certs)
            .build()
            .expect("build HTTP client");

        Self {
            config,
            indexes,
            interval_task,
            client,
        }
    }
}

#[instrument(skip(client, auth), name = "splunk get_indexes")]
async fn get_indexes(client: &Client, base_url: &str, auth: &SplunkAuth) -> Result<SplunkIndexes> {
    let start = std::time::Instant::now();
    METRICS
        .connector_requests_total
        .with_label_values(&[CONNECTOR_SPLUNK, OP_GET_INDEXES])
        .inc();

    let result: Result<SplunkIndexes> = async {
        let url = format!("{base_url}/services/data/indexes?output_mode=json");
        let req = auth.apply_to_request(client.get(&url));
        let mut bytes = send_request(req).await?;
        let response: IndexListResponse =
            simd_json::serde::from_slice(bytes.as_mut()).context("parse indexes response")?;

        let mut indexes = HashMap::new();
        for entry in response.entry {
            indexes.insert(entry.name, SplunkIndex);
        }

        Ok(indexes)
    }
    .await;

    let duration = start.elapsed().as_secs_f64();
    record_operation_result(CONNECTOR_SPLUNK, OP_GET_INDEXES, &result, duration);
    result
}

async fn refresh_indexes_at_interval(
    config: SplunkConfig,
    weak_indexes: Weak<RwLock<SplunkIndexes>>,
) {
    let client = Client::builder()
        .danger_accept_invalid_certs(config.accept_invalid_certs)
        .build()
        .expect("build HTTP client");

    run_at_interval(
        async || {
            let Some(indexes) = weak_indexes.upgrade() else {
                return false;
            };

            match get_indexes(&client, &config.url, &config.auth).await {
                Ok(response) => {
                    debug!("Got Splunk indexes: {:?}", &response);
                    let mut guard = indexes.write();
                    *guard = response;
                }
                Err(e) => {
                    error!("Failed to get Splunk indexes: {:#}", e);
                }
            }

            true
        },
        config.refresh_interval,
    )
    .await;
}

#[instrument(skip(client, auth), name = "splunk create_job")]
async fn create_search_job(
    client: &Client,
    base_url: &str,
    spl: &str,
    auth: &SplunkAuth,
) -> Result<String> {
    let start = std::time::Instant::now();
    METRICS
        .connector_requests_total
        .with_label_values(&[CONNECTOR_SPLUNK, OP_CREATE_JOB])
        .inc();

    let url = format!("{base_url}/services/search/jobs");

    let form = [
        ("search", spl),
        ("output_mode", "json"),
        ("exec_mode", "normal"),
    ];

    let req = auth.apply_to_request(client.post(&url)).form(&form);

    let result: Result<String> = async {
        let mut bytes = send_request(req).await?;
        let response: CreateJobResponse =
            simd_json::serde::from_slice(bytes.as_mut()).context("parse job creation response")?;
        Ok(response.sid)
    }
    .await;

    let duration = start.elapsed().as_secs_f64();
    record_operation_result(CONNECTOR_SPLUNK, OP_CREATE_JOB, &result, duration);
    result
}

#[instrument(skip(client, auth), name = "splunk poll_job")]
async fn poll_job_completion(
    client: &Client,
    base_url: &str,
    sid: &str,
    auth: &SplunkAuth,
    poll_interval: Duration,
    timeout: Duration,
) -> Result<u64> {
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            bail!("Search job {} timed out after {:?}", sid, timeout);
        }

        METRICS
            .connector_requests_total
            .with_label_values(&[CONNECTOR_SPLUNK, OP_POLL_JOB])
            .inc();

        let url = format!("{base_url}/services/search/jobs/{sid}?output_mode=json");
        let req = auth.apply_to_request(client.get(&url));

        let mut bytes = send_request(req).await?;
        let response: JobStatusResponse =
            simd_json::serde::from_slice(bytes.as_mut()).context("parse job status response")?;

        if let Some(entry) = response.entry.first() {
            if entry.content.is_done {
                return Ok(entry.content.result_count);
            }

            match entry.content.dispatch_state.as_str() {
                "FAILED" => bail!("Search job {} failed", sid),
                "PAUSED" => bail!("Search job {} paused unexpectedly", sid),
                _ => {}
            }
        }

        tokio::time::sleep(poll_interval).await;
    }
}

#[instrument(skip(client, auth), name = "splunk fetch_results")]
async fn fetch_results(
    client: &Client,
    base_url: &str,
    sid: &str,
    auth: &SplunkAuth,
    batch_size: u32,
) -> Result<LogTryStream> {
    let url = format!("{base_url}/services/search/jobs/{sid}/results");
    let client = client.clone();
    let auth = auth.clone();

    Ok(Box::pin(try_stream! {
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
    }))
}

impl SplunkConnector {
    fn transform_log(mut log: Log) -> Result<Log> {
        // Parse _raw field if it's valid JSON to get properly-typed values.
        // Splunk only extracts fields that are searched for, but _raw contains
        // the original event with all fields and proper JSON types.
        if let Some(Value::String(raw_str)) = log.get(SPLUNK_RAW_FIELD)
            && let Ok(raw_json) = serde_json::from_str::<serde_json::Value>(raw_str)
            && let serde_json::Value::Object(raw_map) = raw_json
        {
            for (key, json_value) in raw_map {
                log.insert(key, Value::from(json_value));
            }
        }

        if let Some(value) = log.get_mut(SPLUNK_TIME_FIELD) {
            Self::value_to_datetime(value)?;
        }

        // Remove unimportant Splunk internal metadata fields.
        log.remove("_serial");
        log.remove("_bkt");
        log.remove("_cd");
        log.remove("_si");

        Ok(log)
    }

    fn value_to_datetime(value: &mut Value) -> Result<()> {
        match value {
            Value::String(s) => {
                // Try RFC3339 first
                if let Ok(dt) =
                    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                {
                    *value = Value::from(dt);
                } else if let Ok(epoch) = s.parse::<f64>() {
                    // Splunk sometimes returns epoch as string
                    let dt = miso_common::time_utils::parse_timestamp_float(epoch)
                        .map_err(|e| color_eyre::eyre::eyre!("parse splunk datetime: {}", e))?;
                    *value = Value::from(dt);
                }
            }
            Value::Float(f) => {
                let dt = miso_common::time_utils::parse_timestamp_float(*f)
                    .map_err(|e| color_eyre::eyre::eyre!("parse splunk datetime: {}", e))?;
                *value = Value::from(dt)
            }
            Value::Int(n) => {
                let dt = miso_common::time_utils::parse_timestamp(*n)
                    .map_err(|e| color_eyre::eyre::eyre!("parse splunk datetime: {}", e))?;
                *value = Value::from(dt)
            }
            Value::UInt(n) => {
                let dt = miso_common::time_utils::parse_timestamp(*n as i64)
                    .map_err(|e| color_eyre::eyre::eyre!("parse splunk datetime: {}", e))?;
                *value = Value::from(dt)
            }
            _ => {}
        }
        Ok(())
    }

    fn transform_agg_timestamp_value(
        key: &str,
        mut value: Value,
        agg_timestamp_fields: &HashSet<String>,
    ) -> Result<Value> {
        if agg_timestamp_fields.contains(key) {
            Self::value_to_datetime(&mut value)?;
        }
        Ok(value)
    }

    /// Convert numeric strings to proper numeric types.
    /// Splunk stats returns all values as strings, so we try to parse them.
    fn try_parse_numeric_string(value: Value) -> Value {
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

    #[allow(clippy::too_many_arguments)]
    async fn query_with_stats(
        client: Client,
        url: String,
        collection: String,
        handle: SplunkHandle,
        auth: SplunkAuth,
        poll_interval: Duration,
        timeout: Duration,
        batch_size: u32,
    ) -> Result<LogTryStream> {
        let spl = handle.build_spl(&collection);
        info!("Splunk stats search: {}", spl);

        let sid = create_search_job(&client, &url, &spl, &auth).await?;
        let _result_count =
            poll_job_completion(&client, &url, &sid, &auth, poll_interval, timeout).await?;

        let timestamp_agg_fields = handle.get_stats_timestamp_fields().cloned();
        let numeric_agg_fields = handle.get_stats_numeric_fields().cloned();

        let results_url = format!("{url}/services/search/jobs/{sid}/results");

        Ok(Box::pin(try_stream! {
            let mut offset = 0u64;

            loop {
                METRICS
                    .connector_requests_total
                    .with_label_values(&[CONNECTOR_SPLUNK, OP_FETCH_RESULTS])
                    .inc();

                let req = auth.apply_to_request(
                    client.get(&results_url)
                        .query(&[
                            ("output_mode", "json"),
                            ("offset", &offset.to_string()),
                            ("count", &batch_size.to_string()),
                        ])
                );

                let mut bytes = send_request(req).await?;
                let response: ResultsResponse =
                    simd_json::serde::from_slice(bytes.as_mut())
                        .context("parse stats results response")?;

                if response.results.is_empty() {
                    return;
                }

                for mut log in response.results {
                    if let Some(ref ts_fields) = timestamp_agg_fields {
                        for (key, value) in log.clone().iter() {
                            if ts_fields.contains(key)
                                && let Some(v) = log.get_mut(key) {
                                    *v = Self::transform_agg_timestamp_value(key, value.clone(), ts_fields)?;
                                }
                        }
                    }
                    if let Some(ref num_fields) = numeric_agg_fields {
                        for key in num_fields {
                            if let Some(v) = log.get_mut(key) {
                                let old = std::mem::replace(v, Value::Null);
                                *v = Self::try_parse_numeric_string(old);
                            }
                        }
                    }
                    yield log;
                    offset += 1;
                }
            }
        }))
    }
}

#[async_trait]
#[typetag::serde(name = "splunk")]
impl Connector for SplunkConnector {
    #[instrument(skip(self), name = "Splunk get collection")]
    fn get_collection(&self, collection: &str) -> Option<Collection> {
        {
            let guard = self.indexes.read();
            let _ = guard.get(collection)?;
        }

        let mut static_fields = HashMap::new();
        static_fields.insert("@time".to_string(), SPLUNK_TIME_FIELD.to_string());

        Some(Collection { static_fields })
    }

    fn get_handle(&self, _collection: &str) -> Result<Box<dyn QueryHandle>> {
        Ok(Box::new(SplunkHandle::default()))
    }

    #[instrument(skip(self), name = "Splunk query")]
    async fn query(
        &self,
        collection: &str,
        handle: &dyn QueryHandle,
        _split: Option<&dyn Split>,
    ) -> Result<QueryResponse> {
        let url = self.config.url.clone();
        let auth = self.config.auth.clone();

        let handle = downcast_unwrap!(handle, SplunkHandle);
        let has_count = handle.has_count();
        let has_stats = handle.has_stats();

        let spl = handle.build_spl(collection);

        info!(?has_count, "Splunk search '{}': {}", collection, spl);

        if has_count && !has_stats {
            let sid = create_search_job(&self.client, &url, &spl, &auth).await?;
            let result_count = poll_job_completion(
                &self.client,
                &url,
                &sid,
                &auth,
                self.config.job_poll_interval,
                self.config.job_timeout,
            )
            .await?;

            let results_url =
                format!("{url}/services/search/jobs/{sid}/results?output_mode=json&count=1");
            let req = auth.apply_to_request(self.client.get(&results_url));
            let mut bytes = send_request(req).await?;
            let response: ResultsResponse =
                simd_json::serde::from_slice(bytes.as_mut()).context("parse count response")?;

            let count = if let Some(first) = response.results.first() {
                first
                    .get("count")
                    .and_then(|v| match v {
                        Value::UInt(n) => Some(*n),
                        Value::Int(n) => Some(*n as u64),
                        Value::String(s) => s.parse().ok(),
                        _ => None,
                    })
                    .unwrap_or(result_count)
            } else {
                result_count
            };

            return Ok(QueryResponse::Count(count));
        }

        if has_stats {
            return Ok(QueryResponse::Logs(
                Self::query_with_stats(
                    self.client.clone(),
                    url,
                    collection.to_string(),
                    handle.clone(),
                    auth,
                    self.config.job_poll_interval,
                    self.config.job_timeout,
                    self.config.result_batch_size,
                )
                .await?,
            ));
        }

        let sid = create_search_job(&self.client, &url, &spl, &auth).await?;
        let _result_count = poll_job_completion(
            &self.client,
            &url,
            &sid,
            &auth,
            self.config.job_poll_interval,
            self.config.job_timeout,
        )
        .await?;

        Ok(QueryResponse::Logs(
            fetch_results(
                &self.client,
                &url,
                &sid,
                &auth,
                self.config.result_batch_size,
            )
            .await?,
        ))
    }

    fn apply_filter(&self, ast: &Expr, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, SplunkHandle);
        let spl = compile_filter_to_spl(ast)?;
        Some(Box::new(handle.push(SplunkOp::Search(spl))))
    }

    fn apply_project(
        &self,
        _projections: &[ProjectField],
        _handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        // Splunk's `| fields` command doesn't restrict what's in `_raw`,
        // so we can't properly push down projections. Let the workflow
        // layer handle projections instead.
        None
    }

    fn apply_limit(&self, max: u64, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, SplunkHandle);
        Some(Box::new(handle.push(SplunkOp::Head(max))))
    }

    fn apply_topn(
        &self,
        sorts: &[Sort],
        max: u64,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, SplunkHandle);

        let splunk_sorts: Vec<(String, bool)> = sorts
            .iter()
            .map(|s| (s.by.to_string(), s.order.to_string() == "desc"))
            .collect();

        Some(Box::new(
            handle
                .push(SplunkOp::Sort(splunk_sorts))
                .push(SplunkOp::Head(max)),
        ))
    }

    fn apply_count(&self, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, SplunkHandle);
        Some(Box::new(handle.push(SplunkOp::Count)))
    }

    fn apply_summarize(
        &self,
        config: &Summarize,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        // Splunk's `| stats by field` with no aggregations returns all default
        // stats columns, which doesn't match our distinct operation behavior.
        // Don't push down summarize when there are no aggregations.
        if config.aggs.is_empty() {
            return None;
        }

        let handle = downcast_unwrap!(handle, SplunkHandle);

        let mut agg_parts = Vec::new();
        let mut timestamp_agg_fields = HashSet::new();
        let mut numeric_agg_fields = HashSet::new();

        for (output_field, agg) in &config.aggs {
            let agg_str = match agg {
                Aggregation::Count => {
                    numeric_agg_fields.insert(output_field.to_string());
                    format!("count as {}", output_field)
                }
                Aggregation::Min(agg_field) => {
                    if agg_field.to_string() == SPLUNK_TIME_FIELD {
                        timestamp_agg_fields.insert(output_field.to_string());
                    } else {
                        numeric_agg_fields.insert(output_field.to_string());
                    }
                    format!("min({}) as {}", agg_field, output_field)
                }
                Aggregation::Max(agg_field) => {
                    if agg_field.to_string() == SPLUNK_TIME_FIELD {
                        timestamp_agg_fields.insert(output_field.to_string());
                    } else {
                        numeric_agg_fields.insert(output_field.to_string());
                    }
                    format!("max({}) as {}", agg_field, output_field)
                }
                Aggregation::Sum(agg_field) => {
                    numeric_agg_fields.insert(output_field.to_string());
                    format!("sum({}) as {}", agg_field, output_field)
                }
                Aggregation::Avg(agg_field) => {
                    numeric_agg_fields.insert(output_field.to_string());
                    format!("avg({}) as {}", agg_field, output_field)
                }
                Aggregation::DCount(agg_field) => {
                    numeric_agg_fields.insert(output_field.to_string());
                    format!("dc({}) as {}", agg_field, output_field)
                }
                Aggregation::Countif(Expr::Exists(agg_field)) => {
                    numeric_agg_fields.insert(output_field.to_string());
                    format!("count(eval(isnotnull({}))) as {}", agg_field, output_field)
                }
                Aggregation::Countif(..) => return None,
            };
            agg_parts.push(agg_str);
        }

        let aggs = agg_parts.join(", ");

        let by_fields: Vec<String> = config
            .by
            .iter()
            .filter_map(|expr| match expr {
                Expr::Field(field) => Some(field.to_string()),
                // Splunk's binning syntax is different and more complex.
                // For now, don't push down summarize when binning is involved.
                Expr::Bin(..) => None,
                _ => None,
            })
            .collect();

        if by_fields.len() != config.by.len() {
            return None;
        }

        Some(Box::new(handle.push(SplunkOp::Stats {
            aggs,
            by: by_fields,
            timestamp_agg_fields,
            numeric_agg_fields,
        })))
    }

    fn apply_union(
        &self,
        _scan_collection: &str,
        union_collection: &str,
        handle: &dyn QueryHandle,
        union_handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, SplunkHandle);
        let union_handle = downcast_unwrap!(union_handle, SplunkHandle);
        if handle.pipeline != union_handle.pipeline {
            return None;
        }
        Some(Box::new(handle.with_union(union_collection)))
    }

    #[instrument(skip(self), name = "Splunk close")]
    async fn close(&self) {
        self.interval_task.shutdown().await;
    }
}
