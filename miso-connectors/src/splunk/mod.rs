mod query_runner;

use std::{
    any::Any,
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::BytesMut;
use color_eyre::eyre::{Context, Result, bail, eyre};
use hashbrown::{HashMap, HashSet};
use miso_common::{
    humantime_utils::{deserialize_duration, serialize_duration},
    metrics::{CONNECTOR_SPLUNK, METRICS, OP_GET_INDEXES},
    run_at_interval::run_at_interval,
    shutdown_future::ShutdownFuture,
};
use miso_workflow_types::{
    expr::Expr,
    field::Field,
    log::{COUNT_FIELD_NAME, Log},
    project::ProjectField,
    sort::{Sort, SortOrder},
    summarize::{Aggregation, Summarize},
    value::Value,
};
use parking_lot::RwLock;
use reqwest::{Client, RequestBuilder, Response, header};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use time::OffsetDateTime;
use tracing::{debug, error, info, instrument};

use crate::{
    Collection, instrumentation::record_operation_result, splunk::query_runner::QueryRunner,
};

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

fn default_preview_interval() -> Duration {
    Duration::from_secs(2)
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

    #[serde(default)]
    pub enable_partial_stream: bool,

    #[serde(
        default = "default_preview_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    pub preview_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum SplunkOp {
    /// `| search` - faster, uses indexed lookups
    Search(String),
    /// `| where` - slower, but needed for functions like isnotnull(), like()
    Where(String),
    Sort(Vec<(String, SortOrder)>),
    Head(u64),
    Stats {
        aggs: String,
        by: Vec<String>,
        timestamp_agg_fields: HashSet<String>,
        numeric_agg_fields: HashSet<String>,
    },
    Count,
    /// `| rename old as new, ...` - used to rename fields after stats
    Rename(Vec<(String, String)>),
    /// `| rex field=... "..."` - regex extraction
    Rex {
        field: String,
        pattern: String,
        output_field: String,
    },
    /// `| fields - field1, field2, ...` - remove fields from output
    FieldsRemove(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SplunkHandle {
    indexes: Vec<String>,
    pipeline: Vec<SplunkOp>,
    earliest: Option<i64>,
    latest: Option<i64>,
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

    fn with_time_constraints(&self, earliest: Option<i64>, latest: Option<i64>) -> Self {
        let mut handle = self.clone();
        if let Some(e) = earliest {
            handle.earliest = Some(match handle.earliest {
                Some(existing) => existing.max(e),
                None => e,
            });
        }
        if let Some(l) = latest {
            handle.latest = Some(match handle.latest {
                Some(existing) => existing.min(l),
                None => l,
            });
        }
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

        // tstats is much faster for simple count queries without filters
        if self.can_use_tstats() {
            spl.push_str(&format!(
                "| tstats count as {COUNT_FIELD_NAME} where ({index_clause})",
            ));
            if let Some(earliest) = self.earliest {
                spl.push_str(&format!(" earliest={}", earliest));
            }
            if let Some(latest) = self.latest {
                spl.push_str(&format!(" latest={}", latest));
            }
            return spl;
        }

        spl.push_str(&format!("search ({})", index_clause));

        // earliest/latest in base clause is more efficient than filtering
        if let Some(earliest) = self.earliest {
            spl.push_str(&format!(" earliest={}", earliest));
        }
        if let Some(latest) = self.latest {
            spl.push_str(&format!(" latest={}", latest));
        }

        for op in &self.pipeline {
            match op {
                SplunkOp::Search(term) => {
                    spl.push_str(" | search ");
                    spl.push_str(term);
                }
                SplunkOp::Where(term) => {
                    spl.push_str(" | where ");
                    spl.push_str(term);
                }
                SplunkOp::Sort(sorts) => {
                    spl.push_str(" | sort ");
                    let sort_clause = sorts
                        .iter()
                        .map(|(field, order)| match order {
                            SortOrder::Asc => format!("+{}", field),
                            SortOrder::Desc => format!("-{}", field),
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
                    spl.push_str(&format!(" | stats count as {COUNT_FIELD_NAME}"));
                }
                SplunkOp::Rename(renames) => {
                    spl.push_str(" | rename ");
                    let rename_clause = renames
                        .iter()
                        .map(|(from, to)| format!("{} as {}", from, to))
                        .collect::<Vec<_>>()
                        .join(", ");
                    spl.push_str(&rename_clause);
                }
                SplunkOp::Rex {
                    field,
                    pattern,
                    output_field: _,
                } => {
                    spl.push_str(&format!(" | rex field={} \"{}\"", field, pattern));
                }
                SplunkOp::FieldsRemove(fields) => {
                    spl.push_str(&format!(" | fields - {}", fields.join(", ")));
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

    fn can_use_tstats(&self) -> bool {
        self.pipeline.len() == 1 && matches!(self.pipeline.first(), Some(SplunkOp::Count))
    }

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
        let mut items = Vec::with_capacity(1 + self.pipeline.len());

        if !self.indexes.is_empty() {
            items.push(format!("unions=[{}]", self.indexes.join(", ")));
        }

        for op in &self.pipeline {
            match op {
                SplunkOp::Search(term) => items.push(format!("search={}", term)),
                SplunkOp::Where(term) => items.push(format!("where={}", term)),
                SplunkOp::Sort(sorts) => {
                    let s = sorts
                        .iter()
                        .map(|(field, order)| {
                            format!(
                                "{}{}",
                                match order {
                                    SortOrder::Asc => "+",
                                    SortOrder::Desc => "-",
                                },
                                field
                            )
                        })
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
                SplunkOp::Rename(renames) => {
                    let s = renames
                        .iter()
                        .map(|(from, to)| format!("{} as {}", from, to))
                        .collect::<Vec<_>>()
                        .join(", ");
                    items.push(format!("rename={}", s));
                }
                SplunkOp::Rex {
                    field,
                    pattern,
                    output_field,
                } => {
                    items.push(format!(
                        "rex field={} \"{}\" -> {}",
                        field, pattern, output_field
                    ));
                }
                SplunkOp::FieldsRemove(fields) => {
                    items.push(format!("fields - {}", fields.join(", ")));
                }
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
pub(super) struct JobStatusEntry {
    #[serde(rename = "dispatchState")]
    pub dispatch_state: String,

    #[serde(rename = "isDone")]
    pub is_done: bool,

    #[serde(rename = "resultCount", default)]
    pub result_count: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct JobStatusResponse {
    pub entry: Vec<JobStatusResponseEntry>,
}

#[derive(Debug, Deserialize)]
pub(super) struct JobStatusResponseEntry {
    pub content: JobStatusEntry,
}

#[derive(Debug, Deserialize)]
pub(super) struct ResultsResponse {
    pub results: Vec<Log>,
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
pub(super) async fn send_request(req: RequestBuilder) -> Result<BytesMut> {
    match req.send().await {
        Ok(response) => response_to_bytes(response, CONNECTOR_SPLUNK).await,
        Err(e) => Err(eyre!(ConnectorError::Http(e))),
    }
}

fn is_timestamp_field(field: &Field) -> bool {
    let field_str = field.to_string();
    field_str == SPLUNK_TIME_FIELD || field_str == "@time"
}

#[derive(Debug, Clone)]
enum FilterResult {
    Search(String),
    Where(String),
    /// Rex extraction followed by a where clause filter on the extracted field.
    RexThenWhere {
        rex: SplunkOp,
        filter_field: String,
        filter_value: String,
    },
    /// Multiple ops from AND expressions with mixed search/where children.
    Batched(Vec<SplunkOp>),
}

impl FilterResult {
    fn into_spl_ops(self) -> Vec<SplunkOp> {
        match self {
            FilterResult::Search(s) => vec![SplunkOp::Search(s)],
            FilterResult::Where(s) => vec![SplunkOp::Where(s)],
            FilterResult::RexThenWhere {
                rex,
                filter_field,
                filter_value,
            } => vec![
                rex,
                SplunkOp::Where(format!("{filter_field}={filter_value}")),
                SplunkOp::FieldsRemove(vec![filter_field]),
            ],
            FilterResult::Batched(ops) => ops,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            FilterResult::Search(s) | FilterResult::Where(s) => Some(s),
            FilterResult::RexThenWhere { .. } | FilterResult::Batched(_) => None,
        }
    }

    fn is_where(&self) -> bool {
        matches!(
            self,
            FilterResult::Where(_) | FilterResult::RexThenWhere { .. }
        )
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

/// String values wrapped in CASE() for case-sensitive matching in `| search`
fn format_spl_value_for_search(value: &Value) -> String {
    match value {
        Value::String(s) => {
            let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
            format!("CASE(\"{}\")", escaped)
        }
        _ => format_spl_value(value),
    }
}

/// Compile a filter like `extract(regex, group, field) == value` to a Rex + Where.
fn compile_extract_filter(
    regex_expr: &Expr,
    group_expr: &Expr,
    source_expr: &Expr,
    value: &Value,
) -> Option<FilterResult> {
    let Expr::Literal(Value::String(pattern)) = regex_expr else {
        return None;
    };
    let Expr::Literal(group_val) = group_expr else {
        return None;
    };
    let group = group_val.as_i64()?;
    let Expr::Field(source_field) = source_expr else {
        return None;
    };

    let output_field = format!("_extract_{}", source_field.to_string().replace('.', "_"));

    let splunk_pattern = convert_to_splunk_named_capture(pattern, group, &output_field)?;

    Some(FilterResult::RexThenWhere {
        rex: SplunkOp::Rex {
            field: source_field.to_string(),
            pattern: splunk_pattern,
            output_field: output_field.clone(),
        },
        filter_field: output_field,
        filter_value: format_spl_value(value),
    })
}

fn flatten_binary<'a>(
    expr: &'a Expr,
    destructure: fn(&'a Expr) -> Option<(&'a Expr, &'a Expr)>,
) -> Vec<&'a Expr> {
    if let Some((left, right)) = destructure(expr) {
        let mut result = flatten_binary(left, destructure);
        result.extend(flatten_binary(right, destructure));
        result
    } else {
        vec![expr]
    }
}

fn and_combine(parts: &[&str]) -> String {
    if parts.len() == 1 {
        parts[0].to_string()
    } else {
        format!("({})", parts.join(" AND "))
    }
}

fn compile_filter_to_spl(expr: &Expr) -> Option<FilterResult> {
    Some(match expr {
        Expr::Or(_, _) => {
            let children = flatten_binary(expr, |e| match e {
                Expr::Or(l, r) => Some((l, r)),
                _ => None,
            });
            let results: Vec<FilterResult> = children
                .iter()
                .map(|c| compile_filter_to_spl(c))
                .collect::<Option<Vec<_>>>()?;
            let any_where = results.iter().any(|r| r.is_where());
            let parts: Vec<&str> = results
                .iter()
                .map(|r| r.as_str())
                .collect::<Option<Vec<_>>>()?;
            let combined = format!("({})", parts.join(" OR "));
            if any_where {
                FilterResult::Where(combined)
            } else {
                FilterResult::Search(combined)
            }
        }
        Expr::And(_, _) => {
            let children = flatten_binary(expr, |e| match e {
                Expr::And(l, r) => Some((l, r)),
                _ => None,
            });
            let results: Vec<FilterResult> = children
                .iter()
                .map(|c| compile_filter_to_spl(c))
                .collect::<Option<Vec<_>>>()?;

            let mut search_parts: Vec<&str> = Vec::new();
            let mut where_parts: Vec<&str> = Vec::new();
            let mut complex_ops: Vec<SplunkOp> = Vec::new();

            for result in &results {
                match result {
                    FilterResult::Search(s) => search_parts.push(s),
                    FilterResult::Where(s) => where_parts.push(s),
                    _ => complex_ops.extend(result.clone().into_spl_ops()),
                }
            }

            if where_parts.is_empty() && complex_ops.is_empty() {
                return Some(FilterResult::Search(and_combine(&search_parts)));
            }
            if search_parts.is_empty() && complex_ops.is_empty() {
                return Some(FilterResult::Where(and_combine(&where_parts)));
            }

            let mut ops = Vec::new();
            if !search_parts.is_empty() {
                ops.push(SplunkOp::Search(and_combine(&search_parts)));
            }
            if !where_parts.is_empty() {
                ops.push(SplunkOp::Where(and_combine(&where_parts)));
            }
            ops.extend(complex_ops);
            FilterResult::Batched(ops)
        }
        Expr::Not(inner) => {
            let inner_result = compile_filter_to_spl(inner)?;
            let combined = format!("NOT {}", inner_result.as_str()?);
            if inner_result.is_where() {
                FilterResult::Where(combined)
            } else {
                FilterResult::Search(combined)
            }
        }

        Expr::Exists(_) => {
            let field = expr.as_pushable_exists_field()?;
            FilterResult::Where(format!("isnotnull({})", field))
        }

        Expr::Eq(lhs, rhs) => match (&**lhs, &**rhs) {
            (Expr::Field(field), Expr::Literal(value))
            | (Expr::Literal(value), Expr::Field(field)) => {
                if field.has_array_access() {
                    return None;
                }
                FilterResult::Search(format!("{}={}", field, format_spl_value_for_search(value)))
            }
            (Expr::Extract(regex_expr, group_expr, source_expr), Expr::Literal(value))
            | (Expr::Literal(value), Expr::Extract(regex_expr, group_expr, source_expr)) => {
                compile_extract_filter(regex_expr, group_expr, source_expr, value)?
            }
            _ => return None,
        },
        Expr::Ne(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            FilterResult::Search(format!("{}!={}", field, format_spl_value_for_search(value)))
        }
        Expr::Gt(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            FilterResult::Search(format!("{}>{}", field, format_spl_value(value)))
        }
        Expr::Gte(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            FilterResult::Search(format!("{}>={}", field, format_spl_value(value)))
        }
        Expr::Lt(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            FilterResult::Search(format!("{}<{}", field, format_spl_value(value)))
        }
        Expr::Lte(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            FilterResult::Search(format!("{}<={}", field, format_spl_value(value)))
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
                    Expr::Literal(val) => Some(format_spl_value_for_search(val)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;

            let conditions = formatted_values
                .iter()
                .map(|v| format!("{}={}", field, v))
                .collect::<Vec<_>>()
                .join(" OR ");
            FilterResult::Search(format!("({})", conditions))
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
            FilterResult::Search(format!("{}={}*", field, prefix_str))
        }
        Expr::HasCs(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(Value::String(value))) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            FilterResult::Where(format!(
                "like({}, \"%{}%\")",
                field,
                value
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('%', "\\%"),
            ))
        }
        Expr::Has(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(Value::String(value))) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            FilterResult::Where(format!(
                "like(lower({}), \"%{}%\")",
                field,
                value
                    .to_lowercase()
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('%', "\\%"),
            ))
        }

        _ => return None,
    })
}

/// Convert KQL positional capture group to Splunk named capture.
///
/// For example, pattern `error: (\d+)` with group 1 and output "code"
/// becomes `error: (?<code>\d+)`.
///
/// This function finds the Nth capture group (1-indexed) in the regex pattern
/// and converts it to a named capture group with the specified output name.
fn convert_to_splunk_named_capture(pattern: &str, group: i64, output: &str) -> Option<String> {
    if group <= 0 {
        return None;
    }

    let mut result = String::with_capacity(pattern.len() + output.len() + 4);
    let mut current_group = 0i64;
    let mut chars = pattern.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            result.push(c);
            if let Some(next) = chars.next() {
                result.push(next);
            }
        } else if c == '(' {
            let is_non_capturing = if chars.peek() == Some(&'?') {
                let remaining: String = chars.clone().take(3).collect();
                remaining.starts_with("?:")
                    || remaining.starts_with("?=")
                    || remaining.starts_with("?!")
                    || remaining.starts_with("?<!")
                    || remaining.starts_with("?<=")
            } else {
                false
            };

            if is_non_capturing {
                result.push(c);
            } else {
                current_group += 1;
                if current_group == group {
                    result.push_str(&format!("(?<{}>", output));
                    if chars.peek() == Some(&'?') {
                        chars.next();
                        if chars.peek() == Some(&'<') || chars.peek() == Some(&'P') {
                            if chars.peek() == Some(&'P') {
                                chars.next();
                            }
                            chars.next();
                            while let Some(&ch) = chars.peek() {
                                if ch == '>' {
                                    chars.next();
                                    break;
                                }
                                chars.next();
                            }
                        }
                    }
                } else {
                    result.push(c);
                }
            }
        } else {
            result.push(c);
        }
    }

    if current_group < group {
        return None;
    }

    Some(result)
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

impl SplunkConnector {
    pub(crate) fn transform_log(mut log: Log) -> Result<Log> {
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

        log.remove("_serial");
        log.remove("_bkt");
        log.remove("_cd");
        log.remove("_si");

        Ok(log)
    }

    pub(super) fn value_to_datetime(value: &mut Value) -> Result<()> {
        match value {
            Value::String(s) => {
                if let Ok(dt) =
                    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                {
                    *value = Value::from(dt);
                } else if let Ok(epoch) = s.parse::<f64>() {
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
        let handle = downcast_unwrap!(handle, SplunkHandle);
        let has_count = handle.has_count();
        let has_stats = handle.has_stats();

        let spl = handle.build_spl(collection);

        info!(?has_count, "Splunk search '{}': {}", collection, spl);

        let runner = QueryRunner::new(
            self.client.clone(),
            self.config.url.clone(),
            self.config.auth.clone(),
            self.config.job_poll_interval,
            self.config.job_timeout,
            self.config.result_batch_size,
        );

        if has_count && !has_stats {
            if self.config.enable_partial_stream {
                return Ok(QueryResponse::PartialLogs(
                    runner.run_count_with_previews(spl, self.config.preview_interval),
                ));
            }

            let (result_count, log) = runner.run_count(&spl).await?;
            let count = log
                .get(COUNT_FIELD_NAME)
                .and_then(|v| match v {
                    Value::UInt(n) => Some(*n),
                    Value::Int(n) => Some(*n as u64),
                    Value::String(s) => s.parse().ok(),
                    _ => None,
                })
                .unwrap_or(result_count);

            return Ok(QueryResponse::Count(count));
        }

        if has_stats {
            let timestamp_fields = handle
                .get_stats_timestamp_fields()
                .cloned()
                .unwrap_or_default();
            let numeric_fields = handle
                .get_stats_numeric_fields()
                .cloned()
                .unwrap_or_default();

            if self.config.enable_partial_stream {
                return Ok(QueryResponse::PartialLogs(runner.run_stats_with_previews(
                    spl,
                    self.config.preview_interval,
                    timestamp_fields,
                    numeric_fields,
                )));
            }

            return Ok(QueryResponse::Logs(
                runner
                    .run_stats(&spl, timestamp_fields, numeric_fields)
                    .await?,
            ));
        }

        if self.config.enable_partial_stream {
            let runner = QueryRunner::new(
                self.client.clone(),
                self.config.url.clone(),
                self.config.auth.clone(),
                self.config.job_poll_interval,
                self.config.job_timeout,
                self.config.result_batch_size,
            );
            return Ok(QueryResponse::PartialLogs(
                runner.run_with_previews(spl, self.config.preview_interval),
            ));
        }

        Ok(QueryResponse::Logs(runner.run(&spl).await?))
    }

    fn apply_filter(&self, ast: &Expr, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, SplunkHandle);

        let (time_range, remaining_expr) = ast.extract_timestamp_range(is_timestamp_field);

        let mut new_handle = handle.with_time_constraints(time_range.earliest, time_range.latest);

        if let Some(ref expr) = remaining_expr {
            let filter_result = compile_filter_to_spl(expr)?;
            for op in filter_result.into_spl_ops() {
                new_handle = new_handle.push(op);
            }
        }

        Some(Box::new(new_handle))
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

        // Skip sort by _time desc - Splunk returns results in this order by default.
        // Adding `| sort -_time` forces Splunk to process the entire dataset.
        let splunk_sorts: Vec<(String, SortOrder)> = sorts
            .iter()
            .filter(|s| {
                let is_desc = s.order == SortOrder::Desc;
                !(is_timestamp_field(&s.by) && is_desc)
            })
            .map(|s| (s.by.to_string(), s.order))
            .collect();

        let mut new_handle = handle.clone();
        if !splunk_sorts.is_empty() {
            new_handle = new_handle.push(SplunkOp::Sort(splunk_sorts));
        }
        new_handle = new_handle.push(SplunkOp::Head(max));

        Some(Box::new(new_handle))
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
                Aggregation::Countif(expr @ Expr::Exists(_)) => {
                    let agg_field = expr.as_pushable_exists_field()?;
                    numeric_agg_fields.insert(output_field.to_string());
                    format!("count(eval(isnotnull({}))) as {}", agg_field, output_field)
                }
                Aggregation::Countif(..) => return None,
            };
            agg_parts.push(agg_str);
        }

        let aggs = agg_parts.join(", ");

        let mut by_fields = Vec::new();
        let mut renames = Vec::new();
        for bf in &config.by {
            match &bf.expr {
                Expr::Field(field) => {
                    by_fields.push(field.to_string());
                    if *field != bf.name {
                        renames.push((field.to_string(), bf.name.to_string()));
                    }
                }
                // Splunk's binning syntax is different and more complex.
                // For now, don't push down summarize when binning is involved.
                Expr::Bin(..) => return None,
                _ => return None,
            }
        }

        let mut handle = handle.push(SplunkOp::Stats {
            aggs,
            by: by_fields,
            timestamp_agg_fields,
            numeric_agg_fields,
        });

        if !renames.is_empty() {
            handle = handle.push(SplunkOp::Rename(renames));
        }

        Some(Box::new(handle))
    }

    fn apply_extend(
        &self,
        projections: &[ProjectField],
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, SplunkHandle);
        let mut new_handle = handle.clone();

        for proj in projections {
            let Expr::Extract(regex_expr, group_expr, source_expr) = &proj.from else {
                return None;
            };

            let Expr::Literal(Value::String(pattern)) = regex_expr.as_ref() else {
                return None;
            };
            let Expr::Literal(group_val) = group_expr.as_ref() else {
                return None;
            };
            let group = group_val.as_i64()?;
            let Expr::Field(source_field) = source_expr.as_ref() else {
                return None;
            };

            let splunk_pattern =
                convert_to_splunk_named_capture(pattern, group, &proj.to.to_string())?;

            new_handle = new_handle.push(SplunkOp::Rex {
                field: source_field.to_string(),
                pattern: splunk_pattern,
                output_field: proj.to.to_string(),
            });
        }

        Some(Box::new(new_handle))
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

#[cfg(test)]
mod tests {
    use super::*;
    use miso_workflow_types::field::Field;

    fn field(name: &str) -> Field {
        name.parse().unwrap()
    }

    fn field_expr(name: &str) -> Box<Expr> {
        Box::new(Expr::Field(field(name)))
    }

    fn lit_str(s: &str) -> Box<Expr> {
        Box::new(Expr::Literal(Value::String(s.into())))
    }

    fn lit_int(v: i64) -> Box<Expr> {
        Box::new(Expr::Literal(Value::Int(v)))
    }

    fn eq_field(name: &str, val: Box<Expr>) -> Box<Expr> {
        Box::new(Expr::Eq(field_expr(name), val))
    }

    mod build_spl {
        use super::*;

        #[test]
        fn basic_index() {
            let handle = SplunkHandle::default();
            assert_eq!(handle.build_spl("myindex"), "search (index=\"myindex\")");
        }

        #[test]
        fn with_search_filter() {
            let handle = SplunkHandle::default().push(SplunkOp::Search("foo=CASE(\"bar\")".into()));
            assert_eq!(
                handle.build_spl("myindex"),
                "search (index=\"myindex\") | search foo=CASE(\"bar\")"
            );
        }

        #[test]
        fn with_where_filter() {
            let handle = SplunkHandle::default().push(SplunkOp::Where("isnotnull(foo)".into()));
            assert_eq!(
                handle.build_spl("myindex"),
                "search (index=\"myindex\") | where isnotnull(foo)"
            );
        }

        #[test]
        fn with_sort() {
            let handle = SplunkHandle::default().push(SplunkOp::Sort(vec![
                ("foo".into(), SortOrder::Desc),
                ("bar".into(), SortOrder::Asc),
            ]));
            assert_eq!(
                handle.build_spl("myindex"),
                "search (index=\"myindex\") | sort -foo, +bar"
            );
        }

        #[test]
        fn with_head() {
            let handle = SplunkHandle::default().push(SplunkOp::Head(100));
            assert_eq!(
                handle.build_spl("myindex"),
                "search (index=\"myindex\") | head 100"
            );
        }

        #[test]
        fn with_count() {
            let handle = SplunkHandle::default().push(SplunkOp::Count);
            assert_eq!(
                handle.build_spl("myindex"),
                format!("| tstats count as {COUNT_FIELD_NAME} where (index=\"myindex\")").as_str()
            );
        }

        #[test]
        fn with_count_and_filter() {
            let handle = SplunkHandle::default()
                .push(SplunkOp::Search("foo=CASE(\"bar\")".into()))
                .push(SplunkOp::Count);
            assert_eq!(
                handle.build_spl("myindex"),
                format!("search (index=\"myindex\") | search foo=CASE(\"bar\") | stats count as {COUNT_FIELD_NAME}").as_str()
            );
        }

        #[test]
        fn with_stats() {
            let handle = SplunkHandle::default().push(SplunkOp::Stats {
                aggs: "count as cnt, sum(value) as total".into(),
                by: vec!["category".into()],
                timestamp_agg_fields: HashSet::new(),
                numeric_agg_fields: HashSet::new(),
            });
            assert_eq!(
                handle.build_spl("myindex"),
                "search (index=\"myindex\") | stats count as cnt, sum(value) as total by category"
            );
        }

        #[test]
        fn with_earliest_latest() {
            let handle = SplunkHandle {
                earliest: Some(1000),
                latest: Some(2000),
                ..Default::default()
            };
            assert_eq!(
                handle.build_spl("myindex"),
                "search (index=\"myindex\") earliest=1000 latest=2000"
            );
        }

        #[test]
        fn with_union() {
            let handle = SplunkHandle::default().with_union("other_index");
            let spl = handle.build_spl("myindex");
            assert!(spl.contains("index=\"myindex\""));
            assert!(spl.contains("index=\"other_index\""));
            assert!(spl.contains(" OR "));
        }

        #[test]
        fn tstats_with_time_constraints() {
            let handle = SplunkHandle {
                earliest: Some(1000),
                latest: Some(2000),
                pipeline: vec![SplunkOp::Count],
                ..Default::default()
            };
            assert_eq!(
                handle.build_spl("myindex"),
                "| tstats count as Count where (index=\"myindex\") earliest=1000 latest=2000"
            );
        }

        #[test]
        fn batched_search_then_where() {
            let handle = SplunkHandle::default()
                .push(SplunkOp::Search(
                    r#"(status=CASE("error") AND user=CASE("admin"))"#.into(),
                ))
                .push(SplunkOp::Where("isnotnull(optional)".into()));
            assert_eq!(
                handle.build_spl("logs"),
                r#"search (index="logs") | search (status=CASE("error") AND user=CASE("admin")) | where isnotnull(optional)"#
            );
        }
    }

    mod compile_filter {
        use super::*;
        use test_case::test_case;

        #[test]
        fn eq_string() {
            let expr = Expr::Eq(field_expr("foo"), lit_str("bar"));
            let result = compile_filter_to_spl(&expr).unwrap();
            assert!(matches!(result, FilterResult::Search(_)));
            assert_eq!(result.as_str().unwrap(), "foo=CASE(\"bar\")");
        }

        #[test]
        fn eq_int() {
            let expr = Expr::Eq(field_expr("count"), lit_int(42));
            let result = compile_filter_to_spl(&expr).unwrap();
            assert!(matches!(result, FilterResult::Search(_)));
            assert_eq!(result.as_str().unwrap(), "count=42");
        }

        #[test]
        fn ne() {
            let expr = Expr::Ne(field_expr("status"), lit_str("error"));
            let result = compile_filter_to_spl(&expr).unwrap();
            assert_eq!(result.as_str().unwrap(), "status!=CASE(\"error\")");
        }

        #[test_case(Expr::Gt(field_expr("value"), lit_int(100)),  ">"  ; "gt")]
        #[test_case(Expr::Gte(field_expr("value"), lit_int(100)), ">=" ; "gte")]
        #[test_case(Expr::Lt(field_expr("value"), lit_int(100)),  "<"  ; "lt")]
        #[test_case(Expr::Lte(field_expr("value"), lit_int(100)), "<=" ; "lte")]
        fn comparison(expr: Expr, op: &str) {
            let result = compile_filter_to_spl(&expr).unwrap();
            assert_eq!(result.as_str().unwrap(), format!("value{op}100"));
        }

        #[test]
        fn logical_operators_use_search_when_possible() {
            let eq_a = eq_field("a", lit_int(1));
            let eq_b = eq_field("b", lit_int(2));

            let and_result = compile_filter_to_spl(&Expr::And(eq_a.clone(), eq_b.clone())).unwrap();
            assert!(matches!(and_result, FilterResult::Search(_)));
            assert_eq!(and_result.as_str().unwrap(), "(a=1 AND b=2)");

            let or_result = compile_filter_to_spl(&Expr::Or(eq_a, eq_b)).unwrap();
            assert!(matches!(or_result, FilterResult::Search(_)));
            assert_eq!(or_result.as_str().unwrap(), "(a=1 OR b=2)");
        }

        #[test]
        fn not() {
            let expr = Expr::Not(eq_field("a", lit_int(1)));
            let result = compile_filter_to_spl(&expr).unwrap();
            assert_eq!(result.as_str().unwrap(), "NOT a=1");
        }

        #[test]
        fn exists_uses_where() {
            let expr = Expr::Exists(field_expr("optional_field"));
            let result = compile_filter_to_spl(&expr).unwrap();
            assert!(matches!(result, FilterResult::Where(_)));
            assert_eq!(result.as_str().unwrap(), "isnotnull(optional_field)");
        }

        #[test]
        fn has_uses_where() {
            let expr = Expr::Has(field_expr("message"), lit_str("error"));
            let result = compile_filter_to_spl(&expr).unwrap();
            assert!(matches!(result, FilterResult::Where(_)));
            assert_eq!(
                result.as_str().unwrap(),
                "like(lower(message), \"%error%\")"
            );
        }

        #[test]
        fn and_with_mixed_search_where_batches_by_type() {
            let expr = Expr::And(
                eq_field("a", lit_int(1)),
                Box::new(Expr::Exists(field_expr("b"))),
            );
            let ops = compile_filter_to_spl(&expr).unwrap().into_spl_ops();
            assert_eq!(
                ops,
                vec![
                    SplunkOp::Search("a=1".to_string()),
                    SplunkOp::Where("isnotnull(b)".to_string()),
                ]
            );
        }

        #[test]
        fn deeply_nested_and_flattens_and_batches() {
            // a=1 AND b=2 AND exists(c) AND d=3
            let expr = Expr::And(
                Box::new(Expr::And(
                    eq_field("a", lit_int(1)),
                    eq_field("b", lit_int(2)),
                )),
                Box::new(Expr::And(
                    Box::new(Expr::Exists(field_expr("c"))),
                    eq_field("d", lit_int(3)),
                )),
            );
            let ops = compile_filter_to_spl(&expr).unwrap().into_spl_ops();
            assert_eq!(
                ops,
                vec![
                    SplunkOp::Search("(a=1 AND b=2 AND d=3)".to_string()),
                    SplunkOp::Where("isnotnull(c)".to_string()),
                ]
            );
        }

        #[test]
        fn and_all_where_combines_into_single_where() {
            let expr = Expr::And(
                Box::new(Expr::Exists(field_expr("a"))),
                Box::new(Expr::Exists(field_expr("b"))),
            );
            let result = compile_filter_to_spl(&expr).unwrap();
            assert!(matches!(result, FilterResult::Where(_)));
            assert_eq!(result.as_str().unwrap(), "(isnotnull(a) AND isnotnull(b))");
        }

        #[test]
        fn and_with_extract_and_search_batches() {
            let expr = Expr::And(
                Box::new(Expr::Eq(
                    Box::new(Expr::Extract(
                        lit_str("^(Calc)"),
                        lit_int(1),
                        field_expr("title"),
                    )),
                    lit_str("Calc"),
                )),
                eq_field("status", lit_str("active")),
            );
            let ops = compile_filter_to_spl(&expr).unwrap().into_spl_ops();
            assert_eq!(ops.len(), 4);
            assert_eq!(
                ops[0],
                SplunkOp::Search(r#"status=CASE("active")"#.to_string())
            );
            assert!(matches!(&ops[1], SplunkOp::Rex { .. }));
            assert!(matches!(&ops[2], SplunkOp::Where(_)));
            assert!(matches!(&ops[3], SplunkOp::FieldsRemove(_)));
        }

        #[test]
        fn and_all_complex_batches_ops_in_order() {
            // extract(regex, 1, title) == "Calc" AND extract(regex, 1, name) == "Test"
            let expr = Expr::And(
                Box::new(Expr::Eq(
                    Box::new(Expr::Extract(
                        lit_str("^(Calc)"),
                        lit_int(1),
                        field_expr("title"),
                    )),
                    lit_str("Calc"),
                )),
                Box::new(Expr::Eq(
                    Box::new(Expr::Extract(
                        lit_str("^(Test)"),
                        lit_int(1),
                        field_expr("name"),
                    )),
                    lit_str("Test"),
                )),
            );
            let ops = compile_filter_to_spl(&expr).unwrap().into_spl_ops();
            assert_eq!(ops.len(), 6);
            assert!(matches!(&ops[0], SplunkOp::Rex { field, .. } if field == "title"));
            assert!(matches!(&ops[1], SplunkOp::Where(_)));
            assert!(matches!(&ops[2], SplunkOp::FieldsRemove(_)));
            assert!(matches!(&ops[3], SplunkOp::Rex { field, .. } if field == "name"));
            assert!(matches!(&ops[4], SplunkOp::Where(_)));
            assert!(matches!(&ops[5], SplunkOp::FieldsRemove(_)));
        }

        #[test]
        fn or_with_mixed_types_uses_where() {
            let expr = Expr::Or(
                eq_field("a", lit_int(1)),
                Box::new(Expr::Exists(field_expr("b"))),
            );
            let result = compile_filter_to_spl(&expr).unwrap();
            assert!(matches!(result, FilterResult::Where(_)));
            assert_eq!(result.as_str().unwrap(), "(a=1 OR isnotnull(b))");
        }

        #[test]
        fn in_clause() {
            let expr = Expr::In(
                field_expr("status"),
                vec![
                    Expr::Literal(Value::String("a".into())),
                    Expr::Literal(Value::String("b".into())),
                ],
            );
            let result = compile_filter_to_spl(&expr).unwrap();
            assert_eq!(
                result.as_str().unwrap(),
                "(status=CASE(\"a\") OR status=CASE(\"b\"))"
            );
        }

        #[test]
        fn starts_with() {
            let expr = Expr::StartsWith(field_expr("path"), lit_str("/api/"));
            let result = compile_filter_to_spl(&expr).unwrap();
            assert_eq!(result.as_str().unwrap(), "path=/api/*");
        }
    }

    mod format_value {
        use super::*;
        use test_case::test_case;

        #[test]
        fn string_escaping() {
            assert_eq!(
                format_spl_value(&Value::String("hello \"world\"".into())),
                "\"hello \\\"world\\\"\""
            );
        }

        #[test]
        fn string_case_wrapper_for_search() {
            assert_eq!(
                format_spl_value_for_search(&Value::String("hello".into())),
                "CASE(\"hello\")"
            );
        }

        #[test_case(Value::Int(42), "42" ; "int")]
        #[test_case(Value::Float(2.5), "2.5" ; "float")]
        #[test_case(Value::Bool(true), "true" ; "bool_true")]
        #[test_case(Value::Bool(false), "false" ; "bool_false")]
        fn numeric_and_bool(input: Value, expected: &str) {
            assert_eq!(format_spl_value(&input), expected);
        }
    }

    mod is_timestamp {
        use super::*;
        use test_case::test_case;

        #[test_case("_time",      true  ; "underscore_time")]
        #[test_case("@time",      true  ; "at_time")]
        #[test_case("created_at", false ; "created_at")]
        #[test_case("timestamp",  false ; "timestamp")]
        fn recognizes_time_fields(name: &str, expected: bool) {
            assert_eq!(is_timestamp_field(&field(name)), expected);
        }
    }

    mod can_use_tstats {
        use super::*;

        #[test]
        fn only_for_simple_count() {
            assert!(
                SplunkHandle::default()
                    .push(SplunkOp::Count)
                    .can_use_tstats()
            );

            // Count with filter -> cannot use tstats
            assert!(
                !SplunkHandle::default()
                    .push(SplunkOp::Search("foo=bar".into()))
                    .push(SplunkOp::Count)
                    .can_use_tstats()
            );

            assert!(!SplunkHandle::default().can_use_tstats());

            // Stats (not count) -> cannot use tstats
            assert!(
                !SplunkHandle::default()
                    .push(SplunkOp::Stats {
                        aggs: "sum(x)".into(),
                        by: vec![],
                        timestamp_agg_fields: HashSet::new(),
                        numeric_agg_fields: HashSet::new(),
                    })
                    .can_use_tstats()
            );
        }
    }

    mod time_constraints {
        use super::*;

        #[test]
        fn merge_takes_more_restrictive_values() {
            let handle = SplunkHandle {
                earliest: Some(1000),
                latest: Some(3000),
                ..Default::default()
            };
            let updated = handle.with_time_constraints(Some(1500), Some(2500));
            assert_eq!(updated.earliest, Some(1500));
            assert_eq!(updated.latest, Some(2500));
        }

        #[test]
        fn sets_values_when_none() {
            let handle = SplunkHandle::default();
            let updated = handle.with_time_constraints(Some(1000), Some(2000));
            assert_eq!(updated.earliest, Some(1000));
            assert_eq!(updated.latest, Some(2000));
        }
    }

    mod convert_named_capture {
        use super::*;
        use test_case::test_case;

        #[test_case(r"error: (\d+)", 1, "code", r"error: (?<code>\d+)" ; "simple_capture_group")]
        #[test_case(r"(\w+): (\d+)", 2, "num", r"(\w+): (?<num>\d+)" ; "second_capture_group")]
        #[test_case(r"\(test\) (\d+)", 1, "num", r"\(test\) (?<num>\d+)" ; "preserves_escaped_parens")]
        #[test_case(r"(?:prefix)(\d+)", 1, "num", r"(?:prefix)(?<num>\d+)" ; "skips_non_capturing_groups")]
        #[test_case(r"(?<existing>\w+)(\d+)", 1, "word", r"(?<word>\w+)(\d+)" ; "named_group_first")]
        #[test_case(r"(?<existing>\w+)(\d+)", 2, "num", r"(?<existing>\w+)(?<num>\d+)" ; "named_group_second")]
        #[test_case(r"(?P<existing>\w+)(\d+)", 1, "word", r"(?<word>\w+)(\d+)" ; "python_style_named_group")]
        #[test_case(r"(?=prefix)(\d+)", 1, "num", r"(?=prefix)(?<num>\d+)" ; "skips_lookahead")]
        #[test_case(r"(?!bad)(\d+)", 1, "num", r"(?!bad)(?<num>\d+)" ; "skips_negative_lookahead")]
        #[test_case(r"(?<=pre)(\d+)", 1, "num", r"(?<=pre)(?<num>\d+)" ; "skips_lookbehind")]
        #[test_case(r"(?<!bad)(\d+)", 1, "num", r"(?<!bad)(?<num>\d+)" ; "skips_negative_lookbehind")]
        fn converts(pattern: &str, group: i64, name: &str, expected: &str) {
            assert_eq!(
                convert_to_splunk_named_capture(pattern, group, name),
                Some(expected.to_string())
            );
        }

        #[test_case(r"(\d+)", 0, "code" ; "group_zero")]
        #[test_case(r"(\d+)", 5, "code" ; "invalid_group_number")]
        fn returns_none(pattern: &str, group: i64, name: &str) {
            assert_eq!(convert_to_splunk_named_capture(pattern, group, name), None);
        }
    }

    mod rex_spl {
        use super::*;

        fn rex(field: &str, pattern: &str, output_field: &str) -> SplunkOp {
            SplunkOp::Rex {
                field: field.to_string(),
                pattern: pattern.to_string(),
                output_field: output_field.to_string(),
            }
        }

        #[test]
        fn basic_rex_command() {
            let handle =
                SplunkHandle::default().push(rex("message", r"error: (?<code>\d+)", "code"));
            assert_eq!(
                handle.build_spl("myindex"),
                r#"search (index="myindex") | rex field=message "error: (?<code>\d+)""#
            );
        }

        #[test]
        fn rex_with_other_ops() {
            let handle = SplunkHandle::default()
                .push(SplunkOp::Search("foo=bar".into()))
                .push(rex("msg", r"id=(?<id>\w+)", "id"))
                .push(SplunkOp::Head(100));
            assert_eq!(
                handle.build_spl("myindex"),
                r#"search (index="myindex") | search foo=bar | rex field=msg "id=(?<id>\w+)" | head 100"#
            );
        }

        #[test]
        fn rex_followed_by_filter() {
            let handle = SplunkHandle::default()
                .push(rex("msg", r"error: (?<code>\d+)", "code"))
                .push(SplunkOp::Search(r#"code=CASE("123")"#.into()));
            assert_eq!(
                handle.build_spl("myindex"),
                r#"search (index="myindex") | rex field=msg "error: (?<code>\d+)" | search code=CASE("123")"#
            );
        }

        #[test]
        fn rex_followed_by_where() {
            let handle = SplunkHandle::default()
                .push(rex("msg", r"status: (?<status>\w+)", "status"))
                .push(SplunkOp::Where(r#"status != "error""#.into()));
            assert_eq!(
                handle.build_spl("myindex"),
                r#"search (index="myindex") | rex field=msg "status: (?<status>\w+)" | where status != "error""#
            );
        }

        #[test]
        fn fields_remove() {
            let handle = SplunkHandle::default()
                .push(rex("msg", r"(?<_tmp>\w+)", "_tmp"))
                .push(SplunkOp::Where(r#"_tmp="foo""#.into()))
                .push(SplunkOp::FieldsRemove(vec!["_tmp".to_string()]));
            assert_eq!(
                handle.build_spl("myindex"),
                r#"search (index="myindex") | rex field=msg "(?<_tmp>\w+)" | where _tmp="foo" | fields - _tmp"#
            );
        }
    }

    mod extract_filter_vs_extend_filter {
        use super::*;

        #[test]
        fn filter_with_inlined_extract_creates_temporary_field_and_removes_it() {
            let filter_expr = Expr::Eq(
                Box::new(Expr::Extract(
                    lit_str("^(Calculate)"),
                    lit_int(1),
                    field_expr("title"),
                )),
                lit_str("Calculate"),
            );

            let ops = compile_filter_to_spl(&filter_expr)
                .expect("Should compile successfully")
                .into_spl_ops();

            assert_eq!(
                ops.len(),
                3,
                "Expected 3 operations: Rex, Where, FieldsRemove"
            );
            assert!(
                matches!(&ops[0], SplunkOp::Rex { field, pattern, output_field }
                    if field == "title"
                    && pattern == "^(?<_extract_title>Calculate)"
                    && output_field == "_extract_title"),
                "First op should be Rex with temporary field name"
            );
            assert!(
                matches!(&ops[1], SplunkOp::Where(w) if w == r#"_extract_title="Calculate""#),
                "Second op should be Where filtering on temporary field"
            );
            assert!(
                matches!(&ops[2], SplunkOp::FieldsRemove(fields) if fields == &vec!["_extract_title".to_string()]),
                "Third op should remove the temporary field"
            );
        }

        #[test]
        fn extend_then_filter_keeps_user_field() {
            let filter_expr = Expr::Eq(field_expr("calc"), lit_str("Calculate"));

            let ops = compile_filter_to_spl(&filter_expr)
                .expect("Should compile successfully")
                .into_spl_ops();

            assert_eq!(ops.len(), 1, "Expected 1 operation: Search");
            assert!(
                matches!(&ops[0], SplunkOp::Search(s) if s == r#"calc=CASE("Calculate")"#),
                "Should be a simple Search on the user's field, got: {:?}",
                ops[0]
            );
        }
    }
}
