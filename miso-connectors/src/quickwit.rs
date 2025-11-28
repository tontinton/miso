use std::{
    any::Any,
    fmt,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use async_stream::try_stream;
use axum::async_trait;
use bytes::BytesMut;
use color_eyre::eyre::{Context, OptionExt, Result, bail, eyre};
use futures_util::stream;
use hashbrown::{HashMap, HashSet};
use miso_common::{
    humantime_utils::{deserialize_duration, serialize_duration},
    metrics::METRICS,
    run_at_interval::run_at_interval,
    shutdown_future::ShutdownFuture,
};
use miso_workflow_types::{
    expr::Expr,
    field::Field,
    json,
    log::{Log, LogTryStream},
    project::ProjectField,
    sort::Sort,
    summarize::{Aggregation, Summarize},
    value::{Map, Value},
};
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use reqwest::{Client, RequestBuilder, Response};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use time::{OffsetDateTime, format_description};
use tracing::{debug, error, info, instrument};

use crate::Collection;

use super::{Connector, ConnectorError, QueryHandle, QueryResponse, Split, downcast_unwrap};

const AGGREGATION_RESULTS_NAME: &str = "summarize";

/// Used on aggregation queries where there's only count, and no other aggregations.
/// In that case we add another aggregation on this field name, just so we can get the
/// doc_count for each bucket.
const ONLY_COUNT_AGG_FIELD_NAME: &str = "_remove_me";

/// Quickwit doesn't yet support pagination over aggregation queries.
/// This will be the max amount of groups we pull from it (taken from quickwit's code).
const MAX_NUM_GROUPS: usize = 65000;

static DATETIME_FORMAT: Lazy<Vec<format_description::FormatItem<'static>>> = Lazy::new(|| {
    format_description::parse("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3]")
        .expect("quickwit datetime format to compile")
});

macro_rules! increment_and_ret_on_limit {
    ($counter:expr, $limit:expr) => {
        $counter += 1;
        if let Some(limit) = $limit {
            if $counter >= limit {
                return;
            }
        }
    };
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuickwitSplit {}

#[typetag::serde]
impl Split for QuickwitSplit {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct QuickwitHandle {
    timestamp_field: Option<String>,
    queries: Vec<Value>,
    source_includes: Vec<String>,
    sorts: Option<Value>,
    aggs: Option<Value>,
    group_by: Vec<String>,
    count_fields: Vec<String>,
    agg_timestamp_fields: HashSet<String>,
    limit: Option<u64>,
    count: bool,
    collections: Vec<String>,
}

#[typetag::serde]
impl QueryHandle for QuickwitHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl QuickwitHandle {
    fn new(timestamp_field: Option<String>) -> Self {
        Self {
            timestamp_field,
            ..Default::default()
        }
    }

    fn with_filter(&self, query: Value) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.queries.push(query);
        handle
    }

    fn with_source_includes(&self, fields: Vec<String>) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.source_includes = fields;
        handle
    }

    fn with_limit(&self, limit: u64) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.limit = Some(limit);
        handle
    }

    fn with_topn(&self, sort: Value, limit: u64) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.limit = Some(limit);
        handle.sorts = Some(sort);
        handle
    }

    fn with_count(&self) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.count = true;
        handle.source_includes.clear(); // Output fields are changed.
        handle
    }

    fn with_summarize(
        &self,
        aggs: Value,
        group_by: Vec<String>,
        count_fields: Vec<String>,
        agg_timestamp_fields: HashSet<String>,
    ) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.aggs = Some(aggs);
        handle.group_by = group_by;
        handle.count_fields = count_fields;
        handle.agg_timestamp_fields = agg_timestamp_fields;
        handle.source_includes.clear(); // Output fields are changed.
        handle
    }

    fn with_union(&self, collection: &str) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.collections.push(collection.to_string());
        handle
    }
}

impl fmt::Display for QuickwitHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut items = Vec::new();

        if self.count {
            items.push("count".to_string());
        }

        if !self.collections.is_empty() {
            let mut s = "unions=[".to_string();
            for (i, collection) in self.collections.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(collection);
            }
            s.push(']');
            items.push(s);
        }

        if !self.queries.is_empty() {
            let mut s = "filters=[".to_string();
            for (i, query) in self.queries.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(&format!("{query}"));
            }
            s.push(']');
            items.push(s);
        }

        if let Some(aggs) = &self.aggs {
            items.push(format!("aggs={aggs}"));
        }

        if !self.group_by.is_empty() {
            let mut s = "group_by=[".to_string();
            for (i, key) in self.group_by.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(key);
            }
            s.push(']');
            items.push(s);
        }

        if let Some(sorts) = &self.sorts {
            items.push(format!("sorts={sorts}"));
        }

        if let Some(limit) = self.limit {
            items.push(format!("limit={limit}"));
        }

        write!(f, "{}", items.join(", "))
    }
}

#[derive(Debug, Deserialize)]
struct DocMapping {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timestamp_field: Option<String>,
}

#[derive(Debug, Deserialize)]
struct IndexResponseConfig {
    index_id: String,
    doc_mapping: DocMapping,
}

#[derive(Debug, Deserialize)]
struct IndexResponse {
    index_config: IndexResponseConfig,
}

#[derive(Debug, Deserialize)]
struct SearchResponseHit {
    #[serde(rename = "_source")]
    source: Log,
}

#[derive(Debug, Deserialize)]
struct SearchResponseHits {
    hits: Vec<SearchResponseHit>,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    #[serde(rename = "_scroll_id")]
    scroll_id: String,
    hits: SearchResponseHits,
}

#[derive(Debug, Serialize)]
struct ContinueSearchRequest {
    scroll_id: String,
    scroll: String,
}

#[derive(Debug, Deserialize)]
struct CountResponse {
    count: u64,
}

#[derive(Debug, Deserialize)]
struct SearchAggregationBucket {
    doc_count: u64,
    key: Value,

    #[serde(rename = "key_as_string", default)]
    _key_as_string: Option<String>,

    #[serde(flatten)]
    buckets_or_value: HashMap<String, SearchAggregationBucketsOrValue>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum SearchAggregationBucketsOrValue {
    Value(SearchAggregationValue),
    Buckets(SearchAggregationBuckets),
}

#[derive(Debug, Deserialize)]
struct SearchAggregationValue {
    value: Value,
}

#[derive(Debug, Deserialize)]
struct SearchAggregationBuckets {
    buckets: Vec<SearchAggregationBucket>,
}

#[derive(Debug, Deserialize)]
struct SearchAggregationHitsTotal {
    value: u64,
}

#[derive(Debug, Deserialize)]
struct SearchAggregationHits {
    total: SearchAggregationHitsTotal,
}

#[derive(Debug, Deserialize)]
struct SearchAggregationResponse {
    hits: SearchAggregationHits,
    aggregations: HashMap<String, SearchAggregationBucketsOrValue>,
}

fn default_refresh_interval() -> Duration {
    humantime::parse_duration("1m").expect("Invalid duration format")
}

fn default_scroll_timeout() -> Duration {
    humantime::parse_duration("1m").expect("Invalid duration format")
}

fn default_scroll_size() -> u16 {
    10000
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct QuickwitConfig {
    url: String,

    #[serde(
        default = "default_refresh_interval",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    refresh_interval: Duration,

    #[serde(
        default = "default_scroll_timeout",
        serialize_with = "serialize_duration",
        deserialize_with = "deserialize_duration"
    )]
    scroll_timeout: Duration,

    #[serde(default = "default_scroll_size")]
    scroll_size: u16,
}

impl QuickwitConfig {
    pub fn new_with_interval(url: String, refresh_interval: Duration) -> Self {
        Self {
            url,
            refresh_interval,
            scroll_timeout: default_scroll_timeout(),
            scroll_size: default_scroll_size(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct QuickwitIndex {
    timestamp_field: Option<String>,
}

type QuickwitIndexes = HashMap<String, QuickwitIndex>;

#[derive(Debug)]
pub struct QuickwitConnector {
    config: QuickwitConfig,
    indexes: Arc<RwLock<QuickwitIndexes>>,
    interval_task: ShutdownFuture,
    client: Client,
}

impl Serialize for QuickwitConnector {
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

impl<'de> Deserialize<'de> for QuickwitConnector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        QuickwitConfig::deserialize(deserializer).map(QuickwitConnector::new)
    }
}

fn format_datetime(dt: &OffsetDateTime) -> String {
    dt.format(&DATETIME_FORMAT)
        .expect("format quickwit datetime from an already parsed miso datetime")
}

fn format_value(value: &Value) -> String {
    if let Value::Timestamp(dt) = value {
        format_datetime(dt)
    } else {
        value.to_string()
    }
}

fn compile_filter_ast(expr: &Expr) -> Option<Value> {
    Some(match expr {
        Expr::Or(left, right) => {
            json!({
                "bool": {
                    "should": vec![left, right]
                        .into_iter()
                        .map(|x| compile_filter_ast(x))
                        .collect::<Option<Vec<_>>>()?,
                }
            })
        }
        Expr::And(left, right) => {
            json!({
                "bool": {
                    "must": vec![left, right]
                        .into_iter()
                        .map(|x| compile_filter_ast(x))
                        .collect::<Option<Vec<_>>>()?,
                }
            })
        }
        Expr::Not(expr) => {
            json!({
                "bool": {
                    "must_not": compile_filter_ast(expr)?,
                }
            })
        }
        Expr::Exists(field) if !field.has_array_access() => {
            json!({
                "exists": {
                    "field": field,
                }
            })
        }
        Expr::In(lhs, rhs) => {
            let Expr::Field(field) = &**lhs else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }

            let values = rhs
                .iter()
                .map(|x| match x {
                    Expr::Literal(value) => Some(format_value(value)),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;

            json!({
                "terms": {
                    field: values
                }
            })
        }
        Expr::StartsWith(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(prefix)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            json!({
                "match_phrase_prefix": {
                    field: {
                        "query": format_value(prefix),
                    }
                }
            })
        }
        Expr::HasCs(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(phrase)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            json!({
                "match_phrase": {
                    field: format_value(phrase),
                }
            })
        }
        Expr::Eq(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            json!({
                "term": {
                    field: {
                        "value": format_value(value),
                    }
                }
            })
        }
        Expr::Ne(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            json!({
                "bool": {
                    "must_not": {
                        "term": {
                            field: format_value(value),
                        }
                    }
                }
            })
        }
        Expr::Gt(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            json!({
                "range": {
                    field: {
                        "gt": format_value(value),
                    }
                }
            })
        }
        Expr::Gte(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            json!({
                "range": {
                    field: {
                        "gte": format_value(value),
                    }
                }
            })
        }
        Expr::Lt(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            json!({
                "range": {
                    field: {
                        "lt": format_value(value),
                    }
                }
            })
        }
        Expr::Lte(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            json!({
                "range": {
                    field: {
                        "lte": format_value(value),
                    }
                }
            })
        }
        _ => return None,
    })
}

async fn response_to_bytes(response: Response) -> Result<BytesMut> {
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        bail!(ConnectorError::ServerResp(status.as_u16(), text));
    }
    let bytes = response.bytes().await.context("bytes from response")?;
    METRICS.downloaded_bytes.inc_by(bytes.len() as u64);
    Ok(bytes.into())
}

async fn send_request(req: RequestBuilder) -> Result<BytesMut> {
    match req.send().await {
        Ok(response) => response_to_bytes(response).await,
        Err(e) => Err(eyre!(ConnectorError::Http(e))),
    }
}

#[instrument(skip(query), name = "GET and parse quickwit begin search results")]
async fn begin_search(
    client: &Client,
    base_url: &str,
    index: &str,
    query: Option<Value>,
    source_includes: Vec<String>,
    scroll_timeout: &Duration,
    scroll_size: u16,
) -> Result<(Vec<Log>, String)> {
    let source_includes = if source_includes.is_empty() {
        "".to_string()
    } else {
        format!("_source_includes={}&", source_includes.join(","))
    };

    let url = format!(
        "{}/api/v1/_elastic/{}/_search?{}scroll={}ms&size={}",
        base_url,
        index,
        source_includes,
        scroll_timeout.as_millis(),
        scroll_size,
    );

    let mut req = client.get(&url);
    if let Some(query) = query {
        req = req.json(&query);
    }

    let mut bytes = send_request(req).await?;
    let data: SearchResponse =
        simd_json::serde::from_slice(bytes.as_mut()).context("parse response")?;
    Ok((
        data.hits.hits.into_iter().map(|x| x.source).collect(),
        data.scroll_id,
    ))
}

#[instrument(name = "GET and parse quickwit continue search results")]
async fn continue_search(
    client: &Client,
    base_url: &str,
    scroll_id: String,
    scroll_timeout: &Duration,
) -> Result<(Vec<Log>, String)> {
    let url = format!("{base_url}/api/v1/_elastic/_search/scroll");

    let req = client.get(&url).json(&ContinueSearchRequest {
        scroll_id,
        scroll: format!("{}ms", scroll_timeout.as_millis()),
    });

    let mut bytes = send_request(req).await?;
    let data: SearchResponse =
        simd_json::serde::from_slice(bytes.as_mut()).context("parse response")?;
    Ok((
        data.hits.hits.into_iter().map(|x| x.source).collect(),
        data.scroll_id,
    ))
}

#[instrument(skip(query), name = "GET and parse quickwit count result")]
async fn count(client: &Client, base_url: &str, index: &str, query: Option<Value>) -> Result<u64> {
    let url = format!("{base_url}/api/v1/_elastic/{index}/_count");

    let mut req = client.get(&url);
    if let Some(query) = query {
        req = req.json(&query);
    }

    let mut bytes = send_request(req).await?;
    let data: CountResponse =
        simd_json::serde::from_slice(bytes.as_mut()).context("parse response")?;
    Ok(data.count)
}

#[instrument(
    skip(query),
    name = "GET and parse quickwit search aggregation results"
)]
async fn search_aggregation(
    client: &Client,
    base_url: &str,
    index: &str,
    query: Option<Value>,
) -> Result<SearchAggregationResponse> {
    let url = format!("{base_url}/api/v1/_elastic/{index}/_search");

    let mut req = client.get(&url);
    if let Some(query) = query {
        req = req.json(&query);
    }

    let mut bytes = send_request(req).await?;
    simd_json::serde::from_slice(bytes.as_mut()).context("parse response")
}

#[instrument(name = "GET and parse quickwit indexes")]
async fn get_indexes(client: &Client, base_url: &str) -> Result<QuickwitIndexes> {
    let url = format!("{base_url}/api/v1/indexes");
    let mut bytes = send_request(client.get(&url)).await?;
    let data: Vec<IndexResponse> =
        simd_json::serde::from_slice(bytes.as_mut()).context("parse response")?;
    Ok(data
        .into_iter()
        .map(|x| {
            (
                x.index_config.index_id,
                QuickwitIndex {
                    timestamp_field: x.index_config.doc_mapping.timestamp_field,
                },
            )
        })
        .collect())
}

async fn refresh_indexes_at_interval(
    config: QuickwitConfig,
    weak_indexes: Weak<RwLock<QuickwitIndexes>>,
) {
    let client = Client::new();

    run_at_interval(
        async || {
            let Some(indexes) = weak_indexes.upgrade() else {
                return false;
            };

            match get_indexes(&client, &config.url).await {
                Ok(response) => {
                    debug!("Got indexes: {:?}", &response);
                    let mut guard = indexes.write();
                    *guard = response;
                }
                Err(e) => {
                    error!("Failed to get quickwit indexes: {:?}", e);
                }
            }

            true
        },
        config.refresh_interval,
    )
    .await;
}

impl QuickwitConnector {
    pub fn new(config: QuickwitConfig) -> QuickwitConnector {
        let indexes = Arc::new(RwLock::new(HashMap::new()));
        let interval_task = ShutdownFuture::new(
            refresh_indexes_at_interval(config.clone(), Arc::downgrade(&indexes)),
            "Quickwit indexes refresher",
        );

        Self {
            config,
            indexes,
            interval_task,
            client: Client::new(),
        }
    }

    fn transform_log(mut log: Log, timestamp_field: &Option<String>) -> Result<Log> {
        if let Some(timestamp_field) = timestamp_field
            && let Some(value) = log.get_mut(timestamp_field)
            && let Value::String(s) = value
        {
            let dt = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                .context("parse quickwit datetime string response")?;
            *value = Value::from(dt)
        }
        Ok(log)
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_search(
        client: Client,
        url: String,
        index: String,
        query: Option<Value>,
        scroll_timeout: Duration,
        scroll_size: u16,
        limit: Option<u64>,
        timestamp_field: Option<String>,
        source_includes: Vec<String>,
    ) -> Result<LogTryStream> {
        let start = Instant::now();

        let (mut logs, mut scroll_id) = begin_search(
            &client,
            &url,
            &index,
            query,
            source_includes,
            &scroll_timeout,
            scroll_size,
        )
        .await?;

        let duration = start.elapsed();
        debug!(elapsed_time = ?duration, "Begin search time");

        if logs.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        Ok(Box::pin(try_stream! {
            let mut streamed = 0;

            for log in logs {
                yield Self::transform_log(log, &timestamp_field)?;
                increment_and_ret_on_limit!(streamed, limit);
            }

            loop {
                (logs, scroll_id) = continue_search(&client, &url, scroll_id, &scroll_timeout).await?;
                if logs.is_empty() {
                    return;
                }
                for log in logs {
                    yield Self::transform_log(log, &timestamp_field)?;
                    increment_and_ret_on_limit!(streamed, limit);
                }
            }
        }))
    }

    fn group_by_value_to_datetime(value: &mut Value) -> Result<()> {
        match value {
            Value::String(s) => {
                let dt = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .context("parse quickwit datetime string response")?;
                *value = Value::from(dt)
            }
            Value::Float(f) => {
                let dt = OffsetDateTime::from_unix_timestamp_nanos(*f as i128 * 1_000_000)
                    .context("parse quickwit datetime float response")?;
                *value = Value::from(dt)
            }
            _ => {}
        }
        Ok(())
    }

    fn agg_value_to_datetime(value: &mut Value) -> Result<()> {
        if let Value::Float(f) = value {
            let dt = OffsetDateTime::from_unix_timestamp_nanos(*f as i128)
                .context("parse quickwit datetime float response")?;
            *value = Value::from(dt)
        }
        Ok(())
    }

    fn transform_group_by_timestamp_value(
        key: &str,
        mut value: Value,
        timestamp_field: &Option<String>,
    ) -> Result<Value> {
        if timestamp_field.as_deref() == Some(key) {
            Self::group_by_value_to_datetime(&mut value)?;
        }
        Ok(value)
    }

    fn transform_agg_timestamp_value(
        key: &str,
        mut value: Value,
        agg_timestamp_fields: &HashSet<String>,
    ) -> Result<Value> {
        if agg_timestamp_fields.contains(key) {
            Self::agg_value_to_datetime(&mut value)?;
        }
        Ok(value)
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_last_bucket(
        buckets_or_value_wrap: HashMap<String, SearchAggregationBucketsOrValue>,
        doc_count: u64,
        group_by: &[String],
        count_fields: &[String],
        timestamp_field: &Option<String>,
        agg_timestamp_fields: &HashSet<String>,
        keys_stack: &[Value],
        logs: &mut Vec<Log>,
    ) -> Result<()> {
        if doc_count == 0 {
            // Quickwit fills in groups when running date_histogram aggregation,
            // we need to filter them out.
            return Ok(());
        }

        let mut log = Log::new();

        for (key, value) in group_by.iter().zip(keys_stack.iter()) {
            log.insert(
                key.clone(),
                Self::transform_group_by_timestamp_value(key, value.clone(), timestamp_field)
                    .context("aggregation transform result")?,
            );
        }
        for key in count_fields {
            log.insert(key.clone(), Value::from(doc_count));
        }

        for (field, buckets_or_value) in buckets_or_value_wrap {
            let SearchAggregationBucketsOrValue::Value(value_wrap) = buckets_or_value else {
                bail!("expected value, not bucket");
            };
            let value =
                Self::transform_agg_timestamp_value(&field, value_wrap.value, agg_timestamp_fields)
                    .context("aggregation transform result")?;
            log.insert(field, value);
        }

        logs.push(log);

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_buckets(
        buckets_or_value: SearchAggregationBucketsOrValue,
        index: usize,
        group_by: &[String],
        count_fields: &[String],
        timestamp_field: &Option<String>,
        agg_timestamp_fields: &HashSet<String>,
        keys_stack: &mut Vec<Value>,
        logs: &mut Vec<Log>,
    ) -> Result<()> {
        let SearchAggregationBucketsOrValue::Buckets(buckets_wrap) = buckets_or_value else {
            bail!("expected buckets, not value");
        };

        for mut bucket in buckets_wrap.buckets {
            keys_stack.push(bucket.key);

            if keys_stack.len() == group_by.len() {
                Self::parse_last_bucket(
                    bucket.buckets_or_value,
                    bucket.doc_count,
                    group_by,
                    count_fields,
                    timestamp_field,
                    agg_timestamp_fields,
                    keys_stack,
                    logs,
                )?;
            } else {
                let bucket_name = format!("{AGGREGATION_RESULTS_NAME}_{index}");
                let Some(next_buckets_or_value) = bucket.buckets_or_value.remove(&bucket_name)
                else {
                    bail!("bucket '{bucket_name}' not found");
                };

                Self::parse_buckets(
                    next_buckets_or_value,
                    index + 1,
                    group_by,
                    count_fields,
                    timestamp_field,
                    agg_timestamp_fields,
                    keys_stack,
                    logs,
                )?;
            }

            keys_stack.pop();
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn query_aggregation(
        client: Client,
        url: String,
        index: String,
        query: Option<Value>,
        limit: Option<u64>,
        group_by: Vec<String>,
        count_fields: Vec<String>,
        agg_timestamp_fields: HashSet<String>,
        timestamp_field: Option<String>,
    ) -> Result<LogTryStream> {
        let mut response = search_aggregation(&client, &url, &index, query)
            .await
            .context("run quickwit aggregation query")?;

        let mut logs = Vec::new();

        let first_bucket_name = format!("{AGGREGATION_RESULTS_NAME}_0");
        if let Some(buckets_or_value) = response.aggregations.remove(&first_bucket_name) {
            Self::parse_buckets(
                buckets_or_value,
                1,
                &group_by,
                &count_fields,
                &timestamp_field,
                &agg_timestamp_fields,
                &mut Vec::new(),
                &mut logs,
            )
            .context("parse quickwit aggregation response (group by)")?;
        } else {
            Self::parse_last_bucket(
                response.aggregations,
                response.hits.total.value,
                &[],
                &count_fields,
                &timestamp_field,
                &agg_timestamp_fields,
                &[],
                &mut logs,
            )
            .context("parse quickwit aggregation response (no group by)")?;
        }

        Ok(Box::pin(try_stream! {
            if logs.is_empty() {
                return;
            }

            let mut streamed = 0;
            for mut log in logs {
                log.remove(ONLY_COUNT_AGG_FIELD_NAME);
                yield log;
                increment_and_ret_on_limit!(streamed, limit);
            }
        }))
    }
}

#[async_trait]
#[typetag::serde(name = "quickwit")]
impl Connector for QuickwitConnector {
    #[instrument(skip(self), name = "Quickwit get collection")]
    fn get_collection(&self, collection: &str) -> Option<Collection> {
        let guard = self.indexes.read();
        let index = guard.get(collection)?;

        let mut static_fields = HashMap::new();
        if let Some(timestamp_field) = &index.timestamp_field {
            static_fields.insert("@time".to_string(), timestamp_field.clone());
        }

        Some(Collection { static_fields })
    }

    fn get_splits(&self) -> Vec<Box<dyn Split>> {
        vec![Box::new(QuickwitSplit {}) as Box<dyn Split>]
    }

    fn get_handle(&self, collection: &str) -> Result<Box<dyn QueryHandle>> {
        let timestamp_field = self
            .indexes
            .read()
            .get(collection)
            .ok_or_eyre("collection not found")?
            .timestamp_field
            .clone();
        Ok(Box::new(QuickwitHandle::new(timestamp_field)))
    }

    #[instrument(skip(self), name = "Quickwit query")]
    async fn query(
        &self,
        collection: &str,
        handle: &dyn QueryHandle,
        split: Option<&dyn Split>,
    ) -> Result<QueryResponse> {
        if let Some(split) = split {
            let Some(_) = split.as_any().downcast_ref::<QuickwitSplit>() else {
                bail!("Downcasting split to wrong struct?");
            };
        }

        let url = self.config.url.clone();
        let scroll_timeout = self.config.scroll_timeout;

        let handle = downcast_unwrap!(handle, QuickwitHandle);
        let limit = handle.limit;
        let source_includes = handle.source_includes.clone();
        let scroll_size = limit.map_or(self.config.scroll_size, |l| {
            l.min(self.config.scroll_size as u64) as u16
        });
        let timestamp_field = handle.timestamp_field.clone();

        let mut collections = Vec::with_capacity(1 + handle.collections.len());
        collections.push(collection);
        collections.extend(handle.collections.iter().map(String::as_str));
        collections.sort();
        collections.dedup();
        let collections = collections.join(",");

        let mut query_map = Map::new();

        if !handle.queries.is_empty() {
            query_map.insert(
                "query",
                json!({
                    "bool": {
                        "must": handle.queries.clone(),
                    }
                }),
            );
        }

        if let Some(sorts) = &handle.sorts {
            query_map.insert("sort", sorts.clone());
        }

        let is_aggregation_query = if let Some(aggs) = &handle.aggs {
            query_map.insert("size", json!(0));
            for (key, value) in aggs.as_object().unwrap() {
                query_map.insert(key, value.clone());
            }
            true
        } else {
            if let Some(limit) = limit {
                query_map.insert("size", limit.into());
            }
            false
        };

        let query = if !query_map.is_empty() {
            Some(json!(query_map))
        } else {
            None
        };

        info!(
            ?handle.count,
            ?scroll_size,
            ?limit,
            ?source_includes,
            "Quickwit search '{}': {}",
            collections,
            serde_json::to_string(&query)?
        );

        if handle.count {
            let mut result = count(&self.client, &url, &collections, query).await?;
            if let Some(limit) = limit {
                result = limit.min(result);
            }
            return Ok(QueryResponse::Count(result));
        }

        if let Some(limit) = limit
            && limit == 0
        {
            return Ok(QueryResponse::Logs(Box::pin(stream::empty())));
        }

        if is_aggregation_query {
            return Ok(QueryResponse::Logs(
                Self::query_aggregation(
                    self.client.clone(),
                    url,
                    collections,
                    query,
                    limit,
                    handle.group_by.clone(),
                    handle.count_fields.clone(),
                    handle.agg_timestamp_fields.clone(),
                    timestamp_field,
                )
                .await?,
            ));
        }

        Ok(QueryResponse::Logs(
            Self::query_search(
                self.client.clone(),
                url,
                collections,
                query,
                scroll_timeout,
                scroll_size,
                limit,
                timestamp_field,
                source_includes,
            )
            .await?,
        ))
    }

    fn apply_filter(&self, ast: &Expr, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if handle.sorts.is_some() || !handle.group_by.is_empty() {
            // Cannot filter over top-n / group by in Quickwit.
            return None;
        }
        Some(Box::new(handle.with_filter(compile_filter_ast(ast)?)))
    }

    fn apply_project(
        &self,
        projections: &[ProjectField],
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if handle.count || !handle.group_by.is_empty() {
            return None;
        }

        let mut source_includes = Vec::with_capacity(projections.len());
        for project in projections {
            let Expr::Field(field) = &project.from else {
                return None;
            };
            if field != &project.to {
                return None;
            }
            source_includes.push(field.into());
        }
        Some(Box::new(handle.with_source_includes(source_includes)))
    }

    fn apply_limit(&self, mut max: u64, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if let Some(limit) = handle.limit
            && limit < max
        {
            max = limit;
        }
        Some(Box::new(handle.with_limit(max)))
    }

    fn apply_topn(
        &self,
        sorts: &[Sort],
        mut max: u64,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if handle.sorts.is_some() {
            // Cannot top-n over top-n in Quickwit.
            return None;
        }

        if !handle.group_by.is_empty() {
            // Maybe this can actually be implemented, need to check in the future.
            // https://quickwit.io/docs/reference/aggregation#order,
            // https://quickwit.io/docs/reference/aggregation#size.
            return None;
        }

        if let Some(limit) = handle.limit
            && limit < max
        {
            max = limit;
        }

        let sorts = Value::Array(
            sorts
                .iter()
                .map(|sort| {
                    json!({
                        &sort.by: {
                            "order": sort.order.to_string(),
                            "nulls": sort.nulls.to_string(),
                        }
                    })
                })
                .collect(),
        );
        Some(Box::new(handle.with_topn(sorts, max)))
    }

    fn apply_count(&self, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if !handle.group_by.is_empty() {
            // Quickwit count query returns number of items instead of number of unique groups.
            // This is fine, as usually aggregation requests return few results, we can count
            // them ourselves.
            return None;
        }
        Some(Box::new(handle.with_count()))
    }

    fn apply_summarize(
        &self,
        config: &Summarize,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if handle.limit.is_some() || handle.sorts.is_some() || !handle.group_by.is_empty() {
            // Quickwit's query (like Elasticsearch's) is not pipelined, most similar to SQL.
            // When you request it to both sort (or limit) and aggregate, it will always first
            // aggregate and then sort (or limit), no way to control the order of these 2 AFAIK.
            // So we do the aggregation in-process instead of pushing it down to Quickwit.
            return None;
        }

        let mut count_fields = Vec::new();
        let mut agg_timestamp_fields = HashSet::new();
        let mut inner_aggs = Map::new();

        fn agg_json(
            op: &str,
            agg_field: &Field,
            timestamp_field: &Option<String>,
        ) -> (Value, bool) {
            (
                json!({ op: { "field": agg_field } }),
                timestamp_field == &Some(agg_field.to_string()),
            )
        }

        for (output_field, agg) in &config.aggs {
            let (value, is_timestamp) = match agg {
                Aggregation::Min(agg_field) => agg_json("min", agg_field, &handle.timestamp_field),
                Aggregation::Max(agg_field) => agg_json("max", agg_field, &handle.timestamp_field),
                Aggregation::Sum(agg_field) => agg_json("sum", agg_field, &handle.timestamp_field),
                Aggregation::Avg(agg_field) => agg_json("avg", agg_field, &handle.timestamp_field),
                Aggregation::Countif(Expr::Exists(agg_field)) => {
                    agg_json("value_count", agg_field, &handle.timestamp_field)
                }
                Aggregation::Count => {
                    // Count is always returned in doc_count.
                    count_fields.push(output_field.to_string());
                    continue;
                }
                Aggregation::DCount(agg_field) => {
                    agg_json("cardinality", agg_field, &handle.timestamp_field)
                }

                // Currently unsupported.
                Aggregation::Countif(..) => return None,
            };

            if is_timestamp {
                agg_timestamp_fields.insert(output_field.to_string());
            }
            inner_aggs.insert(output_field.to_string(), value);
        }

        if !count_fields.is_empty() && inner_aggs.is_empty() {
            inner_aggs.insert(
                ONLY_COUNT_AGG_FIELD_NAME.to_string(),
                json!({"max": {"field": "a"}}),
            );
        }

        let mut aggs = Map::new();
        let mut current_agg = &mut aggs;

        for (i, expr) in config.by.iter().enumerate() {
            let name = format!("{AGGREGATION_RESULTS_NAME}_{i}");

            let bucket_def = match expr {
                Expr::Field(field) => json!({
                    "terms": {
                        "field": field,
                        "size": MAX_NUM_GROUPS,
                    }
                }),
                Expr::Bin(lhs, rhs) => {
                    let Expr::Field(field) = &**lhs else {
                        return None;
                    };
                    let Expr::Literal(value) = &**rhs else {
                        return None;
                    };
                    match value {
                        Value::Timespan(dur) => json!({
                            "date_histogram": {
                                "field": field,
                                "fixed_interval": *dur,
                            }
                        }),
                        _ => json!({
                            "histogram": {
                                "field": field,
                                "interval": value.clone(),
                            }
                        }),
                    }
                }
                _ => return None,
            };

            current_agg.insert(name.clone(), bucket_def);

            if let Some(Value::Object(bucket)) = current_agg.get_mut(&name) {
                bucket.insert("aggs".to_string(), json!({}));
                if let Some(Value::Object(next)) = bucket.get_mut("aggs") {
                    current_agg = next;
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }

        if !inner_aggs.is_empty() {
            for (k, v) in inner_aggs {
                current_agg.insert(k, v);
            }
        }

        let group_by = config
            .by
            .iter()
            .map(|expr| match expr {
                Expr::Field(field) => Some(field.to_string()),
                Expr::Bin(lhs, _) => match &**lhs {
                    Expr::Field(field) => Some(field.to_string()),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;

        Some(Box::new(handle.with_summarize(
            json!({"aggs": aggs}),
            group_by,
            count_fields,
            agg_timestamp_fields,
        )))
    }

    fn apply_union(
        &self,
        scan_collection: &str,
        union_collection: &str,
        handle: &dyn QueryHandle,
        union_handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        let union_handle = downcast_unwrap!(union_handle, QuickwitHandle);

        if handle != union_handle {
            // Quickwit only supports querying multiple indexes with the exact same query.
            return None;
        }

        let can_union = match (
            self.indexes.read().get(scan_collection),
            self.indexes.read().get(union_collection),
        ) {
            (Some(l), Some(r)) => l.timestamp_field == r.timestamp_field,
            _ => false,
        };

        if !can_union {
            // Quickwit only supports multi index search when the timestamp fields are the same.
            return None;
        }

        Some(Box::new(handle.with_union(union_collection)))
    }

    #[instrument(skip(self), name = "Quickwit close")]
    async fn close(&self) {
        self.interval_task.shutdown().await;
    }
}
