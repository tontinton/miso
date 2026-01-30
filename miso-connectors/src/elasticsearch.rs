use std::{
    any::Any,
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};

use async_stream::try_stream;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::BytesMut;
use color_eyre::eyre::{Context, OptionExt, Result, bail, eyre};
use futures_util::stream;
use hashbrown::{HashMap, HashSet};
use miso_common::{
    humantime_utils::{deserialize_duration, serialize_duration},
    metrics::{
        CONNECTOR_ELASTICSEARCH, METRICS, OP_BEGIN_SEARCH, OP_CONTINUE_SCROLL, OP_GET_INDEXES,
    },
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
use parking_lot::RwLock;
use reqwest::{Client, RequestBuilder, Response, header};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use time::OffsetDateTime;
use tracing::{debug, error, info, instrument};

use crate::{Collection, instrumentation::record_operation_result};

use super::{Connector, ConnectorError, QueryHandle, QueryResponse, Split, downcast_unwrap};

const AGGREGATION_RESULTS_NAME: &str = "summarize";
const ONLY_COUNT_AGG_FIELD_NAME: &str = "_remove_me";
const MAX_NUM_GROUPS: usize = 65000;

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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[derive(Default)]
pub enum ElasticsearchAuth {
    #[default]
    None,
    Basic {
        username: String,
        password: String,
    },
    ApiKey {
        key: String,
    },
}

impl ElasticsearchAuth {
    fn apply_to_request(&self, req: RequestBuilder) -> RequestBuilder {
        match self {
            ElasticsearchAuth::None => req,
            ElasticsearchAuth::Basic { username, password } => {
                let credentials = format!("{username}:{password}");
                let encoded = BASE64.encode(credentials);
                req.header(header::AUTHORIZATION, format!("Basic {encoded}"))
            }
            ElasticsearchAuth::ApiKey { key } => {
                req.header(header::AUTHORIZATION, format!("ApiKey {key}"))
            }
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchConfig {
    url: String,

    #[serde(default)]
    auth: ElasticsearchAuth,

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

impl ElasticsearchConfig {
    pub fn new_with_interval(url: String, refresh_interval: Duration) -> Self {
        Self {
            url,
            auth: ElasticsearchAuth::None,
            refresh_interval,
            scroll_timeout: default_scroll_timeout(),
            scroll_size: default_scroll_size(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct ElasticsearchHandle {
    timestamp_field: Option<String>,
    query: Vec<Value>,
    _source: Option<Vec<String>>,
    size: Option<u64>,
    sort: Option<Value>,
    aggs: Option<Value>,
    group_by: Vec<String>,
    count_fields: Vec<String>,
    agg_timestamp_fields: HashSet<String>,
    count: bool,
    collections: Vec<String>,
}

#[typetag::serde]
impl QueryHandle for ElasticsearchHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ElasticsearchHandle {
    fn new(timestamp_field: Option<String>) -> Self {
        Self {
            timestamp_field,
            ..Default::default()
        }
    }

    fn with_filter(&self, filter: Value) -> Self {
        let mut handle = self.clone();
        handle.query.push(filter);
        handle
    }

    fn with_project(&self, fields: Vec<String>) -> Self {
        let mut handle = self.clone();
        handle._source = Some(fields);
        handle
    }

    fn with_limit(&self, limit: u64) -> Self {
        let mut handle = self.clone();
        handle.size = Some(limit);
        handle
    }

    fn with_topn(&self, sort: Value, limit: u64) -> Self {
        let mut handle = self.clone();
        handle.size = Some(limit);
        handle.sort = Some(sort);
        handle
    }

    fn with_count(&self) -> Self {
        let mut handle = self.clone();
        handle.count = true;
        handle._source = None;
        handle
    }

    fn with_summarize(
        &self,
        aggs: Value,
        group_by: Vec<String>,
        count_fields: Vec<String>,
        agg_timestamp_fields: HashSet<String>,
    ) -> Self {
        let mut handle = self.clone();
        handle.aggs = Some(aggs);
        handle.group_by = group_by;
        handle.count_fields = count_fields;
        handle.agg_timestamp_fields = agg_timestamp_fields;
        handle._source = None;
        handle
    }

    fn with_union(&self, collection: &str) -> Self {
        let mut handle = self.clone();
        handle.collections.push(collection.to_string());
        handle
    }
}

impl fmt::Display for ElasticsearchHandle {
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

        if !self.query.is_empty() {
            let mut s = "filters=[".to_string();
            for (i, q) in self.query.iter().enumerate() {
                if i > 0 {
                    s.push_str(", ");
                }
                s.push_str(&format!("{q}"));
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

        if let Some(sort) = &self.sort {
            items.push(format!("sort={sort}"));
        }

        if let Some(size) = self.size {
            items.push(format!("size={size}"));
        }

        write!(f, "{}", items.join(", "))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ElasticsearchIndex {
    timestamp_field: Option<String>,
}

type ElasticsearchIndexes = HashMap<String, ElasticsearchIndex>;

#[derive(Debug)]
pub struct ElasticsearchConnector {
    config: ElasticsearchConfig,
    indexes: Arc<RwLock<ElasticsearchIndexes>>,
    interval_task: ShutdownFuture,
    client: Client,
}

impl Serialize for ElasticsearchConnector {
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

impl<'de> Deserialize<'de> for ElasticsearchConnector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        ElasticsearchConfig::deserialize(deserializer).map(ElasticsearchConnector::new)
    }
}

#[derive(Debug, Deserialize)]
struct CatIndicesResponse {
    index: String,
}

#[derive(Debug, Deserialize)]
struct IndexMappingProperties {
    #[serde(default)]
    properties: HashMap<String, IndexFieldMapping>,
}

#[derive(Debug, Deserialize)]
struct IndexFieldMapping {
    #[serde(rename = "type", default)]
    field_type: String,
}

#[derive(Debug, Deserialize)]
struct IndexMappingResponse {
    mappings: IndexMappingProperties,
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
    scroll_id: Option<String>,
    hits: SearchResponseHits,
}

#[derive(Debug, Serialize)]
struct ScrollRequest {
    scroll: String,
    scroll_id: String,
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

async fn response_to_bytes(response: Response) -> Result<BytesMut> {
    let status = response.status();
    if !status.is_success() {
        let text = response.text().await.unwrap_or_default();
        bail!(ConnectorError::ServerResp(status.as_u16(), text));
    }
    let bytes = response.bytes().await.context("bytes from response")?;
    METRICS
        .downloaded_bytes
        .with_label_values(&[CONNECTOR_ELASTICSEARCH])
        .inc_by(bytes.len() as u64);
    Ok(bytes.into())
}

#[instrument(skip_all, name = "elasticsearch send_request")]
async fn send_request(req: RequestBuilder) -> Result<BytesMut> {
    match req.send().await {
        Ok(response) => response_to_bytes(response).await,
        Err(e) => Err(eyre!(ConnectorError::Http(e))),
    }
}

fn format_value(value: &Value) -> String {
    if let Value::Timestamp(dt) = value {
        dt.format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| value.to_string())
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
                    "minimum_should_match": 1,
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
        Expr::Has(lhs, rhs) => {
            let (Expr::Field(field), Expr::Literal(Value::String(value))) = (&**lhs, &**rhs) else {
                return None;
            };
            if field.has_array_access() {
                return None;
            }
            json!({
                "match_phrase": {
                    field: value.as_str(),
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

impl ElasticsearchConnector {
    pub fn new(config: ElasticsearchConfig) -> Self {
        let indexes = Arc::new(RwLock::new(HashMap::new()));
        let interval_task = ShutdownFuture::new(
            refresh_indexes_at_interval(config.clone(), Arc::downgrade(&indexes)),
            "Elasticsearch indexes refresher",
        );

        let client = Client::new();

        Self {
            config,
            indexes,
            interval_task,
            client,
        }
    }
}

#[instrument(skip(client, auth), name = "elasticsearch get_indexes")]
async fn get_indexes(
    client: &Client,
    base_url: &str,
    auth: &ElasticsearchAuth,
) -> Result<ElasticsearchIndexes> {
    let start = std::time::Instant::now();
    METRICS
        .connector_requests_total
        .with_label_values(&[CONNECTOR_ELASTICSEARCH, OP_GET_INDEXES])
        .inc();

    let result: Result<ElasticsearchIndexes> = async {
        let url = format!("{base_url}/_cat/indices?format=json");
        let req = auth.apply_to_request(client.get(&url));
        let mut bytes = send_request(req).await?;
        let indices: Vec<CatIndicesResponse> =
            simd_json::serde::from_slice(bytes.as_mut()).context("parse indices response")?;

        let mut indexes = HashMap::new();
        for index_info in indices {
            let index_name = index_info.index;

            let mapping_url = format!("{base_url}/{index_name}/_mapping");
            let req = auth.apply_to_request(client.get(&mapping_url));
            let mut mapping_bytes = send_request(req).await?;

            let mapping_response: HashMap<String, IndexMappingResponse> =
                simd_json::serde::from_slice(mapping_bytes.as_mut())
                    .context("parse mapping response")?;

            let timestamp_field = mapping_response.get(&index_name).and_then(|resp| {
                resp.mappings
                    .properties
                    .iter()
                    .find(|(_, field)| field.field_type == "date")
                    .map(|(name, _)| name.clone())
            });

            indexes.insert(index_name, ElasticsearchIndex { timestamp_field });
        }

        Ok(indexes)
    }
    .await;

    let duration = start.elapsed().as_secs_f64();
    record_operation_result(CONNECTOR_ELASTICSEARCH, OP_GET_INDEXES, &result, duration);
    result
}

async fn refresh_indexes_at_interval(
    config: ElasticsearchConfig,
    weak_indexes: Weak<RwLock<ElasticsearchIndexes>>,
) {
    let client = Client::new();

    run_at_interval(
        async || {
            let Some(indexes) = weak_indexes.upgrade() else {
                return false;
            };

            match get_indexes(&client, &config.url, &config.auth).await {
                Ok(response) => {
                    debug!("Got indexes: {:?}", &response);
                    let mut guard = indexes.write();
                    *guard = response;
                }
                Err(e) => {
                    error!("Failed to get elasticsearch indexes: {:#}", e);
                }
            }

            true
        },
        config.refresh_interval,
    )
    .await;
}

#[instrument(skip(query, auth), name = "elasticsearch begin search")]
async fn begin_search(
    client: &Client,
    base_url: &str,
    index: &str,
    query: Option<Value>,
    auth: &ElasticsearchAuth,
    scroll_timeout: &Duration,
    scroll_size: u16,
) -> Result<(Vec<Log>, Option<String>)> {
    let start = std::time::Instant::now();
    METRICS
        .connector_requests_total
        .with_label_values(&[CONNECTOR_ELASTICSEARCH, OP_BEGIN_SEARCH])
        .inc();

    let url = format!(
        "{base_url}/{index}/_search?scroll={}s&size={scroll_size}",
        scroll_timeout.as_secs(),
    );

    let req = auth
        .apply_to_request(client.post(&url))
        .json(&query.unwrap_or_else(|| json!({})));

    let result: Result<(Vec<Log>, Option<String>)> = async {
        let mut bytes = send_request(req).await?;
        let data: SearchResponse =
            simd_json::serde::from_slice(bytes.as_mut()).context("parse search response")?;
        Ok((
            data.hits.hits.into_iter().map(|x| x.source).collect(),
            data.scroll_id,
        ))
    }
    .await;

    let duration = start.elapsed().as_secs_f64();
    record_operation_result(CONNECTOR_ELASTICSEARCH, OP_BEGIN_SEARCH, &result, duration);
    result
}

#[instrument(
    level = "debug",
    skip(client, auth),
    name = "elasticsearch continue scroll"
)]
async fn continue_scroll(
    client: &Client,
    base_url: &str,
    scroll_id: String,
    auth: &ElasticsearchAuth,
    scroll_timeout: &Duration,
) -> Result<(Vec<Log>, Option<String>)> {
    let start = std::time::Instant::now();
    METRICS
        .connector_requests_total
        .with_label_values(&[CONNECTOR_ELASTICSEARCH, OP_CONTINUE_SCROLL])
        .inc();

    let url = format!("{base_url}/_search/scroll");

    let req = auth
        .apply_to_request(client.post(&url))
        .json(&ScrollRequest {
            scroll: format!("{}s", scroll_timeout.as_secs()),
            scroll_id,
        });

    let result: Result<(Vec<Log>, Option<String>)> = async {
        let mut bytes = send_request(req).await?;
        let data: SearchResponse =
            simd_json::serde::from_slice(bytes.as_mut()).context("parse scroll response")?;
        Ok((
            data.hits.hits.into_iter().map(|x| x.source).collect(),
            data.scroll_id,
        ))
    }
    .await;

    let duration = start.elapsed().as_secs_f64();
    record_operation_result(
        CONNECTOR_ELASTICSEARCH,
        OP_CONTINUE_SCROLL,
        &result,
        duration,
    );
    result
}

#[instrument(skip(query, auth), name = "elasticsearch count")]
async fn count(
    client: &Client,
    base_url: &str,
    index: &str,
    query: Option<Value>,
    auth: &ElasticsearchAuth,
) -> Result<u64> {
    let url = format!("{base_url}/{index}/_count");

    let req = auth
        .apply_to_request(client.post(&url))
        .json(&query.unwrap_or_else(|| json!({})));

    let mut bytes = send_request(req).await?;
    let data: CountResponse =
        simd_json::serde::from_slice(bytes.as_mut()).context("parse count response")?;
    Ok(data.count)
}

impl ElasticsearchConnector {
    fn transform_log(mut log: Log, timestamp_field: &Option<String>) -> Result<Log> {
        if let Some(timestamp_field) = timestamp_field
            && let Some(value) = log.get_mut(timestamp_field)
            && let Value::String(s) = value
        {
            let dt = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                .context("parse elasticsearch datetime string response")?;
            *value = Value::from(dt)
        }
        Ok(log)
    }

    fn value_to_datetime(value: &mut Value) -> Result<()> {
        match value {
            Value::String(s) => {
                let dt = OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                    .context("parse elasticsearch datetime string")?;
                *value = Value::from(dt)
            }
            Value::Float(f) => {
                let dt = miso_common::time_utils::parse_timestamp_float(*f)
                    .map_err(|e| color_eyre::eyre::eyre!("parse elasticsearch datetime: {}", e))?;
                *value = Value::from(dt)
            }
            Value::UInt(n) => {
                let dt = miso_common::time_utils::parse_timestamp(*n as i64)
                    .map_err(|e| color_eyre::eyre::eyre!("parse elasticsearch datetime: {}", e))?;
                *value = Value::from(dt)
            }
            _ => {}
        }
        Ok(())
    }

    fn transform_group_by_timestamp_value(
        key: &str,
        mut value: Value,
        timestamp_field: &Option<String>,
    ) -> Result<Value> {
        if timestamp_field.as_deref() == Some(key) {
            Self::value_to_datetime(&mut value)?;
        }
        Ok(value)
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
        auth: ElasticsearchAuth,
        limit: Option<u64>,
        group_by: Vec<String>,
        count_fields: Vec<String>,
        agg_timestamp_fields: HashSet<String>,
        timestamp_field: Option<String>,
    ) -> Result<LogTryStream> {
        let url_search = format!("{url}/{index}/_search");
        let req = auth
            .apply_to_request(client.post(&url_search))
            .json(&query.unwrap_or_else(|| json!({})));

        let mut bytes = send_request(req).await?;
        let mut response: SearchAggregationResponse =
            simd_json::serde::from_slice(bytes.as_mut()).context("parse aggregation response")?;

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
            .context("parse elasticsearch aggregation response (group by)")?;
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
            .context("parse elasticsearch aggregation response (no group by)")?;
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

    #[allow(clippy::too_many_arguments)]
    async fn query_search(
        client: Client,
        url: String,
        index: String,
        query: Option<Value>,
        auth: ElasticsearchAuth,
        scroll_timeout: Duration,
        scroll_size: u16,
        limit: Option<u64>,
        timestamp_field: Option<String>,
    ) -> Result<LogTryStream> {
        let (mut logs, mut scroll_id) = begin_search(
            &client,
            &url,
            &index,
            query,
            &auth,
            &scroll_timeout,
            scroll_size,
        )
        .await?;

        if logs.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        Ok(Box::pin(try_stream! {
            let mut streamed = 0;

            for log in logs {
                yield Self::transform_log(log, &timestamp_field)?;
                increment_and_ret_on_limit!(streamed, limit);
            }

            while let Some(sid) = scroll_id {
                (logs, scroll_id) = continue_scroll(&client, &url, sid, &auth, &scroll_timeout).await?;
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
}

#[async_trait]
#[typetag::serde(name = "elasticsearch")]
impl Connector for ElasticsearchConnector {
    #[instrument(skip(self), name = "Elasticsearch get collection")]
    fn get_collection(&self, collection: &str) -> Option<Collection> {
        let guard = self.indexes.read();
        let index = guard.get(collection)?;

        let mut static_fields = HashMap::new();
        if let Some(timestamp_field) = &index.timestamp_field {
            static_fields.insert("@time".to_string(), timestamp_field.clone());
        }

        Some(Collection { static_fields })
    }

    fn get_handle(&self, collection: &str) -> Result<Box<dyn QueryHandle>> {
        let timestamp_field = self
            .indexes
            .read()
            .get(collection)
            .ok_or_eyre("collection not found")?
            .timestamp_field
            .clone();
        Ok(Box::new(ElasticsearchHandle::new(timestamp_field)))
    }

    #[instrument(skip(self), name = "Elasticsearch query")]
    async fn query(
        &self,
        collection: &str,
        handle: &dyn QueryHandle,
        _split: Option<&dyn Split>,
    ) -> Result<QueryResponse> {
        let url = self.config.url.clone();
        let auth = self.config.auth.clone();
        let scroll_timeout = self.config.scroll_timeout;

        let handle = downcast_unwrap!(handle, ElasticsearchHandle);
        let limit = handle.size;
        let scroll_size = limit.map_or(self.config.scroll_size, |l| {
            l.min(self.config.scroll_size as u64) as u16
        });
        let timestamp_field = handle.timestamp_field.clone();

        let mut collections = Vec::with_capacity(1 + handle.collections.len());
        collections.push(collection);
        collections.extend(handle.collections.iter().map(String::as_str));
        collections.sort();
        collections.dedup();
        let index = collections.join(",");

        let mut query_map = Map::new();

        if !handle.query.is_empty() {
            query_map.insert(
                "query",
                json!({
                    "bool": {
                        "must": handle.query.clone(),
                    }
                }),
            );
        }

        if let Some(source) = &handle._source {
            query_map.insert("_source", json!(source.clone()));
        }

        if let Some(sort) = &handle.sort {
            query_map.insert("sort", sort.clone());
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
            "Elasticsearch search '{}': {}",
            index,
            serde_json::to_string(&query)?
        );

        if handle.count {
            let mut result = count(&self.client, &url, &index, query, &auth).await?;
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
                    index,
                    query,
                    auth,
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
                index,
                query,
                auth,
                scroll_timeout,
                scroll_size,
                limit,
                timestamp_field,
            )
            .await?,
        ))
    }

    fn apply_filter(&self, ast: &Expr, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, ElasticsearchHandle);
        if handle.sort.is_some() || !handle.group_by.is_empty() {
            return None;
        }
        Some(Box::new(handle.with_filter(compile_filter_ast(ast)?)))
    }

    fn apply_project(
        &self,
        projections: &[ProjectField],
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, ElasticsearchHandle);
        if handle.count || !handle.group_by.is_empty() {
            return None;
        }

        let mut fields = Vec::with_capacity(projections.len());
        for project in projections {
            let Expr::Field(field) = &project.from else {
                return None;
            };
            if field != &project.to {
                return None;
            }
            fields.push(field.into());
        }
        Some(Box::new(handle.with_project(fields)))
    }

    fn apply_limit(&self, mut max: u64, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, ElasticsearchHandle);
        if let Some(size) = handle.size
            && size < max
        {
            max = size;
        }
        Some(Box::new(handle.with_limit(max)))
    }

    fn apply_topn(
        &self,
        sorts: &[Sort],
        mut max: u64,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, ElasticsearchHandle);
        if handle.sort.is_some() || !handle.group_by.is_empty() {
            return None;
        }

        if let Some(size) = handle.size
            && size < max
        {
            max = size;
        }

        let sort = Value::Array(
            sorts
                .iter()
                .map(|s| {
                    json!({
                        &s.by: {
                            "order": s.order.to_string(),
                            "missing": match s.nulls.to_string().as_str() {
                                "first" => "_first",
                                "last" => "_last",
                                _ => "_last",
                            }
                        }
                    })
                })
                .collect(),
        );
        Some(Box::new(handle.with_topn(sort, max)))
    }

    fn apply_count(&self, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, ElasticsearchHandle);
        if !handle.group_by.is_empty() {
            return None;
        }
        Some(Box::new(handle.with_count()))
    }

    fn apply_summarize(
        &self,
        config: &Summarize,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, ElasticsearchHandle);
        if handle.size.is_some() || handle.sort.is_some() || !handle.group_by.is_empty() {
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
                Aggregation::DCount(agg_field) => {
                    agg_json("cardinality", agg_field, &handle.timestamp_field)
                }
                Aggregation::Countif(Expr::Exists(agg_field)) => {
                    agg_json("value_count", agg_field, &handle.timestamp_field)
                }
                Aggregation::Count => {
                    count_fields.push(output_field.to_string());
                    continue;
                }
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

        for (i, bf) in config.by.iter().enumerate() {
            let name = format!("{AGGREGATION_RESULTS_NAME}_{i}");

            let bucket_def = match &bf.expr {
                Expr::Field(field) => json!({
                    "terms": {
                        "field": field,
                        "size": MAX_NUM_GROUPS,
                    }
                }),
                Expr::Bin(lhs, rhs) => {
                    let Expr::Field(field) = lhs.as_ref() else {
                        return None;
                    };
                    let Expr::Literal(value) = rhs.as_ref() else {
                        return None;
                    };
                    match value {
                        Value::Timespan(dur) => json!({
                            "date_histogram": {
                                "field": field,
                                "fixed_interval": format!("{}ms", dur.whole_milliseconds()),
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
            .map(|bf| match &bf.expr {
                Expr::Field(field) => Some(field.to_string()),
                Expr::Bin(lhs, _) => match lhs.as_ref() {
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
        let handle = downcast_unwrap!(handle, ElasticsearchHandle);
        let union_handle = downcast_unwrap!(union_handle, ElasticsearchHandle);

        if handle != union_handle {
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
            return None;
        }

        Some(Box::new(handle.with_union(union_collection)))
    }

    #[instrument(skip(self), name = "Elasticsearch close")]
    async fn close(&self) {
        self.interval_task.shutdown().await;
    }
}
