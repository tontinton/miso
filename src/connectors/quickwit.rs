use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use async_stream::try_stream;
use axum::async_trait;
use color_eyre::eyre::{bail, Context, Result};
use futures_util::stream;
use parking_lot::RwLock;
use reqwest::Client;
use serde::{ser::SerializeMap, Deserialize, Serialize, Serializer};
use serde_json::{json, to_string, Value};
use tracing::{debug, error, info, instrument};

use crate::{
    downcast_unwrap,
    humantime_utils::{deserialize_duration, serialize_duration},
    log::{Log, LogTryStream},
    run_at_interval::run_at_interval,
    shutdown_future::ShutdownFuture,
    workflow::{
        filter::FilterAst,
        sort::Sort,
        summarize::{Aggregation, Summarize},
        Workflow, WorkflowStep,
    },
};

use super::{Connector, QueryHandle, QueryResponse, Split};

static AGGREGATION_RESULTS_NAME: &str = "summarize";

/// Quickwit doesn't yet support pagination over aggregation queries.
/// This will be the max amount of groups we pull from it (taken from quickwit's code).
const MAX_NUM_GROUPS: usize = 65000;

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
    queries: Vec<Value>,
    sorts: Option<Value>,
    aggs: Option<Value>,
    group_by: Vec<String>,
    count_fields: Vec<String>,
    limit: Option<u32>,
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
    fn is_empty(&self) -> bool {
        self == &Self::default()
    }

    fn with_filter(&self, query: Value) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.queries.push(query);
        handle
    }

    fn with_limit(&self, limit: u32) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.limit = Some(limit);
        handle
    }

    fn with_topn(&self, sort: Value, limit: u32) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.limit = Some(limit);
        handle.sorts = Some(sort);
        handle
    }

    fn with_count(&self) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.count = true;
        handle
    }

    fn with_summarize(
        &self,
        aggs: Value,
        group_by: Vec<String>,
        count_fields: Vec<String>,
    ) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.aggs = Some(aggs);
        handle.group_by = group_by;
        handle.count_fields = count_fields;
        handle
    }

    fn with_union(&self, collection: &str) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.collections.push(collection.to_string());
        handle
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

type QuickwitIndexes = BTreeMap<String, QuickwitIndex>;

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

fn filter_ast_to_query(ast: &FilterAst) -> Option<Value> {
    Some(match ast {
        FilterAst::Or(filters) => {
            json!({
                "bool": {
                    "should": filters.iter()
                        .map(filter_ast_to_query)
                        .collect::<Option<Vec<_>>>()?,
                }
            })
        }
        FilterAst::And(filters) => {
            json!({
                "bool": {
                    "must": filters.iter()
                        .map(filter_ast_to_query)
                        .collect::<Option<Vec<_>>>()?,
                }
            })
        }
        FilterAst::Not(filter) => {
            json!({
                "bool": {
                    "must_not": filter_ast_to_query(filter)?,
                }
            })
        }
        FilterAst::Exists(field) => {
            json!({
                "exists": {
                    "field": field,
                }
            })
        }
        FilterAst::In(lhs, rhs) => {
            let FilterAst::Id(field) = &**lhs else {
                return None;
            };

            let values = rhs
                .iter()
                .map(|x| match x {
                    FilterAst::Lit(Value::String(v)) => Some(v.clone()),
                    FilterAst::Lit(value) => Some(value.to_string()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>()?;

            json!({
                "terms": {
                    field: values
                }
            })
        }
        FilterAst::StartsWith(lhs, rhs) => {
            let (FilterAst::Id(field), FilterAst::Lit(prefix)) = (&**lhs, &**rhs) else {
                return None;
            };
            json!({
                "match_phrase_prefix": {
                    field: {
                        "query": if let Value::String(v) = prefix {
                            v.clone()
                        } else {
                            prefix.to_string()
                        },
                    }
                }
            })
        }
        FilterAst::Eq(lhs, rhs) => {
            let (FilterAst::Id(field), FilterAst::Lit(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            json!({
                "term": {
                    field: {
                        "value": if let Value::String(v) = value {
                            v.clone()
                        } else {
                            value.to_string()
                        },
                    }
                }
            })
        }
        FilterAst::Ne(lhs, rhs) => {
            let (FilterAst::Id(field), FilterAst::Lit(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            json!({
                "bool": {
                    "must_not": {
                        "term": {
                            field: if let Value::String(v) = value {
                                v.clone()
                            } else {
                                value.to_string()
                            },
                        }
                    }
                }
            })
        }
        FilterAst::Gt(lhs, rhs) => {
            let (FilterAst::Id(field), FilterAst::Lit(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            json!({
                "range": {
                    field: {
                        "gt": if let Value::String(v) = value {
                            v.clone()
                        } else {
                            value.to_string()
                        },
                    }
                }
            })
        }
        FilterAst::Gte(lhs, rhs) => {
            let (FilterAst::Id(field), FilterAst::Lit(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            json!({
                "range": {
                    field: {
                        "gte": if let Value::String(v) = value {
                            v.clone()
                        } else {
                            value.to_string()
                        },
                    }
                }
            })
        }
        FilterAst::Lt(lhs, rhs) => {
            let (FilterAst::Id(field), FilterAst::Lit(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            json!({
                "range": {
                    field: {
                        "lt": if let Value::String(v) = value {
                            v.clone()
                        } else {
                            value.to_string()
                        },
                    }
                }
            })
        }
        FilterAst::Lte(lhs, rhs) => {
            let (FilterAst::Id(field), FilterAst::Lit(value)) = (&**lhs, &**rhs) else {
                return None;
            };
            json!({
                "range": {
                    field: {
                        "lte": if let Value::String(v) = value {
                            v.clone()
                        } else {
                            value.to_string()
                        },
                    }
                }
            })
        }
        _ => return None,
    })
}

#[instrument(skip(query), name = "GET and parse quickwit begin search results")]
async fn begin_search(
    client: &Client,
    base_url: &str,
    index: &str,
    query: Option<Value>,
    scroll_timeout: &Duration,
    scroll_size: u16,
) -> Result<(Vec<Log>, String)> {
    let url = format!(
        "{}/api/v1/_elastic/{}/_search?scroll={}ms&size={}",
        base_url,
        index,
        scroll_timeout.as_millis(),
        scroll_size,
    );

    let mut req = client.get(&url);
    if let Some(query) = query {
        req = req.json(&query);
    }

    let response = req.send().await.context("http request")?;
    let status = response.status();
    if !status.is_success() {
        if let Ok(text) = response.text().await {
            bail!("GET {} failed with status {}: {}", &url, status, text);
        } else {
            bail!("GET {} failed with status {}", &url, status);
        }
    }
    let text = response.text().await.context("text from response")?;
    let data: SearchResponse = serde_json::from_str(&text).context("parse response")?;
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
    let url = format!("{}/api/v1/_elastic/_search/scroll", base_url);
    let client = Client::new();
    let response = client
        .get(&url)
        .json(&ContinueSearchRequest {
            scroll_id,
            scroll: format!("{}ms", scroll_timeout.as_millis()),
        })
        .send()
        .await
        .context("http request")?;
    if !response.status().is_success() {
        bail!("GET {} failed with status: {}", &url, response.status());
    }
    let text = response.text().await.context("text from response")?;
    let data: SearchResponse = serde_json::from_str(&text)?;
    Ok((
        data.hits.hits.into_iter().map(|x| x.source).collect(),
        data.scroll_id,
    ))
}

#[instrument(skip(query), name = "GET and parse quickwit count result")]
async fn count(client: &Client, base_url: &str, index: &str, query: Option<Value>) -> Result<u64> {
    let url = format!("{}/api/v1/_elastic/{}/_count", base_url, index);

    let mut req = client.get(&url);
    if let Some(query) = query {
        req = req.json(&query);
    }

    let response = req.send().await.context("http request")?;
    let status = response.status();
    if !status.is_success() {
        if let Ok(text) = response.text().await {
            bail!("GET {} failed with status {}: {}", &url, status, text);
        } else {
            bail!("GET {} failed with status {}", &url, status);
        }
    }
    let text = response.text().await.context("text from response")?;
    let data: CountResponse = serde_json::from_str(&text)?;
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
    let url = format!("{}/api/v1/_elastic/{}/_search", base_url, index,);

    let mut req = client.get(&url);
    if let Some(query) = query {
        req = req.json(&query);
    }

    let response = req.send().await.context("http request")?;
    let status = response.status();
    if !status.is_success() {
        if let Ok(text) = response.text().await {
            bail!("GET {} failed with status {}: {}", &url, status, text);
        } else {
            bail!("GET {} failed with status {}", &url, status);
        }
    }
    let text = response.text().await.context("get text from response")?;
    serde_json::from_str(&text).context("parse response")
}

#[instrument(name = "GET and parse quickwit indexes")]
async fn get_indexes(client: &Client, base_url: &str) -> Result<QuickwitIndexes> {
    let url = format!("{}/api/v1/indexes", base_url);
    let response = client.get(&url).send().await.context("http request")?;
    if !response.status().is_success() {
        bail!("GET {} failed with status: {}", &url, response.status());
    }
    let text = response.text().await.context("text from response")?;
    let data: Vec<IndexResponse> = serde_json::from_str(&text)?;
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
                    error!("Failed to get quickwit indexes: {}", e);
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
        let indexes = Arc::new(RwLock::new(BTreeMap::new()));
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

    async fn query_search(
        client: Client,
        url: String,
        index: String,
        query: Option<Value>,
        scroll_timeout: Duration,
        scroll_size: u16,
        limit: Option<u32>,
    ) -> Result<LogTryStream> {
        let start = Instant::now();

        let (mut logs, mut scroll_id) =
            begin_search(&client, &url, &index, query, &scroll_timeout, scroll_size).await?;

        let duration = start.elapsed();
        debug!(elapsed_time = ?duration, "Begin search time");

        if logs.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        Ok(Box::pin(try_stream! {
            let mut streamed = 0;

            for log in logs {
                yield log;
                streamed += 1;
                if let Some(limit) = limit {
                    if streamed >= limit {
                        return;
                    }
                }
            }

            loop {
                (logs, scroll_id) = continue_search(&client, &url, scroll_id, &scroll_timeout).await?;
                if logs.is_empty() {
                    return;
                }
                for log in logs {
                    yield log;
                    streamed += 1;
                    if let Some(limit) = limit {
                        if streamed >= limit {
                            return;
                        }
                    }
                }
            }
        }))
    }

    fn parse_last_bucket(
        buckets_or_value_wrap: HashMap<String, SearchAggregationBucketsOrValue>,
        doc_count: u64,
        group_by: &[String],
        count_fields: &[String],
        keys_stack: &[Value],
        logs: &mut Vec<Log>,
    ) -> Result<()> {
        let mut log = Log::new();

        for (key, value) in group_by.iter().zip(keys_stack.iter()) {
            log.insert(key.clone(), value.clone());
        }
        for key in count_fields {
            log.insert(key.clone(), Value::from(doc_count));
        }

        for (field, buckets_or_value) in buckets_or_value_wrap {
            let SearchAggregationBucketsOrValue::Value(value_wrap) = buckets_or_value else {
                bail!("expected value, not bucket");
            };
            log.insert(field, value_wrap.value);
        }

        logs.push(log);

        Ok(())
    }

    fn parse_buckets(
        buckets_or_value: SearchAggregationBucketsOrValue,
        index: usize,
        group_by: &[String],
        count_fields: &[String],
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
                    keys_stack,
                    logs,
                )?;
            } else {
                let bucket_name = format!("{}_{}", AGGREGATION_RESULTS_NAME, index);
                let Some(next_buckets_or_value) = bucket.buckets_or_value.remove(&bucket_name)
                else {
                    bail!("bucket '{bucket_name}' not found");
                };

                Self::parse_buckets(
                    next_buckets_or_value,
                    index + 1,
                    group_by,
                    count_fields,
                    keys_stack,
                    logs,
                )?;
            }

            keys_stack.pop();
        }

        Ok(())
    }

    async fn query_aggregation(
        client: Client,
        url: String,
        index: String,
        query: Option<Value>,
        limit: Option<u32>,
        group_by: Vec<String>,
        count_fields: Vec<String>,
    ) -> Result<LogTryStream> {
        let mut response = search_aggregation(&client, &url, &index, query)
            .await
            .context("run quickwit aggregation query")?;

        let mut logs = Vec::new();

        let first_bucket_name = format!("{}_0", AGGREGATION_RESULTS_NAME);
        if let Some(buckets_or_value) = response.aggregations.remove(&first_bucket_name) {
            Self::parse_buckets(
                buckets_or_value,
                1,
                &group_by,
                &count_fields,
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

            for log in logs {
                yield log;
                streamed += 1;
                if let Some(limit) = limit {
                    if streamed >= limit {
                        return;
                    }
                }
            }
        }))
    }
}

#[async_trait]
#[typetag::serde(name = "quickwit")]
impl Connector for QuickwitConnector {
    fn does_collection_exist(&self, collection: &str) -> bool {
        self.indexes.read().contains_key(collection)
    }

    async fn get_splits(&self) -> Vec<Arc<dyn Split>> {
        vec![Arc::new(QuickwitSplit {}) as Arc<dyn Split>]
    }

    fn get_handle(&self) -> Box<dyn QueryHandle> {
        Box::new(QuickwitHandle::default())
    }

    async fn query(
        &self,
        collection: &str,
        split: &dyn Split,
        handle: &dyn QueryHandle,
    ) -> Result<QueryResponse> {
        let Some(_) = split.as_any().downcast_ref::<QuickwitSplit>() else {
            bail!("Downcasting split to wrong struct?");
        };

        let url = self.config.url.clone();
        let scroll_timeout = self.config.scroll_timeout;

        let handle = downcast_unwrap!(handle, QuickwitHandle);
        let limit = handle.limit;
        let scroll_size = limit.map_or(self.config.scroll_size, |l| {
            l.min(self.config.scroll_size as u32) as u16
        });

        let mut collections = Vec::with_capacity(1 + handle.collections.len());
        collections.push(collection);
        collections.extend(handle.collections.iter().map(String::as_str));
        collections.sort();
        collections.dedup();
        let collections = collections.join(",");

        let mut query_map = BTreeMap::new();

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
            "Quickwit search '{}': {}",
            collections,
            to_string(&query)?
        );

        if handle.count {
            let mut result = count(&self.client, &url, &collections, query).await?;
            if let Some(limit) = limit {
                result = (limit as u64).min(result);
            }
            return Ok(QueryResponse::Count(result));
        }

        if let Some(limit) = limit {
            if limit == 0 {
                return Ok(QueryResponse::Logs(Box::pin(stream::empty())));
            }
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
            )
            .await?,
        ))
    }

    fn apply_filter(
        &self,
        ast: &FilterAst,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if handle.sorts.is_some() {
            // Cannot filter over top-n in Quickwit.
            return None;
        }
        Some(Box::new(handle.with_filter(filter_ast_to_query(ast)?)))
    }

    fn apply_limit(&self, mut max: u32, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if let Some(limit) = handle.limit {
            if limit < max {
                max = limit;
            }
        }
        Some(Box::new(handle.with_limit(max)))
    }

    fn apply_topn(
        &self,
        sorts: &[Sort],
        mut max: u32,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if handle.sorts.is_some() {
            // Cannot top-n over top-n in Quickwit.
            return None;
        }

        if !handle.group_by.is_empty() {
            // Maybe this can actually be implemented, need to check in the future.
            return None;
        }

        if let Some(limit) = handle.limit {
            if limit < max {
                max = limit;
            }
        }

        let sorts = Value::Array(
            sorts
                .iter()
                .map(|sort| {
                    json!({
                        &sort.by: {
                            "order": &sort.order,
                            "nulls": &sort.nulls,
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
        if handle.limit.is_some() || handle.sorts.is_some() {
            // Quickwit's query (like Elasticsearch's) is not pipelined, most similar to SQL.
            // When you request it to both sort (or limit) and aggregate, it will always first
            // aggregate and then sort (or limit), no way to control the order of these 2 AFAIK.
            // So we do the aggregation in-process instead of pushing it down to Quickwit.
            return None;
        }

        let mut count_fields = Vec::new();
        let mut inner_aggs = BTreeMap::new();

        for (output_field, agg) in &config.aggs {
            let value = match agg {
                Aggregation::Min(agg_field) => {
                    json!({
                        "min": {
                            "field": agg_field,
                        }
                    })
                }
                Aggregation::Max(agg_field) => {
                    json!({
                        "max": {
                            "field": agg_field,
                        }
                    })
                }
                Aggregation::Sum(agg_field) => {
                    json!({
                        "sum": {
                            "field": agg_field,
                        }
                    })
                }
                Aggregation::Count => {
                    // Count is always returned in doc_count.
                    count_fields.push(output_field.clone());
                    continue;
                }
            };

            inner_aggs.insert(output_field, value);
        }

        let mut aggs = json!({});

        let mut current_agg = &mut aggs;
        for (i, field) in config.by.iter().enumerate() {
            let name = format!("{}_{}", AGGREGATION_RESULTS_NAME, i);
            let nested_agg = json!({
                    &name: {
                        "terms": {
                            "field": field,
                            "size": MAX_NUM_GROUPS,
                        }
                    }
            });
            current_agg["aggs"] = nested_agg;
            current_agg = current_agg.get_mut("aggs").unwrap().get_mut(&name).unwrap();
        }

        current_agg["aggs"] = json!(inner_aggs);

        Some(Box::new(handle.with_summarize(
            aggs,
            config.by.clone(),
            count_fields,
        )))
    }

    fn apply_union(
        &self,
        scan_collection: &str,
        union: &Workflow,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);

        if !handle.is_empty() || union.steps.len() > 1 {
            // Quickwit only supports querying multiple indexes with the exact same query.
            return None;
        }

        let WorkflowStep::Scan(scan) = &union.steps[0] else {
            return None;
        };

        let can_union = match (
            self.indexes.read().get(scan_collection),
            self.indexes.read().get(&scan.collection),
        ) {
            (Some(l), Some(r)) => l.timestamp_field == r.timestamp_field,
            _ => false,
        };

        if !can_union {
            // Quickwit only supports multi index search when the timestamp fields are the same.
            return None;
        }

        Some(Box::new(handle.with_union(&scan.collection)))
    }

    async fn close(&self) {
        self.interval_task.shutdown().await;
    }
}
