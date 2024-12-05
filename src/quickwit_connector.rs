use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};

use async_stream::try_stream;
use axum::async_trait;
use color_eyre::eyre::{bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, to_string};
use tokio::{
    select, spawn,
    sync::{watch, RwLock},
    task::JoinHandle,
    time::sleep,
};
use tracing::{debug, error, info, instrument};
use vrl::core::Value;

use crate::{
    connector::{Connector, QueryHandle, QueryResponse, Split},
    downcast_unwrap,
    log::{Log, LogTryStream},
    workflow::{
        filter::FilterAst,
        sort::Sort,
        summarize::{Aggregation, Summarize},
    },
};

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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct QuickwitHandle {
    queries: Vec<serde_json::Value>,
    sorts: Option<serde_json::Value>,
    aggs: Option<serde_json::Value>,
    group_by: Vec<String>,
    count_fields: Vec<String>,
    limit: Option<u32>,
    count: bool,
}

#[typetag::serde]
impl QueryHandle for QuickwitHandle {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl QuickwitHandle {
    fn with_filter(&self, query: serde_json::Value) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.queries.push(query);
        handle
    }

    fn with_limit(&self, limit: u32) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.limit = Some(limit);
        handle
    }

    fn with_topn(&self, sort: serde_json::Value, limit: u32) -> QuickwitHandle {
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
        aggs: serde_json::Value,
        group_by: Vec<String>,
        count_fields: Vec<String>,
    ) -> QuickwitHandle {
        let mut handle = self.clone();
        handle.aggs = Some(aggs);
        handle.group_by = group_by;
        handle.count_fields = count_fields;
        handle
    }
}

#[derive(Debug, Deserialize)]
struct IndexResponseConfig {
    index_id: String,
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
    doc_count: i64,
    key: Value,

    #[serde(flatten)]
    values: HashMap<String, SearchAggregationBuckets>,
}

#[derive(Debug, Deserialize)]
struct SearchAggregationBuckets {
    buckets: Vec<SearchAggregationBucket>,
}

#[derive(Debug, Deserialize)]
struct SearchAggregationResponse {
    aggregations: HashMap<String, SearchAggregationBuckets>,
}

fn default_refresh_interval() -> Duration {
    humantime::parse_duration("1m").expect("Invalid duration format")
}

fn default_scroll_timeout() -> Duration {
    humantime::parse_duration("1m").expect("Invalid duration format")
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(serde::de::Error::custom)
}

fn default_scroll_size() -> u16 {
    5000
}

#[derive(Debug, Clone, Deserialize)]
pub struct QuickwitConfig {
    url: String,

    #[serde(
        default = "default_refresh_interval",
        deserialize_with = "deserialize_duration"
    )]
    refresh_interval: Duration,

    #[serde(
        default = "default_scroll_timeout",
        deserialize_with = "deserialize_duration"
    )]
    scroll_timeout: Duration,

    #[serde(default = "default_scroll_size")]
    scroll_size: u16,
}

type SharedCollections = Arc<RwLock<Vec<String>>>;

#[derive(Debug)]
pub struct QuickwitConnector {
    config: QuickwitConfig,
    collections: SharedCollections,
    interval_task: JoinHandle<()>,
    shutdown_tx: watch::Sender<()>,
}

fn filter_ast_to_query(ast: &FilterAst) -> Option<serde_json::Value> {
    #[allow(unreachable_patterns)]
    Some(match ast {
        FilterAst::Or(filters) => {
            assert!(!filters.is_empty());
            json!({
                "bool": {
                    "should": filters.iter()
                        .map(filter_ast_to_query)
                        .collect::<Option<Vec<_>>>()?,
                }
            })
        }
        FilterAst::And(filters) => {
            assert!(!filters.is_empty());
            json!({
                "bool": {
                    "must": filters.iter()
                        .map(filter_ast_to_query)
                        .collect::<Option<Vec<_>>>()?,
                }
            })
        }
        FilterAst::StartsWith(field, prefix) => {
            json!({
                "match_phrase_prefix": {
                    field: {
                        "query": prefix
                    }
                }
            })
        }
        FilterAst::Eq(field, value) => {
            json!({
                "term": {
                    field: {
                        "value": value
                    }
                }
            })
        }
        FilterAst::Ne(field, value) => {
            json!({
                "bool": {
                    "must_not": {
                        "term": {
                            field: value
                        }
                    }
                }
            })
        }
        FilterAst::Gt(field, value) => {
            json!({
                "range": {
                    field: {
                        "gt": value
                    }
                }
            })
        }
        FilterAst::Gte(field, value) => {
            json!({
                "range": {
                    field: {
                        "gte": value
                    }
                }
            })
        }
        FilterAst::Lt(field, value) => {
            json!({
                "range": {
                    field: {
                        "lt": value
                    }
                }
            })
        }
        FilterAst::Lte(field, value) => {
            json!({
                "range": {
                    field: {
                        "lte": value
                    }
                }
            })
        }
        _ => return None,
    })
}

#[instrument(name = "GET and parse quickwit begin search results")]
async fn begin_search(
    base_url: &str,
    index: &str,
    query: Option<serde_json::Value>,
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
    let client = Client::new();

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
    let data: SearchResponse = serde_json::from_str(&text)?;
    Ok((
        data.hits.hits.into_iter().map(|x| x.source).collect(),
        data.scroll_id,
    ))
}

#[instrument(name = "GET and parse quickwit continue search results")]
async fn continue_search(
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

#[instrument(name = "GET and parse quickwit count result")]
async fn count(base_url: &str, index: &str, query: Option<serde_json::Value>) -> Result<u64> {
    let url = format!("{}/api/v1/_elastic/{}/_count", base_url, index);
    let client = Client::new();

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

#[instrument(name = "GET and parse quickwit search aggregation results")]
async fn search_aggregation(
    base_url: &str,
    index: &str,
    query: Option<serde_json::Value>,
) -> Result<SearchAggregationResponse> {
    let url = format!("{}/api/v1/_elastic/{}/_search", base_url, index,);
    let client = Client::new();

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
    Ok(serde_json::from_str(&text)?)
}

#[instrument(name = "GET and parse quickwit indexes")]
async fn get_indexes(base_url: &str) -> Result<Vec<String>> {
    let url = format!("{}/api/v1/indexes", base_url);
    let client = Client::new();
    let response = client.get(&url).send().await.context("http request")?;
    if !response.status().is_success() {
        bail!("GET {} failed with status: {}", &url, response.status());
    }
    let text = response.text().await.context("text from response")?;
    let data: Vec<IndexResponse> = serde_json::from_str(&text)?;
    Ok(data.into_iter().map(|x| x.index_config.index_id).collect())
}

async fn refresh_indexes(url: &str, collections: &SharedCollections) {
    match get_indexes(url).await {
        Ok(indexes) => {
            debug!("Got indexes: {:?}", &indexes);
            let mut guard = collections.write().await;
            *guard = indexes;
        }
        Err(e) => {
            error!("Failed to get quickwit indexes: {}", e);
        }
    }
}

async fn run_interval_task(
    config: QuickwitConfig,
    collections: SharedCollections,
    mut shutdown_rx: watch::Receiver<()>,
) {
    let future = async {
        refresh_indexes(&config.url, &collections).await;
        loop {
            sleep(config.refresh_interval).await;
            refresh_indexes(&config.url, &collections).await;
        }
    };

    select! {
        _ = future => {
            panic!("Interval future done looping?");
        }
        _ = shutdown_rx.changed() => {
            info!("Shutdown signal received. Stopping task.");
        }
    }
}

impl QuickwitConnector {
    pub fn new(config: QuickwitConfig) -> QuickwitConnector {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let collections = Arc::new(RwLock::new(Vec::new()));
        let interval_task = spawn(run_interval_task(
            config.clone(),
            collections.clone(),
            shutdown_rx,
        ));

        Self {
            config,
            collections,
            interval_task,
            shutdown_tx,
        }
    }

    fn query_search(
        url: String,
        index: String,
        query: Option<serde_json::Value>,
        scroll_timeout: Duration,
        scroll_size: u16,
        limit: Option<u32>,
    ) -> LogTryStream {
        Box::pin(try_stream! {
            if let Some(limit) = limit {
                if limit == 0 {
                    return;
                }
            }

            let mut streamed = 0;
            let (mut logs, mut scroll_id) = begin_search(
                &url,
                &index,
                query,
                &scroll_timeout,
                scroll_size
            ).await?;

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

            loop {
                (logs, scroll_id) = continue_search(&url, scroll_id, &scroll_timeout).await?;
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
        })
    }

    fn parse_buckets(
        mut buckets_wrap: HashMap<String, SearchAggregationBuckets>,
        index: usize,
        group_by: &[String],
        count_fields: &[String],
        keys_stack: &mut Vec<Value>,
        logs: &mut Vec<Log>,
    ) -> Result<()> {
        let bucket_name = format!("{}_{}", AGGREGATION_RESULTS_NAME, index);
        let Some(buckets) = buckets_wrap.remove(&bucket_name) else {
            bail!("Bucket '{bucket_name}' not found");
        };

        for bucket in buckets.buckets {
            if bucket.values.is_empty() {
                keys_stack.push(bucket.key);

                let mut log = Log::new();
                for (key, value) in group_by.iter().zip(keys_stack.iter()) {
                    log.insert(key.clone().into(), value.clone());
                }
                for key in count_fields {
                    log.insert(key.clone().into(), Value::Integer(bucket.doc_count));
                }
                logs.push(log);

                keys_stack.pop();
            } else {
                keys_stack.push(bucket.key);

                Self::parse_buckets(
                    bucket.values,
                    index + 1,
                    group_by,
                    count_fields,
                    keys_stack,
                    logs,
                )?;

                keys_stack.pop();
            }
        }

        Ok(())
    }

    async fn query_aggregation(
        url: String,
        index: String,
        query: Option<serde_json::Value>,
        group_by: Vec<String>,
        count_fields: Vec<String>,
    ) -> Result<LogTryStream> {
        let response = search_aggregation(&url, &index, query).await?;

        let mut logs = Vec::new();
        Self::parse_buckets(
            response.aggregations,
            0,
            &group_by,
            &count_fields,
            &mut Vec::new(),
            &mut logs,
        )?;

        Ok(Box::pin(try_stream! {
            if logs.is_empty() {
                return;
            }

            for log in logs {
                yield log;
            }
        }))
    }
}

#[async_trait]
impl Connector for QuickwitConnector {
    async fn does_collection_exist(&self, collection: &str) -> bool {
        self.collections
            .read()
            .await
            .iter()
            .any(|s| s == collection)
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
        let collection = collection.to_string();
        let scroll_timeout = self.config.scroll_timeout;

        let handle = downcast_unwrap!(handle, QuickwitHandle);
        let limit = handle.limit;
        let scroll_size = limit.map_or(self.config.scroll_size, |l| {
            l.min(self.config.scroll_size as u32) as u16
        });

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
            collection,
            to_string(&query)?
        );

        if handle.count {
            let mut result = count(&url, &collection, query).await?;
            if let Some(limit) = limit {
                result = (limit as u64).min(result);
            }
            return Ok(QueryResponse::Count(result));
        }

        if is_aggregation_query {
            return Ok(QueryResponse::Logs(
                Self::query_aggregation(
                    url,
                    collection,
                    query,
                    handle.group_by.clone(),
                    handle.count_fields.clone(),
                )
                .await?,
            ));
        }

        Ok(QueryResponse::Logs(Self::query_search(
            url,
            collection,
            query,
            scroll_timeout,
            scroll_size,
            limit,
        )))
    }

    fn apply_filter(
        &self,
        ast: &FilterAst,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        Some(Box::new(handle.with_filter(filter_ast_to_query(ast)?)))
    }

    fn apply_limit(&self, max: u32, handle: &dyn QueryHandle) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if let Some(limit) = handle.limit {
            if limit < max {
                return None;
            }
        }
        Some(Box::new(handle.with_limit(max)))
    }

    fn apply_topn(
        &self,
        sorts: &[Sort],
        max: u32,
        handle: &dyn QueryHandle,
    ) -> Option<Box<dyn QueryHandle>> {
        let handle = downcast_unwrap!(handle, QuickwitHandle);
        if let Some(limit) = handle.limit {
            if limit < max {
                return None;
            }
        }

        let sorts = serde_json::Value::Array(
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
        if handle.aggs.is_some() {
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

        Some(Box::new(handle.with_summarize(
            aggs,
            config.by.clone(),
            count_fields,
        )))
    }

    async fn close(self) {
        if let Err(e) = self.shutdown_tx.send(()) {
            error!("Failed to send shutdown to quickwit interval task: {}", e);
            return;
        }
        if let Err(e) = self.interval_task.await {
            error!("Failed to join quickwit interval task: {}", e);
        }
    }
}
