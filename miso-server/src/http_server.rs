use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_recursion::async_recursion;
use async_stream::stream;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response, Sse, sse::Event},
    routing::{delete, get, post},
};
use color_eyre::{Result, eyre::Context};
use futures_util::{Stream, TryStreamExt};
use miso_common::{
    humantime_utils::deserialize_duration, metrics::METRICS, run_at_interval::run_at_interval,
    shutdown_future::ShutdownFuture,
};
use miso_connectors::{Connector, ConnectorError, ConnectorState, quickwit::QuickwitConnector};
use miso_optimizations::Optimizer;
use miso_workflow::{Workflow, WorkflowStep, partial_stream::PartialStream, scan::Scan};
use miso_workflow_types::{
    filter::FilterAst,
    join::Join,
    project::ProjectField,
    sort::Sort,
    summarize::{GroupAst, Summarize},
};
use prometheus::TextEncoder;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{sync::RwLock, task::spawn_blocking};
use tokio_util::sync::CancellationToken;
use tracing::{Level, debug, error, info, span};
use uuid::Uuid;

const DEFAULT_STATS_FETCH_INTERVAL: Duration = Duration::from_secs(60 * 60 * 3); // 3 hours.
const TOKIO_METRICS_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const VIEWS_CONNECTOR_NAME: &str = "views";
const INTERNAL_SERVER_ERROR: &str = "Internal server error";
const ERROR_LOG_FIELD_NAME: &str = "_error";

pub type ConnectorsMap = BTreeMap<String, Arc<ConnectorState>>;
type ViewsMap = BTreeMap<String, Vec<QueryStep>>;

struct App {
    connectors: RwLock<ConnectorsMap>,
    optimizer: Arc<Optimizer>,
    views: RwLock<ViewsMap>,
    _tokio_metrics_task: ShutdownFuture,
}

impl App {
    fn new(connectors: ConnectorsMap, optimizer: Optimizer) -> Result<Self> {
        let tokio_metrics_task =
            ShutdownFuture::new(collect_tokio_metrics(), "Tokio metrics collector");

        Ok(Self {
            connectors: RwLock::new(connectors),
            optimizer: Arc::new(optimizer),
            views: RwLock::new(BTreeMap::new()),
            _tokio_metrics_task: tokio_metrics_task,
        })
    }
}

async fn collect_tokio_metrics() {
    let handle = tokio::runtime::Handle::current();
    let metrics = handle.metrics();

    run_at_interval(
        async || {
            METRICS
                .tokio_worker_threads
                .set(metrics.num_workers() as i64);
            METRICS
                .tokio_alive_tasks
                .set(metrics.num_alive_tasks() as i64);
            true
        },
        TOKIO_METRICS_UPDATE_INTERVAL,
    )
    .await;
}

#[async_recursion]
pub async fn to_workflow_steps(
    connectors: &ConnectorsMap,
    views: &ViewsMap,
    query_steps: Vec<QueryStep>,
) -> Result<Vec<WorkflowStep>, HttpError> {
    if query_steps.is_empty() {
        return Err(HttpError::new(
            StatusCode::BAD_REQUEST,
            "empty query".to_string(),
        ));
    }

    let QueryStep::Scan(..) = query_steps[0] else {
        return Err(HttpError::new(
            StatusCode::NOT_FOUND,
            "first step must be scan".to_string(),
        ));
    };

    let num_steps = query_steps.len();
    let mut steps = Vec::with_capacity(num_steps);

    for (i, step) in query_steps.into_iter().enumerate() {
        match step {
            QueryStep::Scan(..) if i > 0 => {
                return Err(HttpError::new(
                    StatusCode::BAD_REQUEST,
                    "scan can only be the first step of a query".to_string(),
                ));
            }
            QueryStep::Scan(connector_name, view) if connector_name == VIEWS_CONNECTOR_NAME => {
                let Some(view_steps) = views.get(&view).cloned() else {
                    return Err(HttpError::new(
                        StatusCode::NOT_FOUND,
                        format!("view '{view}' not found"),
                    ));
                };

                steps.extend(to_workflow_steps(connectors, views, view_steps).await?);
            }
            QueryStep::Scan(connector_name, collection) => {
                let Some(connector_state) = connectors.get(&connector_name).cloned() else {
                    return Err(HttpError::new(
                        StatusCode::NOT_FOUND,
                        format!("connector '{connector_name}' not found"),
                    ));
                };

                info!(?collection, "Checking whether collection exists");
                if !connector_state.connector.does_collection_exist(&collection) {
                    info!(?collection, "Collection doesn't exist");
                    return Err(HttpError::new(
                        StatusCode::NOT_FOUND,
                        format!("collection '{collection}' not found"),
                    ));
                }

                steps.push(WorkflowStep::Scan(
                    Scan::from_connector_state(connector_state, connector_name, collection).await,
                ));
            }
            _ if steps.is_empty() => {
                return Err(HttpError::new(
                    StatusCode::BAD_REQUEST,
                    "first query step must be a scan".to_string(),
                ));
            }
            QueryStep::Filter(ast) => {
                steps.push(WorkflowStep::Filter(ast));
            }
            QueryStep::Project(fields) => {
                steps.push(WorkflowStep::Project(fields));
            }
            QueryStep::Extend(fields) => {
                steps.push(WorkflowStep::Extend(fields));
            }
            QueryStep::Limit(max) => {
                steps.push(WorkflowStep::Limit(max));
            }
            QueryStep::Sort(sort) => {
                steps.push(WorkflowStep::Sort(sort));
            }
            QueryStep::Top(sort, max) => {
                steps.push(WorkflowStep::TopN(sort, max));
            }
            QueryStep::Summarize(config) => {
                steps.push(WorkflowStep::Summarize(config));
            }
            QueryStep::Distinct(by) => {
                steps.push(WorkflowStep::Summarize(Summarize {
                    aggs: BTreeMap::new(),
                    by: by.into_iter().map(GroupAst::Id).collect(),
                }));
            }
            QueryStep::Union(inner_steps) => {
                steps.push(WorkflowStep::Union(Workflow::new(
                    to_workflow_steps(connectors, views, inner_steps).await?,
                )));
            }
            QueryStep::Join(config, inner_steps) => {
                steps.push(WorkflowStep::Join(
                    config,
                    Workflow::new(to_workflow_steps(connectors, views, inner_steps).await?),
                ));
            }
            QueryStep::Count => {
                steps.push(WorkflowStep::Count);
            }
        }
    }

    Ok(steps)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryStep {
    Scan(/*connector=*/ String, /*collection=*/ String),
    Filter(FilterAst),
    Project(Vec<ProjectField>),
    Extend(Vec<ProjectField>),
    Limit(u32),
    Sort(Vec<Sort>),
    Top(Vec<Sort>, u32),
    Summarize(Summarize),
    Distinct(Vec<String>),
    Union(Vec<QueryStep>),
    Join(Join, Vec<QueryStep>),
    Count,
}

#[derive(Deserialize)]
struct QueryRequest {
    /// The query id to set. If not set, the server will randomly generate an id.
    query_id: Option<Uuid>,

    /// The query steps to run.
    query: Vec<QueryStep>,

    /// If set, send partial results as soon as a split / union subquery finishes.
    partial_stream: Option<PartialStream>,
}

#[derive(Debug)]
pub struct HttpError {
    status: StatusCode,
    message: String,
}

impl HttpError {
    fn new(status: StatusCode, message: String) -> HttpError {
        Self { status, message }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let body = if self.status.is_server_error() {
            error!("Internal server error: {}", self.message);
            Json(json!({"error": INTERNAL_SERVER_ERROR}))
        } else {
            error!("User error: {}", self.message);
            Json(json!({"error": self.message}))
        };

        (self.status, body).into_response()
    }
}

async fn health_check() -> impl IntoResponse {
    "OK"
}

/// Starts running a new query.
async fn query_stream(
    State(state): State<Arc<App>>,
    Json(req): Json<QueryRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, HttpError> {
    let start = Instant::now();
    METRICS.running_queries.inc();
    let _record_metrics = scopeguard::guard((), |_| {
        debug!("Recording query metrics");
        METRICS.running_queries.dec();
        METRICS.query_latency.observe(start.elapsed().as_secs_f64());
    });

    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);

    let span = span!(Level::INFO, "query", ?query_id);
    let _enter = span.enter();

    info!(?req.query, "Starting to run a new query");
    let workflow = {
        let steps = to_workflow_steps(
            &state.connectors.read().await.clone(),
            &state.views.read().await.clone(),
            req.query,
        )
        .await?;

        let optimized_steps = spawn_blocking(move || state.optimizer.optimize(steps))
            .await
            .expect("optimize thread panicked");

        Workflow::new_with_partial_stream(optimized_steps, req.partial_stream)
    };

    debug!(?workflow, "Executing workflow");

    let cancel = CancellationToken::new();
    let mut logs_stream = workflow.execute(cancel.clone()).map_err(|e| {
        HttpError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to execute workflow: {e}"),
        )
    })?;

    Ok(Sse::new(stream! {
        let _cancel_on_drop = scopeguard::guard(cancel, |cancel| {
            debug!("Cancelling query");
            cancel.cancel();
        });

        loop {
            match logs_stream.try_next().await {
                Ok(None) => break,
                Ok(log) => {
                    yield Event::default().json_data(log);
                },
                Err(e) => {
                    let msg = if let Some(e) = e.downcast_ref::<ConnectorError>() {
                        error!("Workflow connector error: {e:?}");
                        e.to_string()
                    } else {
                        error!("Workflow internal error: {e:?}");
                        INTERNAL_SERVER_ERROR.to_string()
                    };

                    yield Event::default().json_data(json!({ERROR_LOG_FIELD_NAME: msg}));
                    break;
                }
            }
        }
    }))
}

async fn explain(
    State(state): State<Arc<App>>,
    Json(req): Json<QueryRequest>,
) -> Result<Response, HttpError> {
    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);

    let span = span!(Level::INFO, "explain", ?query_id);
    let _enter = span.enter();

    let steps = to_workflow_steps(
        &state.connectors.read().await.clone(),
        &state.views.read().await.clone(),
        req.query,
    )
    .await?;

    let optimized_steps = spawn_blocking(move || state.optimizer.optimize(steps))
        .await
        .expect("optimize thread panicked");

    let optimized_workflow = Workflow::new(optimized_steps);

    Ok(format!("{optimized_workflow}").into_response())
}

async fn get_connectors(State(state): State<Arc<App>>) -> Result<Response, HttpError> {
    let guard = state.connectors.read().await;
    let mut connectors_map = BTreeMap::new();
    for (id, conn_state) in &*guard {
        let connector: &dyn Connector = &*conn_state.connector;
        connectors_map.insert(id, connector);
    }
    Ok(Json(connectors_map).into_response())
}

async fn get_connector(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<Response, HttpError> {
    let guard = state.connectors.read().await;
    let connector_state = guard.get(&id).ok_or_else(|| {
        HttpError::new(StatusCode::NOT_FOUND, format!("Connector '{id}' not found"))
    })?;
    let connector: &dyn Connector = &*connector_state.connector;
    Ok(Json(connector).into_response())
}

fn default_stats_fetch_interval() -> Duration {
    DEFAULT_STATS_FETCH_INTERVAL
}

#[derive(Deserialize)]
struct PostConnectorBody {
    /// The interval to fetch statistics (e.g. distinct count of each field), and cache in memory.
    #[serde(
        default = "default_stats_fetch_interval",
        deserialize_with = "deserialize_duration"
    )]
    stats_fetch_interval: Duration,

    /// The connector config to set.
    connector: Box<dyn Connector>,
}

async fn post_connector(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
    Json(PostConnectorBody {
        stats_fetch_interval,
        connector,
    }): Json<PostConnectorBody>,
) -> Result<(), HttpError> {
    if id == VIEWS_CONNECTOR_NAME {
        return Err(HttpError::new(
            StatusCode::BAD_REQUEST,
            format!("Cannot use the internally used id: {VIEWS_CONNECTOR_NAME}"),
        ));
    }

    let connector_state = Arc::new(ConnectorState::new_with_stats(
        connector.into(),
        stats_fetch_interval,
    ));

    let mut guard = state.connectors.write().await;
    guard.insert(id, connector_state);

    Ok(())
}

async fn delete_connector(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<(), HttpError> {
    let removed = {
        let mut guard = state.connectors.write().await;
        guard.remove(&id)
    };

    let Some(connector_state) = removed else {
        return Err(HttpError::new(
            StatusCode::NOT_FOUND,
            format!("Connector '{id}' not found"),
        ));
    };

    connector_state.close_when_last_owner().await;
    Ok(())
}

async fn get_views(State(state): State<Arc<App>>) -> Result<Response, HttpError> {
    let guard = state.views.read().await;
    let mut views_map = BTreeMap::new();
    for (id, steps) in &*guard {
        views_map.insert(id, steps);
    }
    Ok(Json(views_map).into_response())
}

async fn get_view(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<Response, HttpError> {
    let guard = state.views.read().await;
    let steps = guard
        .get(&id)
        .ok_or_else(|| HttpError::new(StatusCode::NOT_FOUND, format!("View '{id}' not found")))?;
    Ok(Json(steps).into_response())
}

async fn post_view(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
    Json(steps): Json<Vec<QueryStep>>,
) -> Result<(), HttpError> {
    let mut guard = state.views.write().await;
    guard.insert(id, steps);
    Ok(())
}

async fn delete_view(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<(), HttpError> {
    let removed = {
        let mut guard = state.views.write().await;
        guard.remove(&id)
    };

    if removed.is_none() {
        return Err(HttpError::new(
            StatusCode::NOT_FOUND,
            format!("View '{id}' not found"),
        ));
    }

    Ok(())
}

async fn metrics() -> Result<Response, HttpError> {
    let metric_families = prometheus::gather();
    let mut buffer = String::with_capacity(1024);
    let encoder = TextEncoder::new();
    encoder
        .encode_utf8(&metric_families, &mut buffer)
        .map_err(|e| {
            HttpError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to encode metrics: {e}"),
            )
        })?;
    Ok(buffer.into_response())
}

pub enum OptimizationConfig {
    NoOptimizations,
    WithOptimizations {
        dynamic_filter_max_distinct_values: u32,
    },
}

pub fn create_axum_app(config: OptimizationConfig) -> Result<Router> {
    let mut connectors = BTreeMap::new();
    connectors.insert(
        "tony".to_string(),
        Arc::new(ConnectorState::new_with_stats(
            Arc::new(QuickwitConnector::new(serde_json::from_str(
                r#"{
                    "url": "http://127.0.0.1:7280",
                    "refresh_interval": "5s"
                }"#,
            )?)),
            DEFAULT_STATS_FETCH_INTERVAL,
        )),
    );

    let optimizer = match config {
        OptimizationConfig::NoOptimizations => Optimizer::empty(),
        OptimizationConfig::WithOptimizations {
            dynamic_filter_max_distinct_values,
        } => Optimizer::with_dynamic_filtering(dynamic_filter_max_distinct_values),
    };

    let app = App::new(connectors, optimizer).context("create axum app state")?;

    Ok(Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(metrics))
        .route("/query", post(query_stream))
        .route("/explain", post(explain))
        .route("/connectors", get(get_connectors))
        .route("/connectors/:id", get(get_connector))
        .route("/connectors/:id", post(post_connector))
        .route("/connectors/:id", delete(delete_connector))
        .route("/views", get(get_views))
        .route("/views/:id", get(get_view))
        .route("/views/:id", post(post_view))
        .route("/views/:id", delete(delete_view))
        .with_state(Arc::new(app)))
}
