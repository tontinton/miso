use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_recursion::async_recursion;
use async_stream::stream;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{sse::Event, IntoResponse, Response, Sse},
    routing::{delete, get, post},
    Json, Router,
};
use color_eyre::{eyre::Context, Result};
use futures_core::Stream;
use futures_util::TryStreamExt;
use prometheus::{Histogram, HistogramOpts, Registry, TextEncoder};
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, span, Level};
use uuid::Uuid;

use crate::{
    args::Args,
    connectors::{quickwit::QuickwitConnector, Connector, ConnectorState},
    humantime_utils::deserialize_duration,
    optimizations::Optimizer,
    workflow::{
        filter::FilterAst, join::Join, project::ProjectField, sort::Sort, summarize::Summarize,
        Scan, Workflow, WorkflowStep,
    },
};

const DEFAULT_STATS_FETCH_INTERVAL: Duration = Duration::from_secs(60 * 60 * 3); // 3 hours.
const INTERNAL_SERVER_ERROR: &str = "Internal server error";

pub type ConnectorsMap = BTreeMap<String, Arc<ConnectorState>>;

struct App {
    connectors: RwLock<ConnectorsMap>,
    optimizer: Arc<Optimizer>,

    /// Metrics.
    registry: Registry,
    query_latency: Histogram,
}

impl App {
    fn new(connectors: ConnectorsMap, optimizer: Optimizer) -> Result<Self> {
        let query_latency = Histogram::with_opts(HistogramOpts::new(
            "query_duration_seconds",
            "Duration of /query route",
        ))
        .context("create query_latency histogram")?;

        let registry = Registry::new();
        registry
            .register(Box::new(query_latency.clone()))
            .context("register metric")?;

        Ok(Self {
            connectors: RwLock::new(connectors),
            optimizer: Arc::new(optimizer),

            registry,
            query_latency,
        })
    }
}

#[async_recursion]
pub(crate) async fn to_workflow_steps(
    connectors: &ConnectorsMap,
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
            QueryStep::Scan(connector_name, collection) => {
                let Some(connector_state) = connectors.get(&connector_name).cloned() else {
                    return Err(HttpError::new(
                        StatusCode::NOT_FOUND,
                        format!("connector '{}' not found", connector_name),
                    ));
                };

                info!(?collection, "Checking whether collection exists");
                if !connector_state.connector.does_collection_exist(&collection) {
                    info!(?collection, "Collection doesn't exist");
                    return Err(HttpError::new(
                        StatusCode::NOT_FOUND,
                        format!("collection '{}' not found", collection),
                    ));
                }

                steps.push(WorkflowStep::Scan(
                    Scan::from_connector_state(connector_state, collection).await,
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
            QueryStep::Union(inner_steps) => {
                steps.push(WorkflowStep::Union(Workflow::new(
                    to_workflow_steps(connectors, inner_steps).await?,
                )));
            }
            QueryStep::Join(config, inner_steps) => {
                steps.push(WorkflowStep::Join(
                    config,
                    Workflow::new(to_workflow_steps(connectors, inner_steps).await?),
                ));
            }
            QueryStep::Count => {
                steps.push(WorkflowStep::Count);
            }
        }
    }

    Ok(steps)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QueryStep {
    Scan(/*connector=*/ String, /*collection=*/ String),
    Filter(FilterAst),
    Project(Vec<ProjectField>),
    Extend(Vec<ProjectField>),
    Limit(u32),
    Sort(Vec<Sort>),
    Top(Vec<Sort>, u32),
    Summarize(Summarize),
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
}

#[derive(Debug)]
pub(crate) struct HttpError {
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

struct RunOnDrop(Box<dyn Fn() + Send>);

impl Drop for RunOnDrop {
    fn drop(&mut self) {
        debug!("Run on drop");
        self.0();
    }
}

/// Starts running a new query.
async fn query_stream(
    State(state): State<Arc<App>>,
    Json(req): Json<QueryRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, HttpError> {
    let start = Instant::now();

    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);

    let span = span!(Level::INFO, "query", ?query_id);
    let _enter = span.enter();

    info!(?req.query, "Starting to run a new query");
    let workflow = {
        let steps = to_workflow_steps(&state.connectors.read().await.clone(), req.query).await?;
        Workflow::new(state.optimizer.optimize(steps).await)
    };

    debug!(?workflow, "Executing workflow");

    let (cancel_tx, cancel_rx) = watch::channel(());
    let mut logs_stream = workflow.execute(cancel_rx).map_err(|e| {
        HttpError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to execute workflow: {}", e),
        )
    })?;

    Ok(Sse::new(stream! {
        let _notify_on_drop = RunOnDrop(Box::new(move || {
            let _ = cancel_tx.send(());
            state.query_latency.observe(start.elapsed().as_secs_f64());
        }));

        loop {
            match logs_stream.try_next().await {
                Ok(None) => break,
                Ok(log) => {
                    yield Event::default().json_data(log);
                },
                Err(e) => {
                    error!("Workflow error: {e:?}");
                    yield Event::default().json_data(INTERNAL_SERVER_ERROR);
                    break;
                }
            }
        }
    }))
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

async fn get_all_connectors(State(state): State<Arc<App>>) -> Result<Response, HttpError> {
    let guard = state.connectors.read().await;
    let mut connectors_map = BTreeMap::new();
    for (id, conn_state) in &*guard {
        let connector: &dyn Connector = &*conn_state.connector;
        connectors_map.insert(id, connector);
    }
    Ok(Json(connectors_map).into_response())
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

async fn metrics(State(state): State<Arc<App>>) -> Result<Response, HttpError> {
    let metric_families = state.registry.gather();
    let mut buffer = String::new();
    let encoder = TextEncoder::new();
    encoder
        .encode_utf8(&metric_families, &mut buffer)
        .map_err(|e| {
            HttpError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to encode metrics: {}", e),
            )
        })?;
    Ok(buffer.into_response())
}

pub fn create_axum_app(args: &Args) -> Result<Router> {
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

    let optimizer = if args.no_optimizations {
        Optimizer::empty()
    } else {
        Optimizer::with_dynamic_filtering(args.dynamic_filter_max_distinct_values)
    };

    let app = App::new(connectors, optimizer).context("create axum app state")?;

    Ok(Router::new()
        .route("/metrics", get(metrics))
        .route("/query", post(query_stream))
        .route("/connectors", get(get_all_connectors))
        .route("/connectors/:id", get(get_connector))
        .route("/connectors/:id", post(post_connector))
        .route("/connectors/:id", delete(delete_connector))
        .with_state(Arc::new(app)))
}
