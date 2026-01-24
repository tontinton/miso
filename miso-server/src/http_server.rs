use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_stream::stream;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response, Sse, sse::Event},
};
use color_eyre::{
    Result,
    eyre::{Context, eyre},
};
use futures_util::{Stream, TryStreamExt};
use miso_common::{
    metrics::{ERROR_CONNECTOR, ERROR_INTERNAL, METRICS},
    run_at_interval::run_at_interval,
    shutdown_future::ShutdownFuture,
};
use miso_connectors::{Connector, ConnectorError, ConnectorState};
use miso_kql::{ParseError, parse};
use miso_optimizations::Optimizer;
use miso_workflow::{Workflow, partial_stream::PartialStream};
use prometheus::TextEncoder;
use serde::Deserialize;
use serde_json::json;
use time::OffsetDateTime;
use tokio::{sync::RwLock, task::spawn_blocking};
use tokio_util::sync::CancellationToken;
use tower_http::trace::TraceLayer;
use tracing::{Instrument, Level, Span, debug, error, info, span};
use utoipa::OpenApi;
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::{
    VIEWS_CONNECTOR_NAME, ViewsMap,
    config::{ConnectorConfig, ConnectorsMap, load_config},
    query_status::{QUERY_ID_FIELD, QueryStatus, QueryStatusHandle, QueryStatusWriter},
    query_to_workflow::to_workflow_steps,
};

const TOKIO_METRICS_UPDATE_INTERVAL: Duration = Duration::from_secs(1);
const INTERNAL_SERVER_ERROR: &str = "Internal server error";
const ERROR_LOG_FIELD_NAME: &str = "_error";

struct App {
    connectors: RwLock<ConnectorsMap>,
    optimizer: Arc<Optimizer>,
    views: RwLock<ViewsMap>,
    query_status_writer: Option<QueryStatusWriter>,
    _tokio_metrics_task: ShutdownFuture,
}

impl App {
    fn new(
        connectors: ConnectorsMap,
        optimizer: Optimizer,
        query_status_writer: Option<QueryStatusWriter>,
    ) -> Result<Self> {
        let tokio_metrics_task =
            ShutdownFuture::new(collect_tokio_metrics(), "Tokio metrics collector");

        Ok(Self {
            connectors: RwLock::new(connectors),
            optimizer: Arc::new(optimizer),
            views: RwLock::new(BTreeMap::new()),
            query_status_writer,
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

#[derive(Deserialize, utoipa::ToSchema)]
struct QueryRequest {
    /// The query id to set. If not set, the server will randomly generate an id.
    #[schema(value_type = Option<String>, format = "uuid")]
    query_id: Option<Uuid>,

    /// The KQL query to run.
    query: String,

    /// If set, send partial results as soon as a split / union subquery finishes.
    #[schema(value_type = Option<Object>)]
    partial_stream: Option<PartialStream>,
}

#[derive(Debug)]
pub struct HttpError {
    status: StatusCode,
    message: serde_json::Value,
    query_id: Option<String>,
}

impl HttpError {
    pub fn from_string(status: StatusCode, message: String) -> HttpError {
        Self::new(status, serde_json::Value::String(message))
    }

    pub fn new(status: StatusCode, message: serde_json::Value) -> HttpError {
        Self {
            status,
            message,
            query_id: None,
        }
    }

    fn with_query_id(mut self, query_id: String) -> Self {
        self.query_id = Some(query_id);
        self
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        if let Some(query_id) = self.query_id {
            if self.status.is_server_error() {
                error!(%query_id, "Internal server error: {}", self.message);
            } else {
                error!(%query_id, "User error: {}", self.message);
            }
        } else if self.status.is_server_error() {
            error!("Internal server error: {}", self.message);
        } else {
            error!("User error: {}", self.message);
        }

        let body = if self.status.is_server_error() {
            Json(json!({ERROR_LOG_FIELD_NAME: INTERNAL_SERVER_ERROR}))
        } else {
            Json(json!({ERROR_LOG_FIELD_NAME: self.message}))
        };

        (self.status, body).into_response()
    }
}

impl From<Vec<ParseError>> for HttpError {
    fn from(errors: Vec<ParseError>) -> Self {
        HttpError::new(StatusCode::BAD_REQUEST, json!({"parse": errors}))
    }
}

#[utoipa::path(get, path = "/health")]
async fn health_check() -> impl IntoResponse {
    "OK"
}

async fn build_query_workflow(
    state: Arc<App>,
    req: QueryRequest,
    query_id: Uuid,
    non_optimized_output: &mut Option<String>,
) -> Result<Workflow, HttpError> {
    let query_id = query_id.to_string();

    let ast = spawn_blocking(move || Span::current().in_scope(|| parse(&req.query)))
        .await
        .expect("parse thread panicked")
        .map_err(|e| HttpError::from(e).with_query_id(query_id.clone()))?;

    let steps = to_workflow_steps(
        &state.connectors.read().await.clone(),
        &state.views.read().await.clone(),
        ast,
    )
    .map_err(|e| e.with_query_id(query_id))?;

    if let Some(non_optimized) = non_optimized_output {
        *non_optimized = format!(
            "{}",
            Workflow::new_with_partial_stream(steps.clone(), req.partial_stream.clone())
        );
    }

    let optimized_steps =
        spawn_blocking(move || Span::current().in_scope(|| state.optimizer.optimize(steps)))
            .await
            .expect("optimize thread panicked");

    Ok(Workflow::new_with_partial_stream(
        optimized_steps,
        req.partial_stream,
    ))
}

struct QueryStreamSetup {
    workflow_str: String,
    non_optimized_workflow_str: String,
    status_handle: Option<QueryStatusHandle>,
    cancel: CancellationToken,
    logs_stream: miso_workflow_types::log::LogTryStream,
}

// span.enter() doesn't work with async yielding, for more information read the Span::enter() docs.
async fn query_stream_setup(
    state: Arc<App>,
    req: QueryRequest,
    query_id: Uuid,
    query_text: String,
    start: OffsetDateTime,
) -> Result<QueryStreamSetup, HttpError> {
    let status_handle = match state.query_status_writer.as_ref() {
        Some(writer) => Some(writer.start(query_id, query_text.clone()).await),
        None => None,
    };

    info!("Building query workflow");
    let mut non_optimized = Some(String::new());
    let workflow = build_query_workflow(state, req, query_id, &mut non_optimized).await?;

    let non_optimized_workflow_str = non_optimized.unwrap();
    let workflow_str = format!("{workflow}");

    info!(
        %start,
        query = query_text,
        non_optimized = non_optimized_workflow_str,
        optimized = workflow_str,
        "Query started"
    );

    if let Some(ref handle) = status_handle {
        handle.update(QueryStatus::Running).await;
    }

    let cancel = CancellationToken::new();
    let logs_stream = workflow.execute(cancel.clone()).map_err(|e| {
        HttpError::from_string(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to execute workflow: {e}"),
        )
        .with_query_id(query_id.to_string())
    })?;

    Ok(QueryStreamSetup {
        workflow_str,
        non_optimized_workflow_str,
        status_handle,
        cancel,
        logs_stream,
    })
}

/// Starts running a new query.
#[utoipa::path(post, path = "/query")]
async fn query_stream(
    State(state): State<Arc<App>>,
    Json(req): Json<QueryRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, HttpError> {
    let start = OffsetDateTime::now_utc();
    METRICS.running_queries.inc();
    let record_metrics = scopeguard::guard(start, |start| {
        debug!("Recording query metrics");
        METRICS.running_queries.dec();
        let end = OffsetDateTime::now_utc();
        METRICS
            .query_latency
            .observe((end - start).as_seconds_f64());
    });

    // Must be called the same as the const QUERY_ID_FIELD so it will be in all logs.
    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);
    let query_text = req.query.clone();

    let span = span!(parent: None, Level::INFO, "query", ?query_id, query=req.query);
    let stream_span = span.clone();

    let setup = query_stream_setup(state, req, query_id, query_text.clone(), start)
        .instrument(span)
        .await?;

    let QueryStreamSetup {
        workflow_str,
        non_optimized_workflow_str,
        status_handle,
        cancel,
        mut logs_stream,
    } = setup;

    Ok(Sse::new(stream! {
        // Extend lifetime.
        let _record_metrics = record_metrics;
        let span = stream_span;
        let _span = span.enter();

        let final_status = Arc::new(Mutex::new(QueryStatus::Cancelled));

        // Because this guard is created after the span above, it will be dropped before,
        // so no need to move the span inside.
        let _query_done_guard = scopeguard::guard(
            (final_status.clone(), status_handle.clone()),
            move |(final_status, status_handle)| {
                cancel.cancel();

                let end = OffsetDateTime::now_utc();
                let duration = end - start;
                let status = final_status.lock().unwrap().clone();

                info!(
                    %status,
                    %start,
                    %end,
                    duration_secs = duration.as_seconds_f64(),
                    duration_str = %duration,
                    query = query_text,
                    non_optimized = non_optimized_workflow_str,
                    workflow = workflow_str,
                    "Query completed"
                );

                if let Some(handle) = status_handle {
                    let status = final_status.lock().unwrap().clone();
                    tokio::spawn(handle.finish(status, end).in_current_span());
                }
            });

        loop {
            match logs_stream.try_next().await {
                Ok(None) => break,
                Ok(log) => {
                    yield Event::default().json_data(log);
                }
                Err(e) => {
                    let (status, msg, label) = if let Some(e) = e.downcast_ref::<ConnectorError>() {
                        error!("Workflow connector error: {e:?}");
                        let msg = e.to_string();
                        (QueryStatus::ConnectorError(msg.clone()), msg, ERROR_CONNECTOR)
                    } else {
                        error!("Workflow internal error: {e:?}");
                        let msg = INTERNAL_SERVER_ERROR.to_string();
                        (QueryStatus::InternalError(e.to_string()), msg, ERROR_INTERNAL)
                    };

                    *final_status.lock().unwrap() = status;
                    METRICS.query_errors_total.with_label_values(&[label]).inc();

                    yield Event::default().json_data(json!({ERROR_LOG_FIELD_NAME: msg}));
                    break;
                }
            }
        }

        {
            let mut guard = final_status.lock().unwrap();
            if *guard == QueryStatus::Cancelled {
                *guard = QueryStatus::Success;
            }
        }
    }))
}

#[utoipa::path(post, path = "/explain")]
async fn explain(
    State(state): State<Arc<App>>,
    Json(req): Json<QueryRequest>,
) -> Result<Response, HttpError> {
    // Must be called the same as the const QUERY_ID_FIELD so it will be in all logs.
    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);

    let span = span!(parent: None, Level::INFO, "explain", ?query_id);
    let _enter = span.enter();

    let workflow = build_query_workflow(state.clone(), req, query_id, &mut None).await?;

    Ok(format!("{workflow}").into_response())
}

#[utoipa::path(get, path = "/connectors")]
async fn get_connectors(State(state): State<Arc<App>>) -> Result<Response, HttpError> {
    let guard = state.connectors.read().await;
    let mut connectors_map = BTreeMap::new();
    for (id, conn_state) in &*guard {
        let connector: &dyn Connector = &*conn_state.connector;
        connectors_map.insert(id, connector);
    }
    Ok(Json(connectors_map).into_response())
}

#[utoipa::path(get, path = "/connectors/{id}")]
async fn get_connector(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<Response, HttpError> {
    let guard = state.connectors.read().await;
    let connector_state = guard.get(&id).ok_or_else(|| {
        HttpError::from_string(StatusCode::NOT_FOUND, format!("Connector '{id}' not found"))
    })?;
    let connector: &dyn Connector = &*connector_state.connector;
    Ok(Json(connector).into_response())
}

#[utoipa::path(post, path = "/connectors/{id}", request_body(content = inline(Object)))]
async fn post_connector(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
    Json(ConnectorConfig {
        stats_fetch_interval,
        connector,
    }): Json<ConnectorConfig>,
) -> Result<(), HttpError> {
    if id == VIEWS_CONNECTOR_NAME {
        return Err(HttpError::from_string(
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

#[utoipa::path(delete, path = "/connectors/{id}")]
async fn delete_connector(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<(), HttpError> {
    let removed = {
        let mut guard = state.connectors.write().await;
        guard.remove(&id)
    };

    let Some(connector_state) = removed else {
        return Err(HttpError::from_string(
            StatusCode::NOT_FOUND,
            format!("Connector '{id}' not found"),
        ));
    };

    connector_state.close_when_last_owner().await;
    Ok(())
}

#[utoipa::path(get, path = "/views")]
async fn get_views(State(state): State<Arc<App>>) -> Result<Response, HttpError> {
    let guard = state.views.read().await;
    let mut views_map = BTreeMap::new();
    for (id, steps) in &*guard {
        views_map.insert(id, steps);
    }
    Ok(Json(views_map).into_response())
}

#[utoipa::path(get, path = "/views/{id}")]
async fn get_view(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<Response, HttpError> {
    let guard = state.views.read().await;
    let steps = guard.get(&id).ok_or_else(|| {
        HttpError::from_string(StatusCode::NOT_FOUND, format!("View '{id}' not found"))
    })?;
    Ok(Json(steps).into_response())
}

#[utoipa::path(post, path = "/views/{id}")]
async fn post_view(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
    Json(query): Json<String>,
) -> Result<(), HttpError> {
    let steps = spawn_blocking(move || parse(&query))
        .await
        .expect("parse thread panicked")?;

    let mut guard = state.views.write().await;
    guard.insert(id, steps);
    Ok(())
}

#[utoipa::path(delete, path = "/views/{id}")]
async fn delete_view(
    State(state): State<Arc<App>>,
    Path(id): Path<String>,
) -> Result<(), HttpError> {
    let removed = {
        let mut guard = state.views.write().await;
        guard.remove(&id)
    };

    if removed.is_none() {
        return Err(HttpError::from_string(
            StatusCode::NOT_FOUND,
            format!("View '{id}' not found"),
        ));
    }

    Ok(())
}

#[utoipa::path(get, path = "/metrics")]
async fn metrics() -> Result<Response, HttpError> {
    let metric_families = prometheus::gather();
    let mut buffer = String::with_capacity(1024);
    let encoder = TextEncoder::new();
    encoder
        .encode_utf8(&metric_families, &mut buffer)
        .map_err(|e| {
            HttpError::from_string(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to encode metrics: {e}"),
            )
        })?;
    Ok(buffer.into_response())
}

#[derive(OpenApi)]
#[openapi(info(title = "Miso API", version = "0.1.0"))]
struct ApiDoc;

pub enum OptimizationConfig {
    NoOptimizations,
    WithOptimizations {
        dynamic_filter_max_distinct_values: u64,
    },
}

pub fn create_app(config: OptimizationConfig, config_path: Option<&str>) -> Result<Router> {
    let (connectors, query_status_config) = match config_path {
        Some(path) => load_config(path)?,
        None => (BTreeMap::new(), None),
    };

    let query_status_writer = if let Some(cfg) = query_status_config {
        let connector = connectors.get(&cfg.connector_name).ok_or_else(|| {
            eyre!(
                "query_status_collection references unknown connector '{}'. Available connectors: {:?}",
                cfg.connector_name,
                connectors.keys().collect::<Vec<_>>()
            )
        })?;

        let sink = connector
            .connector
            .create_updatable_sink(&cfg.collection_name, QUERY_ID_FIELD)
            .ok_or_else(|| {
                eyre!(
                    "Connector '{}' does not support upsert operations required for query status tracking",
                    cfg.connector_name
                )
            })?;
        Some(QueryStatusWriter::new(sink))
    } else {
        None
    };

    let optimizer = match config {
        OptimizationConfig::NoOptimizations => Optimizer::empty(),
        OptimizationConfig::WithOptimizations {
            dynamic_filter_max_distinct_values,
        } => Optimizer::with_dynamic_filtering(dynamic_filter_max_distinct_values),
    };

    let app =
        App::new(connectors, optimizer, query_status_writer).context("create axum app state")?;

    let (router, api) = OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(health_check))
        .routes(routes!(metrics))
        .routes(routes!(query_stream))
        .routes(routes!(explain))
        .routes(routes!(get_connectors))
        .routes(routes!(get_connector, post_connector, delete_connector))
        .routes(routes!(get_views))
        .routes(routes!(get_view, post_view, delete_view))
        .with_state(Arc::new(app))
        .split_for_parts();

    let router = router
        .merge(SwaggerUi::new("/").url("/openapi.json", api))
        .layer(TraceLayer::new_for_http());

    Ok(router)
}
