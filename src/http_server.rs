use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_recursion::async_recursion;
use async_stream::stream;
use axum::{
    http::StatusCode,
    response::{sse::Event, IntoResponse, Response, Sse},
    routing::post,
    Extension, Json, Router,
};
use color_eyre::Result;
use futures_core::Stream;
use futures_util::TryStreamExt;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::{watch, RwLock};
use tracing::{debug, error, info, span, Level};
use uuid::Uuid;

use crate::{
    args::Args,
    connector::ConnectorState,
    optimizations::Optimizer,
    quickwit_connector::QuickwitConnector,
    workflow::{
        filter::FilterAst, join::Join, project::ProjectField, sort::Sort, summarize::Summarize,
        Scan, Workflow, WorkflowStep,
    },
};

// Should be a configuration in the future.
const UPDATE_STATS_INTERVAL: Duration = Duration::from_secs(60 * 60 * 3); // 3 hours.

const INTERNAL_SERVER_ERROR: &str = "Internal server error";

pub type ConnectorsMap = BTreeMap<String, Arc<ConnectorState>>;

struct State {
    connectors: ConnectorsMap,
    optimizer: Arc<Optimizer>,
}

type SharedState = Arc<RwLock<State>>;

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

struct NotifyOnDrop(watch::Sender<()>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        debug!("Notify on drop");
        let _ = self.0.send(());
    }
}

/// Starts running a new query.
async fn query_stream(
    Extension(state): Extension<SharedState>,
    Json(req): Json<QueryRequest>,
) -> Result<Sse<impl Stream<Item = Result<Event, axum::Error>>>, HttpError> {
    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);

    let span = span!(Level::INFO, "query", ?query_id);
    let _enter = span.enter();

    info!(?req.query, "Starting to run a new query");
    let workflow = {
        let steps = to_workflow_steps(&state.read().await.connectors.clone(), req.query).await?;
        let optimizer = state.read().await.optimizer.clone();
        Workflow::new(optimizer.optimize(steps).await)
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
        let _notify_on_drop = NotifyOnDrop(cancel_tx);

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
            Some(UPDATE_STATS_INTERVAL),
        )),
    );

    let optimizer = Arc::new(if args.no_optimizations {
        Optimizer::empty()
    } else {
        Optimizer::with_dynamic_filtering(args.dynamic_filter_max_distinct_values)
    });

    let state = Arc::new(RwLock::new(State {
        connectors,
        optimizer,
    }));

    Ok(Router::new()
        .route("/query", post(query_stream))
        .layer(Extension(state)))
}
