use std::{collections::HashMap, sync::Arc};

use async_recursion::async_recursion;
use async_stream::stream;
use axum::{
    http::StatusCode,
    response::{sse::Event, IntoResponse, Response, Sse},
    routing::post,
    Extension, Json, Router,
};
use color_eyre::{eyre::Context, Result};
use futures_core::Stream;
use serde::Deserialize;
use serde_json::json;
use tokio::{
    spawn,
    sync::{mpsc, watch, RwLock},
};
use tracing::{debug, error, info, span, Level};
use uuid::Uuid;

use crate::{
    connector::Connector,
    log::Log,
    optimizations::Optimizer,
    quickwit_connector::QuickwitConnector,
    workflow::{
        filter::FilterAst, join::Join, project::ProjectField, sort::Sort, summarize::Summarize,
        Scan, Workflow, WorkflowStep,
    },
};

const INTERNAL_SERVER_ERROR: &str = "Internal server error";

struct State {
    connectors: HashMap<String, Arc<dyn Connector>>,
    optimizer: Arc<Optimizer>,
}

type SharedState = Arc<RwLock<State>>;

#[async_recursion]
async fn to_workflow_steps(
    state: SharedState,
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
                    "cannot scan inside a scan".to_string(),
                ));
            }
            QueryStep::Scan(connector_name, collection) => {
                let Some(connector) = state.read().await.connectors.get(&connector_name).cloned()
                else {
                    return Err(HttpError::new(
                        StatusCode::NOT_FOUND,
                        format!("connector '{}' not found", connector_name),
                    ));
                };

                info!(?collection, "Checking whether collection exists");
                if !connector.does_collection_exist(&collection).await {
                    info!(?collection, "Collection doesn't exist");
                    return Err(HttpError::new(
                        StatusCode::NOT_FOUND,
                        format!("collection '{}' not found", collection),
                    ));
                }

                steps.push(WorkflowStep::Scan(Scan {
                    collection,
                    connector: connector.clone(),
                    splits: connector.clone().get_splits().await,
                    handle: connector.get_handle().into(),
                }));
            }
            QueryStep::Filter(ast) => {
                steps.push(WorkflowStep::Filter(ast));
            }
            QueryStep::Project(fields) => {
                steps.push(WorkflowStep::Project(fields));
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
                    to_workflow_steps(state.clone(), inner_steps).await?,
                )));
            }
            QueryStep::Join((config, inner_steps)) => {
                steps.push(WorkflowStep::Join((
                    config,
                    Workflow::new(to_workflow_steps(state.clone(), inner_steps).await?),
                )));
            }
            QueryStep::Count => {
                if i != num_steps - 1 {
                    return Err(HttpError::new(
                        StatusCode::BAD_REQUEST,
                        "count must be the last step".to_string(),
                    ));
                }
                steps.push(WorkflowStep::Count);
            }
        }
    }

    Ok(steps)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueryStep {
    Scan(/*connector=*/ String, /*collection=*/ String),
    Filter(FilterAst),
    Project(Vec<ProjectField>),
    Limit(u32),
    Sort(Vec<Sort>),
    Top(Vec<Sort>, u32),
    Summarize(Summarize),
    Union(Vec<QueryStep>),
    Join((Join, Vec<QueryStep>)),
    Count,
}

#[derive(Deserialize)]
struct QueryRequest {
    /// The query id to set. If not set, the server will randomly generate an id.
    query_id: Option<Uuid>,

    /// The query steps to run.
    query: Vec<QueryStep>,
}

struct HttpError {
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
        let steps = to_workflow_steps(state.clone(), req.query).await?;
        let optimizer = state.read().await.optimizer.clone();
        Workflow::new(optimizer.optimize(steps).await)
    };

    debug!(?workflow, "Executing workflow");

    let (tx, mut rx) = mpsc::channel(1);
    let send_log_to_sse = move |log: Log| {
        let tx = tx.clone();
        async move {
            tx.send(log).await.context("log to SSE tx")?;
            Ok(())
        }
    };

    let (cancel_tx, cancel_rx) = watch::channel(());

    let mut workflow_task = spawn(workflow.execute(send_log_to_sse, cancel_rx));

    Ok(Sse::new(stream! {
        let _notify_on_drop = NotifyOnDrop(cancel_tx);

        loop {
            tokio::select! {
                log = rx.recv() => {
                    if let Some(log) = log {
                        yield Event::default().json_data(log);
                    } else {
                        break;
                    }
                }
                result = &mut workflow_task => {
                    match result {
                        Ok(Ok(())) => {
                            // Finish reading whatever is left in the channel.
                            while let Some(log) = rx.recv().await {
                                yield Event::default().json_data(log);
                            }
                            break;
                        },
                        Ok(Err(e)) => {
                            error!("Workflow error: {e:?}");
                            yield Event::default().json_data(INTERNAL_SERVER_ERROR);
                            break;
                        }
                        Err(e) => {
                            error!("Task join error: {e:?}");
                            yield Event::default().json_data(INTERNAL_SERVER_ERROR);
                            break;
                        }
                    }
                }
            }
        }
    }))
}

pub fn create_axum_app() -> Result<Router> {
    let mut connectors = HashMap::new();
    connectors.insert(
        "tony".to_string(),
        Arc::new(QuickwitConnector::new(serde_json::from_str(
            r#"{
                "url": "http://127.0.0.1:7280",
                "refresh_interval": "5s"
            }"#,
        )?)) as Arc<dyn Connector>,
    );
    let state = Arc::new(RwLock::new(State {
        connectors,
        optimizer: Arc::new(Optimizer::default()),
    }));

    Ok(Router::new()
        .route("/query", post(query_stream))
        .layer(Extension(state)))
}
