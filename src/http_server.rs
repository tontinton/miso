use std::{collections::HashMap, sync::Arc};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Extension, Json, Router,
};
use color_eyre::Result;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::RwLock;
use tracing::{debug, error, info, span, Level};
use uuid::Uuid;

use crate::{
    connector::Connector,
    optimizations::Optimizer,
    quickwit_connector::QuickwitConnector,
    workflow::{
        filter::FilterAst, project::ProjectField, sort::Sort, Scan, Workflow, WorkflowStep,
    },
};

struct State {
    connectors: HashMap<String, Arc<dyn Connector>>,
    optimizer: Arc<Optimizer>,
}

type SharedState = Arc<RwLock<State>>;

async fn to_workflow(
    state: SharedState,
    query_steps: Vec<QueryStep>,
) -> Result<Workflow, HttpError> {
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

    let optimizer = state.read().await.optimizer.clone();
    Ok(Workflow::new(optimizer.optimize(steps)))
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
    Count,
}

#[derive(Deserialize)]
struct PostQueryRequest {
    /// The query id to set. If not set, the server will randomly generate an id.
    query_id: Option<Uuid>,

    /// The query steps to run.
    query: Vec<QueryStep>,
}

#[derive(Serialize)]
struct QueryResponse {
    query_id: Uuid,
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
        if self.status.is_server_error() {
            error!("Failed internal response: {}", self.message);
        }

        let body = Json(json!({
            "error": self.message,
        }));
        (self.status, body).into_response()
    }
}

/// Starts running a new query.
async fn post_query_handler(
    Extension(state): Extension<SharedState>,
    Json(req): Json<PostQueryRequest>,
) -> Result<Json<QueryResponse>, HttpError> {
    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);

    let span = span!(Level::INFO, "query", ?query_id);
    let _enter = span.enter();

    info!(?req.query, "Starting to run a new query");
    let workflow = to_workflow(state, req.query).await?;

    debug!(?workflow, "Executing workflow");
    if let Err(err) = workflow.execute().await {
        return Err(HttpError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to execute workflow: {}", err),
        ));
    }

    Ok(Json(QueryResponse { query_id }))
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
        .route("/query", post(post_query_handler))
        .layer(Extension(state)))
}
