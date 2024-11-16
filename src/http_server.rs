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
use tracing::{error, info, span, Level};
use uuid::Uuid;

use crate::{
    connector::{Connector, Predicate},
    quickwit_connector::QuickwitConnector,
    workflow::{filter::FilterAst, project::ProjectField, sort::Sort, Workflow, WorkflowStep},
};

struct State {
    connectors: HashMap<String, Arc<dyn Connector>>,
}

type SharedState = Arc<RwLock<State>>;

async fn to_workflow(
    state: SharedState,
    query_steps: Vec<QueryStep>,
) -> Result<Workflow, HttpError> {
    // The steps to run after all predicate pushdowns.
    let mut steps = Vec::new();

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

    let mut collection = String::new();
    let mut connector = None;
    let mut handle = None;
    let mut predicated_steps = 1;

    // Try to pushdown steps.
    for step in query_steps.iter().skip(predicated_steps) {
        let next_handle = handle.take().expect("query handle to exist");

        let predicate = match step {
            QueryStep::Scan(..) if connector.is_some() => {
                return Err(HttpError::new(
                    StatusCode::BAD_REQUEST,
                    "cannot scan inside a scan".to_string(),
                ));
            }
            QueryStep::Scan(connector_name, collection_name) => {
                let Some(scan_connector) =
                    state.read().await.connectors.get(connector_name).cloned()
                else {
                    return Err(HttpError::new(
                        StatusCode::NOT_FOUND,
                        format!("connector '{}' not found", connector_name),
                    ));
                };

                info!(?collection_name, "Checking whether collection exists");
                if !scan_connector.does_collection_exist(collection_name).await {
                    info!(?collection_name, "Collection doesn't exist");
                    return Err(HttpError::new(
                        StatusCode::NOT_FOUND,
                        format!("collection '{}' not found", collection_name),
                    ));
                }

                let next_handle = scan_connector.get_handle();
                connector = Some(scan_connector);
                collection = collection_name.clone();

                // Workflow step will be created right after this loop.
                Predicate::Pushdown(next_handle)
            }
            QueryStep::Filter(ast) => connector
                .clone()
                .expect("scan should be before filter")
                .apply_filter(ast, next_handle),
            QueryStep::Limit(max) => connector
                .clone()
                .expect("scan should be before limit")
                .apply_limit(*max, next_handle),
            // TODO: add project predicate pushdown.
            _ => Predicate::None(next_handle),
        };

        match predicate {
            Predicate::Pushdown(next_handle) => {
                predicated_steps += 1;
                handle = Some(next_handle);
            }
            Predicate::None(final_handle) => {
                handle = Some(final_handle);
                break;
            }
        }
    }

    steps.push(WorkflowStep::Scan {
        collection,
        connector: connector.clone().unwrap(),
        splits: connector.clone().unwrap().get_splits().await,
        handle: handle.expect("query handle to exist").into(),
    });

    // Add leftover steps.
    for step in query_steps.into_iter().skip(predicated_steps) {
        match step {
            QueryStep::Scan(..) => {
                return Err(HttpError::new(
                    StatusCode::BAD_REQUEST,
                    "cannot scan inside a scan".to_string(),
                ));
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
        }
    }

    Ok(Workflow::new(steps))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueryStep {
    Scan(/*connector=*/ String, /*collection=*/ String),
    Filter(FilterAst),
    Project(Vec<ProjectField>),
    Limit(u64),
    Sort(Sort),
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

    info!(?workflow, "Executing workflow");
    if let Err(err) = workflow.execute().await {
        error!(?err, "Failed to execute workflow: {}", err);
        return Err(HttpError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to execute workflow".to_string(),
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
    let state = Arc::new(RwLock::new(State { connectors }));

    Ok(Router::new()
        .route("/query", post(post_query_handler))
        .layer(Extension(state)))
}
