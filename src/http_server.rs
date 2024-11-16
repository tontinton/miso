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
    connector::Connector,
    quickwit_connector::QuickwitConnector,
    workflow::{filter::FilterAst, project::ProjectField, sort::Sort, Workflow, WorkflowStep},
};

struct State {
    connectors: HashMap<String, Arc<dyn Connector>>,
}

type SharedState = Arc<RwLock<State>>;

async fn to_workflow(query_steps: Vec<QueryStep>, connector: &dyn Connector) -> Workflow {
    // The steps to run after all predicate pushdowns.
    let mut steps = Vec::new();

    let mut handle = Some(connector.get_handle());
    let mut num_predicated = 0;

    // Try to pushdown steps.
    for step in &query_steps {
        let current_handle = handle.take().expect("query handle to exist");

        match step {
            QueryStep::Filter(ast) => {
                handle = connector.apply_filter(ast, current_handle);
            }
            QueryStep::Limit(max) => {
                handle = connector.apply_limit(*max, current_handle);
            }
            // TODO: add project predicate pushdown.
            _ => {
                break;
            }
        }

        if handle.is_none() {
            break;
        }
        num_predicated += 1;
    }

    steps.push(WorkflowStep::Scan {
        splits: connector.get_splits().await,
        handle: handle
            .map(|x| x.into())
            .unwrap_or_else(|| connector.get_handle().into()),
    });

    // Add leftover steps.
    for step in query_steps.into_iter().skip(num_predicated) {
        match step {
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

    Workflow::new(steps)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueryStep {
    Filter(FilterAst),
    Project(Vec<ProjectField>),
    Limit(u64),
    Sort(Sort),
}

#[derive(Deserialize)]
struct PostQueryRequest {
    /// The connector to query from.
    connector: String,

    /// The collection inside the connector to query from (index in quickwit).
    collection: String,

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

    if req.query.is_empty() {
        return Err(HttpError::new(
            StatusCode::BAD_REQUEST,
            "empty query".to_string(),
        ));
    }

    let Some(connector) = state.read().await.connectors.get(&req.connector).cloned() else {
        return Err(HttpError::new(
            StatusCode::NOT_FOUND,
            format!("connector '{}' not found", req.connector),
        ));
    };

    info!(?req.collection, "Checking whether collection exists");
    if !connector.does_collection_exist(&req.collection).await {
        info!(?req.collection, "Collection doesn't exist");
        return Err(HttpError::new(
            StatusCode::NOT_FOUND,
            format!("collection '{}' not found", req.collection),
        ));
    }

    info!(?req.query, "Starting to run a new query");
    let workflow = to_workflow(req.query, &*connector).await;

    info!(?workflow, "Executing workflow");
    if let Err(err) = workflow.execute(connector, &req.collection).await {
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
