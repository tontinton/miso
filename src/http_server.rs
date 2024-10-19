use axum::{http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AstNode {
    /// Filter items that contain the input word (2nd arg) in the input field (1st arg).
    Term(/*field=*/ String, /*word=*/ String),
}

#[derive(Debug)]
enum QueryStep {
    /// Run a search query.
    /// In the future it will receive a Split, to be able to distribute a query to multiple
    /// workers in parallel.
    Scan(String),
}

fn ast_to_query(ast: Box<AstNode>) -> Vec<QueryStep> {
    let mut steps = Vec::new();
    match *ast {
        AstNode::Term(field, word) => steps.push(QueryStep::Scan(format!("{field}={word}"))),
    }
    steps
}

#[derive(Deserialize)]
struct PostQueryRequest {
    /// The query id to set. If not set, the server will randomly generate an id.
    query_id: Option<Uuid>,

    /// The query to run. The structure is similar to Elasticsearch's query API (JSON representing
    /// an AST).
    query: Box<AstNode>,
}

#[derive(Serialize)]
struct QueryResponse {
    query_id: Uuid,
}

/// Starts running a new query.
async fn post_query_handler(
    Json(req): Json<PostQueryRequest>,
) -> (StatusCode, Json<QueryResponse>) {
    let query_id = req.query_id.unwrap_or_else(|| Uuid::now_v7());
    info!(?req.query, "Starting to run a new query");
    let steps = ast_to_query(req.query);
    println!("{:?}", steps);
    (StatusCode::OK, Json(QueryResponse { query_id }))
}

#[must_use]
pub fn create_axum_app() -> Router {
    Router::new().route("/query", post(post_query_handler))
}
