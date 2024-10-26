use axum::{http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use crate::ast::{ast_to_vrl, predicate_pushdown, QueryAst};
use crate::{
    connector::{Connector, Split},
    elasticsearch::ElasticsearchConnector,
};

#[derive(Debug, Serialize, Deserialize)]
enum ActivityItem {
    /// Run a search query.
    Scan(Split),
}

#[derive(Debug, Serialize, Deserialize)]
struct Activity {
    item: ActivityItem,

    // Run a VRL script on the resulting data.
    #[serde(skip_serializing_if = "Option::is_none")]
    post_script: Option<String>,
}

#[derive(Debug)]
struct Workflow {
    activities: Vec<Activity>,
}

fn to_workflow(ast: &mut QueryAst, connector: &dyn Connector) -> Workflow {
    predicate_pushdown(ast, connector);

    // Whatever is left in the AST, the connector wasn't able to predicate pushdown.
    // We are responsible to execute the AST now.
    let script = ast_to_vrl(ast);

    Workflow {
        activities: connector
            .get_splits()
            .into_iter()
            .map(|split| Activity {
                item: ActivityItem::Scan(split),
                post_script: Some(script.clone()),
            })
            .collect(),
    }
}

#[derive(Deserialize)]
struct PostQueryRequest {
    /// The query id to set. If not set, the server will randomly generate an id.
    query_id: Option<Uuid>,

    /// The query to run. The structure is similar to Elasticsearch's query API (JSON representing
    /// an AST).
    query: QueryAst,
}

#[derive(Serialize)]
struct QueryResponse {
    query_id: Uuid,
}

/// Starts running a new query.
async fn post_query_handler(
    Json(mut req): Json<PostQueryRequest>,
) -> (StatusCode, Json<QueryResponse>) {
    let query_id = req.query_id.unwrap_or_else(|| Uuid::now_v7());
    info!(?req.query, "Starting to run a new query");
    let connector = ElasticsearchConnector {};
    let workflow = to_workflow(&mut req.query, &connector);
    println!("{:?}", workflow);
    (StatusCode::OK, Json(QueryResponse { query_id }))
}

#[must_use]
pub fn create_axum_app() -> Router {
    Router::new().route("/query", post(post_query_handler))
}
