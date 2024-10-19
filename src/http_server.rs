use axum::{http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;

use crate::ast::QueryAst;
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

fn predicate_pushdown(ast: &mut QueryAst, connector: &dyn Connector) {
    if let Some(filter) = &mut ast.filter {
        if let Some(and) = &mut filter.and {
            and.retain(|item| !connector.apply_filter_and(item));
        }
        if let Some(or) = &filter.or {
            if connector.apply_filter_or(or) {
                filter.or = None;
            }
        }
    }
}

fn to_workflow(ast: &mut QueryAst, connector: &dyn Connector) -> Workflow {
    let mut activities = Vec::new();
    predicate_pushdown(ast, connector);

    let splits = connector.get_splits();
    for split in splits {
        activities.push(Activity {
            item: ActivityItem::Scan(split),
            post_script: None,
        });
    }

    Workflow { activities }
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
