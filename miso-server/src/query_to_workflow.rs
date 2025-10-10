use axum::http::StatusCode;
use hashbrown::HashMap;
use miso_workflow::{Workflow, WorkflowStep, scan::Scan};
use miso_workflow_types::{expr::Expr, query::QueryStep, summarize::Summarize};
use tracing::info;

use crate::{
    VIEWS_CONNECTOR_NAME, ViewsMap,
    http_server::{ConnectorsMap, HttpError},
};

pub fn to_workflow_steps(
    connectors: &ConnectorsMap,
    views: &ViewsMap,
    query_steps: Vec<QueryStep>,
) -> Result<Vec<WorkflowStep>, HttpError> {
    if query_steps.is_empty() {
        return Err(HttpError::from_string(
            StatusCode::BAD_REQUEST,
            "empty query".to_string(),
        ));
    }

    let QueryStep::Scan(..) = query_steps[0] else {
        return Err(HttpError::from_string(
            StatusCode::NOT_FOUND,
            "first step must be scan".to_string(),
        ));
    };

    let num_steps = query_steps.len();
    let mut steps = Vec::with_capacity(num_steps);

    for (i, step) in query_steps.into_iter().enumerate() {
        match step {
            QueryStep::Scan(..) if i > 0 => {
                return Err(HttpError::from_string(
                    StatusCode::BAD_REQUEST,
                    "scan can only be the first step of a query".to_string(),
                ));
            }
            QueryStep::Scan(connector_name, view) if connector_name == VIEWS_CONNECTOR_NAME => {
                let Some(view_steps) = views.get(&view).cloned() else {
                    return Err(HttpError::from_string(
                        StatusCode::NOT_FOUND,
                        format!("view '{view}' not found"),
                    ));
                };

                steps.extend(to_workflow_steps(connectors, views, view_steps)?);
            }
            QueryStep::Scan(connector_name, collection_name) => {
                let Some(connector_state) = connectors.get(&connector_name).cloned() else {
                    return Err(HttpError::from_string(
                        StatusCode::NOT_FOUND,
                        format!("connector '{connector_name}' not found"),
                    ));
                };

                info!(?collection_name, "Getting collection info");
                let Some(collection) = connector_state.connector.get_collection(&collection_name)
                else {
                    info!(?collection_name, "Collection doesn't exist");
                    return Err(HttpError::from_string(
                        StatusCode::NOT_FOUND,
                        format!("collection '{collection_name}' not found"),
                    ));
                };

                steps.push(WorkflowStep::Scan(
                    Scan::new(
                        connector_state,
                        connector_name,
                        collection_name,
                        collection.static_fields,
                    )
                    .map_err(|e| {
                        HttpError::from_string(
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("failed to create connector from scan step: {e}"),
                        )
                    })?,
                ));
            }
            _ if steps.is_empty() => {
                return Err(HttpError::from_string(
                    StatusCode::BAD_REQUEST,
                    "first query step must be a scan".to_string(),
                ));
            }
            QueryStep::Filter(expr) => {
                steps.push(WorkflowStep::Filter(expr));
            }
            QueryStep::Project(fields) => {
                steps.push(WorkflowStep::Project(fields));
            }
            QueryStep::Extend(fields) => {
                steps.push(WorkflowStep::Extend(fields));
            }
            QueryStep::Rename(renames) => {
                steps.push(WorkflowStep::Rename(renames));
            }
            QueryStep::Expand(expand) => {
                steps.push(WorkflowStep::Expand(expand));
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
            QueryStep::Summarize(summarize) => {
                steps.push(WorkflowStep::Summarize(summarize));
            }
            QueryStep::Distinct(by) => {
                steps.push(WorkflowStep::Summarize(Summarize {
                    aggs: HashMap::new(),
                    by: by.into_iter().map(Expr::Field).collect(),
                }));
            }
            QueryStep::Union(inner_steps) => {
                steps.push(WorkflowStep::Union(Workflow::new(to_workflow_steps(
                    connectors,
                    views,
                    inner_steps,
                )?)));
            }
            QueryStep::Join(config, inner_steps) => {
                steps.push(WorkflowStep::Join(
                    config,
                    Workflow::new(to_workflow_steps(connectors, views, inner_steps)?),
                ));
            }
            QueryStep::Count => {
                steps.push(WorkflowStep::Count);
            }
        }
    }

    Ok(steps)
}
