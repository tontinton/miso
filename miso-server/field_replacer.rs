use std::str::FromStr;

use axum::http::StatusCode;
use bytesize::ByteSize;
use hashbrown::HashMap;
use miso_workflow::{
    Workflow, WorkflowStep,
    scan::Scan,
    sort::{Sort as SortConfig, SortLimits},
};
use miso_workflow_types::{
    expr::Expr, field::Field, project::ProjectField, query::QueryStep, summarize::Summarize,
};
use tracing::info;

use crate::{
    VIEWS_CONNECTOR_NAME, ViewsMap,
    config::WorkflowLimits,
    http_server::{ConnectorsMap, HttpError},
};
use tracing::info;

use crate::{
    VIEWS_CONNECTOR_NAME, ViewsMap,
    config::WorkflowLimits,
    http_server::{ConnectorsMap, HttpError},
};

#[derive(Default)]
pub struct QueryToWorkflowTranspiler {
    skip_adding_rename: bool,
}

fn parse_field(name: &str) -> Result<Field, HttpError> {
    Field::from_str(name).map_err(|e| {
        HttpError::from_string(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to parse field ({name}): {e}"),
        )
    })
}

impl QueryToWorkflowTranspiler {
    pub fn skip_adding_rename() -> Self {
        Self {
            skip_adding_rename: true,
        }
    }

    pub fn to_workflow_steps(
        &self,
        connectors: &ConnectorsMap,
        views: &ViewsMap,
        query_steps: Vec<QueryStep>,
        workflow_limits: &WorkflowLimits,
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

                    steps.extend(self.to_workflow_steps(connectors, views, view_steps)?);
                }
                QueryStep::Scan(connector_name, collection_name) => {
                    let Some(connector_state) = connectors.get(&connector_name).cloned() else {
                        return Err(HttpError::from_string(
                            StatusCode::NOT_FOUND,
                            format!("connector '{connector_name}' not found"),
                        ));
                    };

                    info!(?collection_name, "Getting collection info");
                    let Some(collection) =
                        connector_state.connector.get_collection(&collection_name)
                    else {
                        info!(?collection_name, "Collection doesn't exist");
                        return Err(HttpError::from_string(
                            StatusCode::NOT_FOUND,
                            format!("collection '{collection_name}' not found"),
                        ));
                    };

                    let renames = collection
                        .field_replacements
                        .into_iter()
                        .map(|(to, from)| {
                            Ok(ProjectField {
                                from: Expr::Field(parse_field(&from)?),
                                to: parse_field(&to)?,
                            })
                        })
                        .collect::<Result<Vec<_>, HttpError>>()?;

                    steps.push(WorkflowStep::Scan(
                        Scan::from_connector_state(
                            connector_state,
                            connector_name,
                            collection_name,
                        )
                        .map_err(|e| {
                            HttpError::from_string(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("failed to create connector from scan step: {e}"),
                            )
                        })?,
                    ));

                    if !self.skip_adding_rename {
                        let renames = collection
                            .field_replacements
                            .into_iter()
                            .map(|(to, from)| {
                                Ok(ProjectField {
                                    from: Expr::Field(parse_field(&from)?),
                                    to: parse_field(&to)?,
                                })
                            })
                            .collect::<Result<Vec<_>, HttpError>>()?;

                        if !renames.is_empty() {
                            steps.push(WorkflowStep::Extend(renames));
                        }
                    }
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
                QueryStep::Limit(max) => {
                    steps.push(WorkflowStep::Limit(max));
                }
                QueryStep::Sort(sort) => {
                    steps.push(WorkflowStep::Sort(SortConfig {
                        sorts: sort,
                        limits: SortLimits {
                            max_memory_bytes: workflow_limits.sort_memory_limit.as_u64(),
                        },
                    }));
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
                    steps.push(WorkflowStep::Union(Workflow::new(self.to_workflow_steps(
                        connectors,
                        views,
                        inner_steps,
                        workflow_limits,
                    )?)));
                }
                QueryStep::Join(config, inner_steps) => {
                    steps.push(WorkflowStep::Join(
                        config,
                        Workflow::new(self.to_workflow_steps(
                            connectors,
                            views,
                            inner_steps,
                            workflow_limits,
                        )?),
                    ));
                }
                QueryStep::Count => {
                    steps.push(WorkflowStep::Count);
                }
            }
        }

        Ok(steps)
    }
}
