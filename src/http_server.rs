use std::collections::BTreeMap;

use axum::{http::StatusCode, routing::post, Json, Router};
use color_eyre::eyre::bail;
use color_eyre::Result;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;
use vrl::{
    compiler::{compile, state::RuntimeState, Context, TargetValue, TimeZone},
    diagnostic::{DiagnosticList, Severity},
    value::{Secrets, Value},
};

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

fn severity_to_str(severity: &Severity) -> &'static str {
    match severity {
        Severity::Bug => "Bug",
        Severity::Error => "Error",
        Severity::Warning => "Warning",
        Severity::Note => "Note",
    }
}

fn pretty_print_diagnostics(diagnostics: &DiagnosticList) {
    let message = diagnostics
        .iter()
        .map(|d| {
            let labels = d
                .labels
                .iter()
                .map(|l| {
                    format!(
                        "    [{}] {} (Span: {}-{})",
                        if l.primary { "Primary" } else { "Secondary" },
                        l.message,
                        l.span.start(),
                        l.span.end(),
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");

            let notes = d
                .notes
                .iter()
                .map(|n| format!("    Note: {}", n))
                .collect::<Vec<_>>()
                .join("\n");

            format!(
                "{} [{}]: {}\n{}\n{}",
                severity_to_str(&d.severity),
                d.code,
                d.message,
                labels,
                notes
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    println!("{message}");
}

impl Workflow {
    async fn execute(&self, connector: &dyn Connector) -> Result<()> {
        // This is where each activity will be sent to a worker to be executed in parallel.
        // Right now as an example, we run everything sequentially here.
        for activity in &self.activities {
            let post_program = if let Some(script) = &activity.post_script {
                match compile(script, &vrl::stdlib::all()) {
                    Ok(program) => {
                        if !program.warnings.is_empty() {
                            println!("Warnings:");
                            pretty_print_diagnostics(&program.warnings);
                        }
                        Some(program.program)
                    }
                    Err(diagnostics) => {
                        println!("Errors:");
                        pretty_print_diagnostics(&diagnostics);
                        bail!("Failed to compile VRL script:\n{}", script);
                    }
                }
            } else {
                None
            };

            match &activity.item {
                ActivityItem::Scan(split) => {
                    let mut query_stream = connector.query(split);
                    while let Some(log) = query_stream.next().await {
                        if let Some(ref program) = post_program {
                            let mut target = TargetValue {
                                value: serde_json::from_slice::<Value>(log.as_bytes())?,
                                metadata: Value::Object(BTreeMap::new()),
                                secrets: Secrets::default(),
                            };
                            let mut state = RuntimeState::default();
                            let timezone = TimeZone::default();
                            let mut ctx = Context::new(&mut target, &mut state, &timezone);
                            let Value::Boolean(allowed) = program.resolve(&mut ctx)? else {
                                bail!("Response of VRL script not boolean");
                            };

                            if !allowed {
                                continue;
                            }
                        }

                        println!("{log}");
                    }
                }
            }
        }

        Ok(())
    }
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
    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);
    info!(?req.query, "Starting to run a new query");
    let connector = ElasticsearchConnector {};
    let workflow = to_workflow(&mut req.query, &connector);
    workflow.execute(&connector).await.unwrap();
    (StatusCode::OK, Json(QueryResponse { query_id }))
}

pub fn create_axum_app() -> Router {
    Router::new().route("/query", post(post_query_handler))
}
