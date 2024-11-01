use std::collections::BTreeMap;

use axum::{http::StatusCode, routing::post, Json, Router};
use color_eyre::eyre::{bail, Context};
use color_eyre::Result;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::info;
use uuid::Uuid;
use vrl::{
    compiler::{compile, state::RuntimeState, Program, TargetValue, TimeZone},
    diagnostic::{DiagnosticList, Severity},
    value::{Secrets, Value},
};

use crate::ast::{ast_to_vrl, filter_predicate_pushdown, FilterAst};
use crate::{
    connector::{Connector, Split},
    elasticsearch::ElasticsearchConnector,
};

#[derive(Debug)]
enum WorkflowStep {
    /// Run a search query.
    Scan {
        splits: Vec<Split>,
        pushdown: Option<FilterAst>,
    },
    /// Filter some items.
    Filter(FilterAst),
}

#[derive(Debug)]
struct Workflow {
    steps: Vec<WorkflowStep>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueryStep {
    Filter(FilterAst),
}

#[derive(Debug, Deserialize)]
struct QueryPlan {
    steps: Vec<QueryStep>,
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

fn compile_pretty_print_errors(script: &str) -> Result<Program> {
    match compile(script, &vrl::stdlib::all()) {
        Ok(program) => {
            if !program.warnings.is_empty() {
                println!("Warnings:");
                pretty_print_diagnostics(&program.warnings);
            }
            Ok(program.program)
        }
        Err(diagnostics) => {
            println!("Errors:");
            pretty_print_diagnostics(&diagnostics);
            bail!("Failed to compile VRL script:\n{}", script);
        }
    }
}

fn run_vrl_filter(program: &Program, log: &str) -> Result<bool> {
    let mut target = TargetValue {
        value: serde_json::from_slice::<Value>(log.as_bytes())?,
        metadata: Value::Object(BTreeMap::new()),
        secrets: Secrets::default(),
    };
    let mut state = RuntimeState::default();
    let timezone = TimeZone::default();
    let mut ctx = vrl::compiler::Context::new(&mut target, &mut state, &timezone);
    let Value::Boolean(allowed) = program.resolve(&mut ctx)? else {
        bail!("Response of VRL script not boolean");
    };

    Ok(allowed)
}

impl Workflow {
    async fn execute(&self, connector: &dyn Connector) -> Result<()> {
        // This is where each activity will be coordinated to a worker to be executed in parallel.
        // Right now as an example, we run everything sequentially here.
        let mut logs = Vec::new();
        for step in &self.steps {
            match step {
                WorkflowStep::Scan { splits, pushdown } => {
                    // Currently running VRL and not predicating to connector.
                    let program = if let Some(pushdown) = pushdown {
                        let script = ast_to_vrl(pushdown);
                        Some(compile_pretty_print_errors(&script)?)
                    } else {
                        None
                    };

                    for split in splits {
                        let mut query_stream = connector.query(split);
                        while let Some(log) = query_stream.next().await {
                            if let Some(ref program) = program {
                                if !run_vrl_filter(program, &log)
                                    .context("scan fake predicate pushdown")?
                                {
                                    continue;
                                }
                            }

                            logs.push(log);
                        }
                    }
                }
                WorkflowStep::Filter(ast) => {
                    let script = ast_to_vrl(ast);
                    let program = compile_pretty_print_errors(&script)?;
                    let mut filtered_logs = Vec::new();
                    for log in logs {
                        if !run_vrl_filter(&program, &log).context("filter vrl")? {
                            continue;
                        }

                        filtered_logs.push(log);
                    }
                    logs = filtered_logs;
                }
            }
        }

        for log in logs {
            println!("{log}");
        }

        Ok(())
    }
}

fn to_workflow(plan: QueryPlan, connector: &dyn Connector) -> Workflow {
    // The steps to run after all predicate pushdowns.
    let mut steps = Vec::new();

    let mut pushdown_filters = Vec::new();
    let mut steps_predicated = 0;

    // Try to pushdown steps.
    for step in &plan.steps {
        match step {
            QueryStep::Filter(ast) => {
                let (leftover, predicated) = filter_predicate_pushdown(ast.clone(), connector);
                assert!(leftover.is_some() || predicated.is_some());

                steps_predicated += 1;

                if let Some(predicated) = predicated {
                    pushdown_filters.push(predicated);
                }

                if let Some(leftover) = leftover {
                    steps.push(WorkflowStep::Scan {
                        splits: connector.get_splits(),
                        pushdown: (!pushdown_filters.is_empty())
                            .then_some(FilterAst::And(pushdown_filters)),
                    });
                    steps.push(WorkflowStep::Filter(leftover));

                    // Found a step that was not fully predicated pushdown, stop trying to
                    // predicate any step after this.
                    break;
                }
            }
        }
    }

    // Add leftover steps.
    for step in plan.steps.into_iter().skip(steps_predicated) {
        match step {
            QueryStep::Filter(ast) => {
                steps.push(WorkflowStep::Filter(ast));
            }
        }
    }

    Workflow { steps }
}

#[derive(Deserialize)]
struct PostQueryRequest {
    /// The query id to set. If not set, the server will randomly generate an id.
    query_id: Option<Uuid>,

    /// The query to run. The structure is similar to Elasticsearch's query API (JSON representing
    /// an AST).
    query: QueryPlan,
}

#[derive(Serialize)]
struct QueryResponse {
    query_id: Uuid,
}

/// Starts running a new query.
async fn post_query_handler(
    Json(req): Json<PostQueryRequest>,
) -> (StatusCode, Json<QueryResponse>) {
    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);
    info!(?req.query, "Starting to run a new query");
    let connector = ElasticsearchConnector {};
    let workflow = to_workflow(req.query, &connector);
    info!(?workflow, "Executing workflow");
    workflow.execute(&connector).await.unwrap();
    (StatusCode::OK, Json(QueryResponse { query_id }))
}

pub fn create_axum_app() -> Router {
    Router::new().route("/query", post(post_query_handler))
}
