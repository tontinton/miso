use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Arc;

use axum::{http::StatusCode, routing::post, Extension, Json, Router};
use color_eyre::{
    eyre::{bail, Context},
    Result,
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::to_string;
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;
use vrl::{
    compiler::{compile, state::RuntimeState, Program, TargetValue, TimeZone},
    diagnostic::{DiagnosticList, Severity},
    value::{KeyString, Secrets, Value},
};

use crate::connector::FilterPushdown;
use crate::{
    ast::{ast_to_vrl, FilterAst},
    connector::{Connector, Log, Split},
    quickwit_connector::QuickwitConnector,
};

struct State {
    connectors: HashMap<String, Arc<dyn Connector>>,
}

type SharedState = Arc<RwLock<State>>;

#[derive(Debug)]
enum WorkflowStep {
    /// Run a search query.
    Scan {
        splits: Vec<Arc<dyn Split>>,
        pushdown: Option<Arc<dyn FilterPushdown>>,
        limit: Option<u64>,
    },
    /// Filter some items.
    Filter(FilterAst),
}

#[derive(Debug)]
struct Workflow {
    steps: Vec<WorkflowStep>,
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

fn log_to_vrl(log: &Log) -> Value {
    Value::Object(
        log.iter()
            .map(|(k, v)| (KeyString::from(k.clone()), Value::from(v.clone())))
            .collect(),
    )
}

fn run_vrl_filter(program: &Program, log: &Log) -> Result<bool> {
    let mut target = TargetValue {
        value: log_to_vrl(log),
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
    async fn execute(
        &self,
        connector: &dyn Connector,
        collection: &str,
        limit: Option<u64>,
    ) -> Result<()> {
        // This is where each activity will be coordinated to a worker to be executed in parallel.
        // Right now as an example, we run everything sequentially here.
        let mut logs = Vec::new();
        for step in &self.steps {
            match step {
                WorkflowStep::Scan {
                    splits,
                    pushdown,
                    limit,
                } => {
                    for split in splits {
                        let mut query_stream =
                            connector.query(collection, &**split, pushdown, *limit)?;
                        while let Some(log) = query_stream.next().await {
                            logs.push(log?);
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

        let limit = limit.unwrap_or(logs.len() as u64);
        for log in logs.into_iter().take(limit as usize) {
            println!("{}", to_string(&log)?);
        }

        Ok(())
    }
}

fn to_workflow(
    query_steps: Vec<QueryStep>,
    limit: Option<u64>,
    connector: &dyn Connector,
) -> Workflow {
    // The steps to run after all predicate pushdowns.
    let mut steps = Vec::new();

    let mut pushdown_filters = Vec::new();

    // Try to pushdown steps.
    for step in &query_steps {
        match step {
            QueryStep::Filter(ast) => {
                pushdown_filters.push(ast.clone());
            }
        }
    }

    let num_filters = pushdown_filters.len();
    let pushdown_filter = FilterAst::And(pushdown_filters);
    let (number_of_pushdown_steps, pushdown) =
        if let Some(pushdown) = connector.apply_filter(&pushdown_filter) {
            (num_filters, Some(pushdown))
        } else {
            (0, None)
        };

    let limit = if num_filters == query_steps.len() {
        limit
    } else {
        None
    };

    steps.push(WorkflowStep::Scan {
        splits: connector.get_splits(),
        pushdown,
        limit,
    });

    // Add leftover steps.
    for step in query_steps.into_iter().skip(number_of_pushdown_steps) {
        match step {
            QueryStep::Filter(ast) => {
                steps.push(WorkflowStep::Filter(ast));
            }
        }
    }

    Workflow { steps }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueryStep {
    Filter(FilterAst),
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

    /// Maximum number of items.
    #[serde(default)]
    limit: Option<u64>,
}

#[derive(Serialize)]
struct QueryResponse {
    query_id: Uuid,
}

/// Starts running a new query.
async fn post_query_handler(
    Extension(state): Extension<SharedState>,
    Json(req): Json<PostQueryRequest>,
) -> (StatusCode, Json<QueryResponse>) {
    // TODO: remove all unwraps in this function.
    let query_id = req.query_id.unwrap_or_else(Uuid::now_v7);

    if req.query.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(QueryResponse { query_id }));
    }

    let Some(connector) = state.read().await.connectors.get(&req.connector).cloned() else {
        return (StatusCode::NOT_FOUND, Json(QueryResponse { query_id }));
    };

    info!(?req.collection, "Checking whether collection exists");
    if !connector.does_collection_exist(&req.collection).await {
        info!(?req.collection, "Collection doesn't exist");
        return (StatusCode::NOT_FOUND, Json(QueryResponse { query_id }));
    }

    let connector_ref = &*connector;

    info!(?req.query, "Starting to run a new query");
    let workflow = to_workflow(req.query, req.limit, connector_ref);
    info!(?workflow, "Executing workflow");
    workflow
        .execute(connector_ref, &req.collection, req.limit)
        .await
        .unwrap();
    (StatusCode::OK, Json(QueryResponse { query_id }))
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
