use std::collections::{BTreeMap, HashMap};
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

use crate::{
    ast::{ast_to_vrl, filter_predicate_pushdown, FilterAst},
    connector::{Connector, Log, Split},
    quickwit_connector::QuickwitConnector,
};

#[derive(Debug)]
struct State {
    connectors: HashMap<String, Arc<dyn Connector>>,
}

type SharedState = Arc<RwLock<State>>;

#[derive(Debug)]
enum WorkflowStep {
    /// Run a search query.
    Scan {
        splits: Vec<Split>,
        pushdown: Option<FilterAst>,
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
                            connector.query(collection, split, pushdown.clone(), *limit)?;
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
    let mut steps_predicated = 0;

    // Try to pushdown steps.
    for step in &query_steps {
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
                            .then_some(FilterAst::And(pushdown_filters.clone())),
                        limit: None,
                    });
                    steps.push(WorkflowStep::Filter(leftover));

                    // Found a step that was not fully predicated pushdown, stop trying to
                    // predicate any step after this.
                    break;
                }
            }
        }
    }

    if steps.is_empty() && steps_predicated == query_steps.len() {
        steps.push(WorkflowStep::Scan {
            splits: connector.get_splits(),
            pushdown: (!pushdown_filters.is_empty()).then_some(FilterAst::And(pushdown_filters)),
            // Predicate pushdown the limit if all filters were predicated.
            limit,
        });
    } else {
        // Add leftover steps.
        for step in query_steps.into_iter().skip(steps_predicated) {
            match step {
                QueryStep::Filter(ast) => {
                    steps.push(WorkflowStep::Filter(ast));
                }
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
