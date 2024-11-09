use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    sync::Arc,
};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Extension, Json, Router,
};
use color_eyre::{
    eyre::{bail, Context},
    Result,
};
use futures_util::{pin_mut, stream::FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, to_string};
use tokio::{
    select, spawn,
    sync::{mpsc, RwLock},
};
use tracing::{debug, error, info, span, Level};
use uuid::Uuid;
use vrl::{
    compiler::{compile, state::RuntimeState, Program, TargetValue, TimeZone},
    diagnostic::{DiagnosticList, Severity},
    value::{Secrets, Value},
};

use crate::{
    connector::{Connector, FilterPushdown, Log, Split},
    filter::{filter_ast_to_vrl, FilterAst},
    project::{project_fields_to_vrl, ProjectField},
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

    /// Project to select only some of the fields, and optionally rename some.
    Project(Vec<ProjectField>),
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

fn run_vrl(program: &Program, log: Log) -> Result<Value> {
    let mut target = TargetValue {
        value: log.into(),
        metadata: Value::Object(BTreeMap::new()),
        secrets: Secrets::default(),
    };
    let mut state = RuntimeState::default();
    let timezone = TimeZone::default();
    let mut ctx = vrl::compiler::Context::new(&mut target, &mut state, &timezone);
    Ok(program.resolve(&mut ctx)?)
}

fn run_vrl_filter(program: &Program, log: Log) -> Result<bool> {
    let Value::Boolean(allowed) = run_vrl(program, log)? else {
        bail!("Response of VRL script not boolean");
    };
    Ok(allowed)
}

fn run_vrl_project(program: &Program, log: Log) -> Result<Log> {
    let Value::Object(map) = run_vrl(program, log)? else {
        bail!("Response of VRL script not object");
    };
    Ok(map)
}

impl Workflow {
    async fn execute(
        self,
        connector: Arc<dyn Connector>,
        collection: &str,
        limit: Option<u64>,
    ) -> Result<()> {
        if self.steps.is_empty() {
            return Ok(());
        }

        let (mut tx, mut next_rx) = mpsc::channel(1);
        let mut rx: Option<mpsc::Receiver<Log>> = None;

        let mut handles = FuturesUnordered::new();

        for step in self.steps {
            let handle = spawn({
                let tx = tx.clone();
                let rx = rx.take();
                let collection = collection.to_string();
                let connector = connector.clone();

                async move {
                    match step {
                        WorkflowStep::Scan {
                            splits,
                            pushdown,
                            limit,
                        } => {
                            for split in splits {
                                let mut query_stream = connector.query(
                                    &collection,
                                    &*split,
                                    &pushdown.as_deref(),
                                    limit,
                                )?;
                                while let Some(log) = query_stream.next().await {
                                    if let Err(e) = tx.send(log.context("scan")?).await {
                                        debug!("Closing scan step: {}", e);
                                        break;
                                    }
                                }
                            }
                        }
                        WorkflowStep::Filter(ast) => {
                            let mut rx = rx.unwrap();
                            let script = filter_ast_to_vrl(&ast);
                            info!("Filtering: `{script}`");
                            let program = compile_pretty_print_errors(&script)
                                .context("compile filter vrl")?;
                            while let Some(log) = rx.recv().await {
                                if !run_vrl_filter(&program, log.clone()).context("filter vrl")? {
                                    continue;
                                }

                                if let Err(e) = tx.send(log).await {
                                    debug!("Closing filter step: {}", e);
                                    break;
                                }
                            }
                        }
                        WorkflowStep::Project(fields) => {
                            let mut rx = rx.unwrap();
                            let script = project_fields_to_vrl(&fields);
                            info!("Projecting: `{script}`");
                            let program = compile_pretty_print_errors(&script)
                                .context("compile project vrl")?;
                            while let Some(log) = rx.recv().await {
                                let log = run_vrl_project(&program, log).context("project vrl")?;
                                if let Err(e) = tx.send(log).await {
                                    debug!("Closing project step: {}", e);
                                    break;
                                }
                            }
                        }
                    }

                    Ok::<(), color_eyre::eyre::Error>(())
                }
            });

            handles.push(handle);

            rx = Some(next_rx);
            (tx, next_rx) = mpsc::channel(1);
        }

        let print_future = async {
            let mut rx = rx.unwrap();
            let mut received = 0;

            while let Some(log) = rx.recv().await {
                println!("{}", to_string(&log).context("log to string")?);

                received += 1;
                if let Some(limit) = limit {
                    if received >= limit {
                        break;
                    }
                }
            }

            Ok::<(), color_eyre::eyre::Error>(())
        };
        pin_mut!(print_future);

        loop {
            select! {
                result = &mut print_future => {
                    for handle in handles {
                        handle.abort();
                    }
                    result?;
                }
                maybe_result = handles.next() => {
                    if let Some(result) = maybe_result {
                        if let Err(e) = result {
                            for handle in handles {
                                handle.abort();
                            }
                            bail!("Failed one of the workflow step tasks: {}", e);
                        }
                        continue;
                    } else {
                        print_future.await?;
                    }
                }
            }

            debug!("Printing finished successfully");
            break;
        }

        Ok(())
    }
}

async fn to_workflow(
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
            // TODO: add project predicate pushdown.
            _ => {
                break;
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
        splits: connector.get_splits().await,
        pushdown,
        limit,
    });

    // Add leftover steps.
    for step in query_steps.into_iter().skip(number_of_pushdown_steps) {
        match step {
            QueryStep::Filter(ast) => {
                steps.push(WorkflowStep::Filter(ast));
            }
            QueryStep::Project(fields) => {
                steps.push(WorkflowStep::Project(fields));
            }
        }
    }

    Workflow { steps }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueryStep {
    Filter(FilterAst),
    Project(Vec<ProjectField>),
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
    let workflow = to_workflow(req.query, req.limit, &*connector).await;

    info!(?workflow, "Executing workflow");
    if let Err(err) = workflow
        .execute(connector, &req.collection, req.limit)
        .await
    {
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
