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
    value::{KeyString, Secrets, Value},
};

use crate::{
    connector::{Connector, Log, QueryHandle, Split},
    filter::{filter_ast_to_vrl, FilterAst},
    project::{project_fields_to_vrl, ProjectField},
    quickwit_connector::QuickwitConnector,
    vrl_utils::partial_cmp_values,
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
        handle: Arc<dyn QueryHandle>,
    },

    /// Filter some items.
    Filter(FilterAst),

    /// Project to select only some of the fields, and optionally rename some.
    Project(Vec<ProjectField>),

    /// Limit to X amount of items.
    Limit(u64),

    /// Sort items.
    Sort(Sort),
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
    async fn execute(self, connector: Arc<dyn Connector>, collection: &str) -> Result<()> {
        if self.steps.is_empty() {
            return Ok(());
        }

        let (mut tx, mut next_rx) = mpsc::channel(1);
        let mut rx: Option<mpsc::Receiver<Log>> = None;

        let mut handles = FuturesUnordered::new();

        for step in self.steps {
            debug!("Spawning step: {:?}", step);

            let handle = spawn({
                let tx = tx.clone();
                let rx = rx.take();
                let collection = collection.to_string();
                let connector = connector.clone();

                async move {
                    match step {
                        WorkflowStep::Scan { splits, handle } => {
                            for split in splits {
                                let mut query_stream =
                                    connector.query(&collection, &*split, &*handle)?;
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
                        WorkflowStep::Limit(max) => {
                            let mut rx = rx.unwrap();
                            info!("Limitting to {max}");

                            let mut sent = 0;
                            while let Some(log) = rx.recv().await {
                                if sent >= max {
                                    break;
                                }

                                if let Err(e) = tx.send(log).await {
                                    debug!("Closing limit step: {}", e);
                                    break;
                                }

                                sent += 1;
                            }
                        }
                        WorkflowStep::Sort(sort) => {
                            let mut rx = rx.unwrap();
                            info!("Sorting {:?}", sort);

                            let by: KeyString = sort.by.into();
                            let mut tracked_type = None;

                            let mut logs = Vec::new();
                            while let Some(log) = rx.recv().await {
                                if let Some(value) = log.get(&by) {
                                    let value_type = std::mem::discriminant(value);
                                    if let Some(t) = tracked_type {
                                        if t != value_type {
                                            bail!(
                                                "Cannot sort over differing types: {:?} != {:?}",
                                                t,
                                                value_type
                                            );
                                        }
                                    } else {
                                        tracked_type = Some(value_type);
                                    }
                                }

                                logs.push(log);
                            }

                            let nulls_order = sort.nulls;

                            logs.sort_unstable_by(|a, b| {
                                let a_val = a.get(&by).unwrap_or(&Value::Null);
                                let b_val = b.get(&by).unwrap_or(&Value::Null);
                                let order = match (a_val, b_val, nulls_order.clone()) {
                                    (Value::Null, _, NullsOrder::First) => std::cmp::Ordering::Less,
                                    (_, Value::Null, NullsOrder::First) => {
                                        std::cmp::Ordering::Greater
                                    }
                                    (Value::Null, _, NullsOrder::Last) => {
                                        std::cmp::Ordering::Greater
                                    }
                                    (_, Value::Null, NullsOrder::Last) => std::cmp::Ordering::Less,
                                    _ => partial_cmp_values(a_val, b_val)
                                        .expect("Types should be comparable"),
                                };

                                if sort.order == SortOrder::Asc {
                                    order
                                } else {
                                    order.reverse()
                                }
                            });

                            for log in logs {
                                if let Err(e) = tx.send(log).await {
                                    debug!("Closing sort step: {}", e);
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
            while let Some(log) = rx.recv().await {
                println!("{}", to_string(&log).context("log to string")?);
            }
            Ok::<(), color_eyre::eyre::Error>(())
        };
        pin_mut!(print_future);

        debug!("Starting to print logs");

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

async fn to_workflow(query_steps: Vec<QueryStep>, connector: &dyn Connector) -> Workflow {
    // The steps to run after all predicate pushdowns.
    let mut steps = Vec::new();

    let mut handle = Some(connector.get_handle());
    let mut num_predicated = 0;

    // Try to pushdown steps.
    for step in &query_steps {
        match step {
            QueryStep::Filter(ast) => {
                handle = connector.apply_filter(ast, handle.take().expect("query handle to exist"));
            }
            QueryStep::Limit(max) => {
                handle = connector.apply_limit(*max, handle.take().expect("query handle to exist"));
            }
            // TODO: add project predicate pushdown.
            _ => {
                break;
            }
        }

        if handle.is_none() {
            break;
        }
        num_predicated += 1;
    }

    steps.push(WorkflowStep::Scan {
        splits: connector.get_splits().await,
        handle: handle
            .map(|x| x.into())
            .unwrap_or_else(|| connector.get_handle().into()),
    });

    // Add leftover steps.
    for step in query_steps.into_iter().skip(num_predicated) {
        match step {
            QueryStep::Filter(ast) => {
                steps.push(WorkflowStep::Filter(ast));
            }
            QueryStep::Project(fields) => {
                steps.push(WorkflowStep::Project(fields));
            }
            QueryStep::Limit(max) => {
                steps.push(WorkflowStep::Limit(max));
            }
            QueryStep::Sort(sort) => {
                steps.push(WorkflowStep::Sort(sort));
            }
        }
    }

    Workflow { steps }
}

#[derive(Debug, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
enum SortOrder {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Deserialize, Default, Clone)]
#[serde(rename_all = "snake_case")]
enum NullsOrder {
    #[default]
    Last,
    First,
}

#[derive(Debug, Deserialize)]
struct Sort {
    by: String,

    #[serde(default)]
    order: SortOrder,

    #[serde(default)]
    nulls: NullsOrder,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum QueryStep {
    Filter(FilterAst),
    Project(Vec<ProjectField>),
    Limit(u64),
    Sort(Sort),
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
    let workflow = to_workflow(req.query, &*connector).await;

    info!(?workflow, "Executing workflow");
    if let Err(err) = workflow.execute(connector, &req.collection).await {
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
