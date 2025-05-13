use std::{
    fmt,
    hash::BuildHasher,
    sync::Arc,
    time::{Duration, Instant},
};

use async_stream::{stream, try_stream};
use color_eyre::eyre::{bail, Context, Result};
use futures_util::{future::try_join_all, stream::FuturesUnordered, StreamExt};
use hashbrown::{DefaultHashBuilder, HashSet};
use join::{join_streams, Join, JoinType};
use kinded::Kinded;
use parking_lot::Mutex;
use project::extend_stream;
use serde_json::Value;
use summarize::{summarize_stream, Summarize};
use tokio::{
    spawn,
    sync::{mpsc, watch},
    task::JoinHandle,
    time::timeout,
};
use topn::topn_stream;
use tracing::{debug, error, info, instrument};

use crate::{
    connectors::{
        stats::{ConnectorStats, FieldStats},
        Connector, ConnectorState, QueryHandle, QueryResponse,
    },
    log::{Log, LogStream, LogTryStream},
    workflow::{
        filter::filter_stream, limit::limit_stream, project::project_stream, sort::sort_stream,
    },
};

use self::{filter::FilterAst, project::ProjectField, sort::Sort};

pub mod filter;
mod interpreter;
pub mod join;
pub mod limit;
pub mod project;
mod serde_json_utils;
pub mod sort;
pub mod sortable_value;
pub mod summarize;
pub mod topn;

#[cfg(test)]
mod tests;

const COUNT_LOG_FIELD_NAME: &str = "count";
const DYNAMIC_FILTER_TIMEOUT: Duration = Duration::from_secs(30);

const DISPLAY_INDENT: &str = "    ";

type WorkflowTasks = FuturesUnordered<JoinHandle<Result<()>>>;

#[derive(Clone, Debug)]
pub struct Scan {
    pub connector_name: String,
    pub collection: String,

    pub connector: Arc<dyn Connector>,
    pub handle: Arc<dyn QueryHandle>,
    pub stats: Arc<Mutex<ConnectorStats>>,

    pub dynamic_filter_tx: Option<watch::Sender<Option<FilterAst>>>,
    pub dynamic_filter_rx: Option<watch::Receiver<Option<FilterAst>>>,
}

impl PartialEq for Scan {
    fn eq(&self, other: &Self) -> bool {
        // Only checking the name for now.
        self.connector_name == other.connector_name && self.collection == other.collection
    }
}

impl Scan {
    pub async fn from_connector_state(
        connector_state: Arc<ConnectorState>,
        connector_name: String,
        collection: String,
    ) -> Self {
        let connector = connector_state.connector.clone();
        Self {
            connector_name,
            collection,
            handle: connector.get_handle().into(),
            connector,
            stats: connector_state.stats.clone(),
            dynamic_filter_tx: None,
            dynamic_filter_rx: None,
        }
    }

    pub fn get_field_stats(&self, field: &str) -> Option<FieldStats> {
        self.stats
            .lock()
            .get(&self.collection)
            .and_then(|x| x.get(field))
            .cloned()
    }
}

#[derive(Kinded, Clone, Debug, PartialEq)]
pub enum WorkflowStep {
    /// Run a search query.
    Scan(Scan),

    /// Filter some records.
    Filter(FilterAst),

    /// Project to select only some of the fields, and optionally rename some.
    Project(Vec<ProjectField>),

    /// Same as project but keeps original fields, and adds new projected fields to the results.
    Extend(Vec<ProjectField>),

    /// Limit to X amount of records.
    Limit(u32),

    /// Sort records.
    Sort(Vec<Sort>),

    /// Basically like Sort -> Limit, but more memory efficient (holding only N records).
    TopN(Vec<Sort>, u32),

    /// Group records by fields, and aggregate the grouped buckets.
    Summarize(Summarize),

    /// Union results from another query.
    Union(Workflow),

    /// Join results from another query.
    Join(Join, Workflow),

    /// Returns 1 record with a field named "count" containing the number of records.
    Count,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Workflow {
    pub steps: Vec<WorkflowStep>,
}

impl fmt::Display for WorkflowStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let display_step = DisplayableWorkflowStep {
            step: self,
            indent: 0,
        };
        write!(f, "{}", display_step)
    }
}

pub struct DisplayableWorkflowStep<'a> {
    step: &'a WorkflowStep,
    indent: usize,
}

impl<'a> fmt::Display for DisplayableWorkflowStep<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pre = DISPLAY_INDENT.repeat(self.indent);

        match self.step {
            WorkflowStep::Scan(scan) if scan.dynamic_filter_rx.is_some() => {
                write!(
                    f,
                    "{}Scan({}.{}) <Dynamic filter RX>",
                    pre, scan.connector_name, scan.collection
                )
            }
            WorkflowStep::Scan(scan) => write!(
                f,
                "{}Scan({}.{})",
                pre, scan.connector_name, scan.collection
            ),
            WorkflowStep::Filter(..) => write!(f, "{}Filter", pre),
            WorkflowStep::Project(..) => write!(f, "{}Project", pre),
            WorkflowStep::Extend(..) => write!(f, "{}Extend", pre),
            WorkflowStep::Limit(limit) => write!(f, "{}Limit({})", pre, limit),
            WorkflowStep::Sort(sorts) => {
                write!(f, "{}Sort(", pre)?;
                for (i, sort) in sorts.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", sort)?;
                }
                write!(f, ")")
            }
            WorkflowStep::TopN(sorts, limit) => {
                write!(f, "{}TopN({})(", pre, limit)?;
                for (i, sort) in sorts.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", sort)?;
                }
                write!(f, ")")
            }
            WorkflowStep::Summarize(summarize) => write!(f, "{}Summarize({})", pre, summarize),
            WorkflowStep::Union(workflow) => {
                let display_steps = DisplayableWorkflowSteps {
                    steps: &workflow.steps,
                    indent: self.indent,
                };
                write!(f, "{}Union: {}", pre, display_steps)
            }
            WorkflowStep::Join(join, workflow) => {
                let dynamic_filter_tx = match workflow.steps.first() {
                    Some(WorkflowStep::Scan(scan)) if scan.dynamic_filter_tx.is_some() => {
                        " <Dynamic filter TX>".to_string()
                    }
                    _ => String::new(),
                };

                let display_steps = DisplayableWorkflowSteps {
                    steps: &workflow.steps,
                    indent: self.indent,
                };
                write!(
                    f,
                    "{}Join({}){}: {}",
                    pre, join, dynamic_filter_tx, display_steps
                )
            }
            WorkflowStep::Count => write!(f, "{}Count", pre),
        }
    }
}

impl fmt::Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            DisplayableWorkflowSteps {
                steps: &self.steps,
                indent: 0,
            }
        )
    }
}

pub struct DisplayableWorkflowSteps<'a> {
    steps: &'a Vec<WorkflowStep>,
    indent: usize,
}

impl<'a> fmt::Display for DisplayableWorkflowSteps<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[\n")?;
        for (i, step) in self.steps.iter().enumerate() {
            if i > 0 {
                write!(f, ",\n")?;
            }
            let display_step = DisplayableWorkflowStep {
                step,
                indent: self.indent + 1,
            };
            write!(f, "{}", display_step)?;
        }
        let pre = DISPLAY_INDENT.repeat(self.indent);
        write!(f, "\n{}]", pre)
    }
}

fn rx_stream(mut rx: mpsc::Receiver<Log>) -> LogStream {
    Box::pin(stream! {
        while let Some(log) = rx.recv().await {
            yield log
        }
    })
}

fn rx_to_dynamic_filter_tx_stream(
    mut rx: mpsc::Receiver<Log>,
    dynamic_filter_tx: watch::Sender<Option<FilterAst>>,
    field: impl Into<String>,
) -> LogStream {
    let field = field.into();
    let mut values = HashSet::new();

    Box::pin(stream! {
        while let Some(log) = rx.recv().await {
            if let Some(value) = log.get(&field) {
                values.insert(value.clone());
            }
            yield log;
        }

        let ast = FilterAst::In(
            Box::new(FilterAst::Id(field)),
            values.into_iter().map(FilterAst::Lit).collect(),
        );
        if let Err(e) = dynamic_filter_tx.send(Some(ast)) {
            error!("Failed sending dynamic filter: {e:?}");
        }
    })
}

async fn count_to_tx(count: u64, tx: mpsc::Sender<Log>) {
    let mut count_log = Log::new();
    count_log.insert(COUNT_LOG_FIELD_NAME.into(), Value::from(count));
    if let Err(e) = tx.send(count_log).await {
        debug!("Not sending count: {:?}", e);
    }
}

async fn stream_to_tx(mut stream: LogTryStream, tx: mpsc::Sender<Log>, tag: &str) -> Result<()> {
    while let Some(log) = stream.next().await {
        if let Err(e) = tx.send(log.context(format!("tx {tag}"))?).await {
            debug!("Closing {} step: {:?}", tag, e);
            break;
        }
    }
    Ok(())
}

async fn logs_vec_to_tx(logs: Vec<Log>, tx: mpsc::Sender<Log>, tag: &str) {
    for log in logs {
        if let Err(e) = tx.send(log).await {
            debug!("Closing {} step: {:?}", tag, e);
            break;
        }
    }
}

async fn pipe(mut rx: mpsc::Receiver<Log>, tx: mpsc::Sender<Log>) {
    while let Some(log) = rx.recv().await {
        if tx.send(log).await.is_err() {
            break;
        }
    }
}

async fn pipe_task(rx: mpsc::Receiver<Log>, tx: mpsc::Sender<Log>) -> Result<()> {
    pipe(rx, tx).await;
    Ok(())
}

async fn apply_dynamic_filter(
    connector: &dyn Connector,
    handle: &dyn QueryHandle,
    mut dynamic_filter_rx: watch::Receiver<Option<FilterAst>>,
) -> Option<Arc<dyn QueryHandle>> {
    info!("Waiting for dynamic filter");

    match timeout(DYNAMIC_FILTER_TIMEOUT, dynamic_filter_rx.changed()).await {
        Ok(Ok(())) => {
            info!("Got dynamic filter");
            if let Some(ast) = dynamic_filter_rx.borrow().as_ref() {
                if let Some(dynamic_filtered_handle) = connector.apply_filter(ast, handle) {
                    info!("Applied dynamic filter");
                    return Some(dynamic_filtered_handle.into());
                }
            }
        }
        Ok(Err(e)) => {
            error!("Error waiting on dynamic filter: {e:?}");
        }
        Err(e) => {
            debug!("Timeout waiting on dynamic filter: {e:?}");
        }
    }
    None
}

pub async fn join_streams_partitioned(
    config: Join,
    mut left_stream: LogStream,
    mut right_stream: LogStream,
    partitions: usize,
    output_tx: mpsc::Sender<Log>,
) -> Result<()> {
    let mut tasks = Vec::with_capacity(partitions);
    let mut left_txs = Vec::with_capacity(partitions);
    let mut right_txs = Vec::with_capacity(partitions);

    for i in 0..partitions {
        let (left_tx, left_rx) = mpsc::channel(1);
        let (right_tx, right_rx) = mpsc::channel(1);

        let config = config.clone();
        let output_tx = output_tx.clone();

        let task = spawn(async move {
            let stream = join_streams(config, rx_stream(left_rx), rx_stream(right_rx)).await;
            stream_to_tx(stream, output_tx, &format!("join({i})")).await?;
            Ok::<(), color_eyre::eyre::Error>(())
        });

        tasks.push(task);
        left_txs.push(left_tx);
        right_txs.push(right_tx);
    }

    let (left_key, right_key) = &config.on;
    let build_hasher = DefaultHashBuilder::default();

    loop {
        tokio::select! {
            Some(log) = left_stream.next() => {
                if let Some(value) = log.get(left_key) {
                    let i = (build_hasher.hash_one(value) % partitions as u64) as usize;
                    if let Err(e) = left_txs[i].send(log).await {
                        debug!("Closing partition join step ({i}): {e:?}");
                        break;
                    }
                }
            },
            Some(log) = right_stream.next() => {
                if let Some(value) = log.get(right_key) {
                    let i = (build_hasher.hash_one(value) % partitions as u64) as usize;
                    if let Err(e) = right_txs[i].send(log).await {
                        debug!("Closing partition join step ({i}): {e:?}");
                        break;
                    }
                }
            },
            else => break,
        }
    }

    drop(left_txs);
    drop(right_txs);

    let join_results = try_join_all(tasks).await?;
    for join_result in join_results {
        join_result?;
    }

    Ok(())
}

impl WorkflowStep {
    #[instrument(skip_all, fields(step = %self))]
    async fn execute(
        self: WorkflowStep,
        rx: Option<mpsc::Receiver<Log>>,
        tx: mpsc::Sender<Log>,
        cancel_rx: watch::Receiver<()>,
    ) -> Result<()> {
        let start = Instant::now();
        let _guard = scopeguard::guard((), |_| {
            let duration = start.elapsed();
            info!(elapsed_time = ?duration, "Workflow step execution time");
        });

        match self {
            WorkflowStep::Scan(Scan {
                collection,
                connector,
                mut handle,
                dynamic_filter_rx,
                ..
            }) => {
                if let Some(filter_rx) = dynamic_filter_rx {
                    if let Some(dynamic_filtered_handle) =
                        apply_dynamic_filter(connector.as_ref(), handle.as_ref(), filter_rx).await
                    {
                        handle = dynamic_filtered_handle;
                    }
                }

                let splits = connector.get_splits(handle.as_ref());

                if splits.is_empty() {
                    let response = connector.query(&collection, handle.as_ref(), None).await?;
                    match response {
                        QueryResponse::Logs(stream) => {
                            stream_to_tx(stream, tx, "scan").await?;
                        }
                        QueryResponse::Count(count) => {
                            count_to_tx(count, tx).await;
                        }
                    }
                    return Ok(());
                }

                let mut split_tasks = Vec::with_capacity(splits.len());

                for (i, split) in splits.into_iter().enumerate() {
                    let collection = collection.clone();
                    let connector = connector.clone();
                    let handle = handle.clone();
                    let tx = tx.clone();

                    split_tasks.push(spawn(async move {
                        let response = connector
                            .query(&collection, handle.as_ref(), Some(split.as_ref()))
                            .await?;

                        match response {
                            QueryResponse::Logs(stream) => {
                                stream_to_tx(stream, tx, &format!("scan({i})")).await?;
                            }
                            QueryResponse::Count(count) => return Ok(Some(count)),
                        }

                        Ok::<Option<u64>, color_eyre::eyre::Error>(None)
                    }));
                }

                let join_results = try_join_all(split_tasks).await?;

                let mut count = None;
                for join_result in join_results {
                    if let Some(split_count) = join_result? {
                        if let Some(ref mut inner) = count {
                            *inner += split_count;
                        } else {
                            count = Some(split_count);
                        }
                    } else if count.is_some() {
                        bail!("some queries responded with count and some with logs");
                    }
                }

                if let Some(inner) = count {
                    count_to_tx(inner, tx).await;
                }
            }
            WorkflowStep::Filter(ast) => {
                let stream = filter_stream(ast, rx_stream(rx.unwrap()))?;
                stream_to_tx(stream, tx, "filter").await?;
            }
            WorkflowStep::Project(fields) => {
                let stream = project_stream(fields, rx_stream(rx.unwrap())).await?;
                stream_to_tx(stream, tx, "project").await?;
            }
            WorkflowStep::Extend(fields) => {
                let stream = extend_stream(fields, rx_stream(rx.unwrap())).await?;
                stream_to_tx(stream, tx, "extend").await?;
            }
            WorkflowStep::Limit(limit) => {
                let stream = limit_stream(limit, rx_stream(rx.unwrap()))?;
                stream_to_tx(stream, tx, "limit").await?;
            }
            WorkflowStep::Sort(sorts) => {
                let logs = sort_stream(sorts, rx_stream(rx.unwrap())).await?;
                logs_vec_to_tx(logs, tx, "sort").await;
            }
            WorkflowStep::TopN(sorts, limit) => {
                let logs = topn_stream(sorts, limit, rx_stream(rx.unwrap())).await?;
                logs_vec_to_tx(logs, tx, "top-n").await;
            }
            WorkflowStep::Summarize(config) => {
                let logs = summarize_stream(config, rx_stream(rx.unwrap())).await?;
                logs_vec_to_tx(logs, tx, "summarize").await;
            }
            WorkflowStep::Union(workflow) => {
                if workflow.steps.is_empty() {
                    return Ok(());
                }

                let (tasks, union_rx) = workflow.create_tasks(cancel_rx.clone())?;

                tasks.push(spawn(pipe_task(union_rx, tx.clone())));
                tasks.push(spawn(pipe_task(rx.unwrap(), tx)));

                execute_tasks(tasks, cancel_rx).await?;
            }
            WorkflowStep::Join(join, workflow) => {
                if workflow.steps.is_empty() {
                    if matches!(join.type_, JoinType::Left | JoinType::Outer) {
                        pipe(rx.unwrap(), tx).await;
                    }
                    return Ok(());
                }

                let WorkflowStep::Scan(scan) = &workflow.steps[0] else {
                    panic!("scan not as first step in join?");
                };
                let is_left_sending_dynamic_filter = scan.dynamic_filter_rx.is_some();
                let dynamic_filter_tx = scan.dynamic_filter_tx.clone();

                let (tasks, right_rx) = workflow.create_tasks(cancel_rx.clone())?;

                tasks.push(spawn(async move {
                    let left_rx = rx.unwrap();

                    let (left_stream, right_stream) = match dynamic_filter_tx {
                        Some(tx) if is_left_sending_dynamic_filter => (
                            rx_to_dynamic_filter_tx_stream(left_rx, tx, &join.on.0),
                            rx_stream(right_rx),
                        ),
                        Some(tx) => (
                            rx_stream(left_rx),
                            rx_to_dynamic_filter_tx_stream(right_rx, tx, &join.on.1),
                        ),
                        _ => (rx_stream(left_rx), rx_stream(right_rx)),
                    };

                    let partitions = join.partitions;
                    if partitions > 1 {
                        join_streams_partitioned(join, left_stream, right_stream, partitions, tx)
                            .await?;
                    } else {
                        let stream = join_streams(join, left_stream, right_stream).await;
                        stream_to_tx(stream, tx, "join").await?;
                    }
                    Ok(())
                }));

                execute_tasks(tasks, cancel_rx).await?;
            }
            WorkflowStep::Count => {
                let mut rx = rx.unwrap();

                let mut count: u64 = 0;
                while rx.recv().await.is_some() {
                    count += 1;
                }

                count_to_tx(count, tx).await;
            }
        }

        Ok(())
    }
}

#[instrument(skip_all)]
async fn execute_tasks(mut tasks: WorkflowTasks, mut cancel_rx: watch::Receiver<()>) -> Result<()> {
    let start = Instant::now();

    loop {
        tokio::select! {
            // First select cancel, and only then the tasks.
            biased;

            _ = cancel_rx.changed() => {
                info!("Workflow cancelled");
                for handle in &tasks {
                    handle.abort();
                }
                break;
            }
            task_result = tasks.next() => {
                let Some(join_result) = task_result else {
                    break;
                };

                let result = join_result?;
                if let Err(e) = result {
                    for handle in &tasks {
                        handle.abort();
                    }
                    return Err(e.wrap_err("failed one of the workflow steps"));
                }
            }
        }
    }

    let duration = start.elapsed();
    info!(elapsed_time = ?duration, "Workflow execution time");

    Ok(())
}

impl Workflow {
    pub fn new(steps: Vec<WorkflowStep>) -> Self {
        Self { steps }
    }

    fn create_tasks(
        self,
        cancel_rx: watch::Receiver<()>,
    ) -> Result<(WorkflowTasks, mpsc::Receiver<Log>)> {
        assert!(!self.steps.is_empty());

        let (mut tx, mut next_rx) = mpsc::channel(1);
        let mut rx: Option<mpsc::Receiver<Log>> = None;

        let tasks = FuturesUnordered::new();

        for step in self.steps {
            debug!("Spawning step: {:?}", step);

            tasks.push(spawn(step.execute(rx.take(), tx, cancel_rx.clone())));

            rx = Some(next_rx);
            (tx, next_rx) = mpsc::channel(1);
        }

        Ok((tasks, rx.unwrap()))
    }

    pub fn execute(self, cancel_rx: watch::Receiver<()>) -> Result<LogTryStream> {
        if self.steps.is_empty() {
            return Ok(Box::pin(futures_util::stream::empty()));
        }

        let (tasks, mut rx) = self.create_tasks(cancel_rx.clone())?;
        let mut task = spawn(execute_tasks(tasks, cancel_rx));

        Ok(Box::pin(try_stream! {
            let task_alive = loop {
                tokio::select! {
                    log = rx.recv() => {
                        if let Some(log) = log {
                            yield log;
                        } else {
                            break Ok(true);
                        }
                    }
                    result = &mut task => {
                        match result {
                            Ok(Ok(())) => {
                                // Finish reading whatever is left in the channel.
                                while let Some(log) = rx.recv().await {
                                    yield log;
                                }
                                break Ok(false);
                            },
                            Ok(Err(e)) => {
                                break Err(e);
                            }
                            Err(e) => {
                                break Err(e.into());
                            }
                        }
                    }
                }
            }?;

            if task_alive {
                task.await.context("join workflow task")?.context("run workflow task")?;
            }
        }))
    }
}
