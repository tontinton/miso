use std::{sync::Arc, time::Instant};

use async_stream::stream;
use color_eyre::eyre::{bail, Context, Result};
use futures_core::Future;
use futures_util::{future::try_join_all, stream::FuturesUnordered, StreamExt};
use join::{join_streams, Join, JoinType};
use kinded::Kinded;
use summarize::{summarize_stream, Summarize};
use tokio::{
    spawn,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use topn::topn_stream;
use tracing::{debug, info, instrument};
use vrl::core::Value;

use crate::{
    connector::{Connector, QueryHandle, QueryResponse, Split},
    log::{Log, LogStream, LogTryStream},
    workflow::{
        filter::filter_stream, limit::limit_stream, project::project_stream, sort::sort_stream,
    },
};

use self::{filter::FilterAst, project::ProjectField, sort::Sort};

pub mod filter;
pub mod join;
pub mod limit;
pub mod project;
pub mod sort;
mod sortable_value;
pub mod summarize;
pub mod topn;
mod vrl_utils;

const COUNT_LOG_FIELD_NAME: &str = "count";

type WorkflowTasks = FuturesUnordered<JoinHandle<Result<()>>>;

#[derive(Clone, Debug)]
pub struct Scan {
    pub collection: String,
    pub connector: Arc<dyn Connector>,
    pub splits: Vec<Arc<dyn Split>>,
    pub handle: Arc<dyn QueryHandle>,
}

#[derive(Kinded, Clone, Debug)]
pub enum WorkflowStep {
    /// Run a search query.
    Scan(Scan),

    /// Filter some records.
    Filter(FilterAst),

    /// Project to select only some of the fields, and optionally rename some.
    Project(Vec<ProjectField>),

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
    Join((Join, Workflow)),

    /// The number of records. Only works as the last step.
    Count,
}

impl std::fmt::Display for WorkflowStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkflowStep::Scan(..) => write!(f, "scan"),
            WorkflowStep::Filter(..) => write!(f, "filter"),
            WorkflowStep::Project(..) => write!(f, "project"),
            WorkflowStep::Limit(limit) => write!(f, "limit({})", limit),
            WorkflowStep::Sort(..) => write!(f, "sort"),
            WorkflowStep::TopN(.., limit) => write!(f, "top-n({})", limit),
            WorkflowStep::Summarize(..) => write!(f, "summarize"),
            WorkflowStep::Union(..) => write!(f, "union"),
            WorkflowStep::Join(..) => write!(f, "join"),
            WorkflowStep::Count => write!(f, "count"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Workflow {
    pub steps: Vec<WorkflowStep>,
}

fn rx_stream(mut rx: mpsc::Receiver<Log>) -> LogStream {
    Box::pin(stream! {
        while let Some(log) = rx.recv().await {
            yield log
        }
    })
}

async fn count_to_tx(count: i64, tx: mpsc::Sender<Log>) {
    let mut count_log = Log::new();
    count_log.insert(COUNT_LOG_FIELD_NAME.into(), Value::Integer(count));
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

impl WorkflowStep {
    #[instrument(skip_all, fields(step = %self))]
    async fn execute(
        self: WorkflowStep,
        rx: Option<mpsc::Receiver<Log>>,
        tx: mpsc::Sender<Log>,
        cancel_rx: watch::Receiver<()>,
    ) -> Result<()> {
        let start = Instant::now();

        match self {
            WorkflowStep::Scan(Scan {
                collection,
                connector,
                splits,
                handle,
            }) => {
                let mut split_tasks = Vec::new();

                for (i, split) in splits.into_iter().enumerate() {
                    let collection = collection.clone();
                    let connector = connector.clone();
                    let handle = handle.clone();
                    let tx = tx.clone();

                    split_tasks.push(spawn(async move {
                        let response = connector
                            .query(&collection, split.as_ref(), handle.as_ref())
                            .await?;

                        match response {
                            QueryResponse::Logs(stream) => {
                                stream_to_tx(stream, tx, &format!("scan({i})")).await?;
                            }
                            QueryResponse::Count(count) => return Ok(Some(count)),
                        }

                        Ok::<Option<i64>, color_eyre::eyre::Error>(None)
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
                let stream = filter_stream(&ast, rx_stream(rx.unwrap()))?;
                stream_to_tx(stream, tx, "filter").await?;
            }
            WorkflowStep::Project(fields) => {
                let stream = project_stream(&fields, rx_stream(rx.unwrap()))?;
                stream_to_tx(stream, tx, "project").await?;
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
            WorkflowStep::Join((config, workflow)) => {
                if workflow.steps.is_empty() {
                    if matches!(config.type_, JoinType::Left | JoinType::Outer) {
                        pipe(rx.unwrap(), tx).await;
                    }
                    return Ok(());
                }

                let (tasks, join_rx) = workflow.create_tasks(cancel_rx.clone())?;

                tasks.push(spawn(async move {
                    let stream =
                        join_streams(config, rx_stream(rx.unwrap()), rx_stream(join_rx)).await;
                    stream_to_tx(stream, tx, "join").await?;
                    Ok(())
                }));

                execute_tasks(tasks, cancel_rx).await?;
            }
            WorkflowStep::Count => {
                let mut rx = rx.unwrap();

                let mut count: i64 = 0;
                while rx.recv().await.is_some() {
                    count += 1;
                }

                count_to_tx(count, tx).await;
            }
        }

        let duration = start.elapsed();
        info!(elapsed_time = ?duration, "Workflow step execution time");

        Ok(())
    }
}

#[instrument(skip_all)]
async fn execute_tasks(mut tasks: WorkflowTasks, mut cancel_rx: watch::Receiver<()>) -> Result<()> {
    let start = Instant::now();

    loop {
        tokio::select! {
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
            _ = cancel_rx.changed() => {
                info!("Workflow cancelled");
                for handle in &tasks {
                    handle.abort();
                }
                break;
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

    pub async fn execute<F, Fut>(self, on_log: F, cancel_rx: watch::Receiver<()>) -> Result<()>
    where
        F: Fn(Log) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if self.steps.is_empty() {
            return Ok(());
        }

        let (tasks, mut rx) = self.create_tasks(cancel_rx.clone())?;
        tasks.push(spawn(async move {
            while let Some(log) = rx.recv().await {
                on_log(log).await?;
            }
            Ok(())
        }));

        execute_tasks(tasks, cancel_rx).await?;
        Ok(())
    }
}
