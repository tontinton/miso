use std::{
    convert::identity,
    hash::BuildHasher,
    sync::Arc,
    time::{Duration, Instant},
};

use async_stream::{stream, try_stream};
use color_eyre::eyre::{Context, Result};
use count::{count_stream, count_to_log, mux_count_stream, PartialMuxCountExecutor};
use futures_util::{
    future::try_join_all,
    stream::{self, select_all, FuturesUnordered},
    StreamExt,
};
use hashbrown::{DefaultHashBuilder, HashSet};
use join::{join_streams, Join, JoinType};
use kinded::Kinded;
use parking_lot::Mutex;
use project::extend_stream;
use summarize::{create_summarize_executor, Summarize};
use tokio::{
    spawn,
    sync::{mpsc, watch, Notify},
    task::JoinHandle,
    time::timeout,
};
use topn::{topn_stream, PartialTopNExecutor, SORT_CONFIG};
use tracing::{debug, error, info, instrument};

use crate::{
    connectors::{
        stats::{ConnectorStats, FieldStats},
        Connector, ConnectorState, QueryHandle, QueryResponse, Split,
    },
    log::{Log, LogStream, LogTryStream},
    workflow::{
        filter::filter_stream,
        limit::limit_stream,
        partial_stream::{execute_partial_stream, PartialStream},
        project::project_stream,
        sort::sort_stream,
    },
};

use self::{filter::FilterAst, project::ProjectField, sort::Sort};

mod count;
mod display;
pub mod filter;
mod interpreter;
pub mod join;
pub mod limit;
pub mod partial_stream;
pub mod project;
mod serde_json_utils;
pub mod sort;
pub mod sortable_value;
pub mod summarize;
pub mod topn;

#[cfg(test)]
mod tests;

const MISO_METADATA_FIELD_NAME: &str = "_miso";
const DYNAMIC_FILTER_TIMEOUT: Duration = Duration::from_secs(30);

type WorkflowTasks = FuturesUnordered<JoinHandle<Result<()>>>;

#[derive(Clone, Debug)]
pub struct Scan {
    pub connector_name: String,
    pub collection: String,

    pub connector: Arc<dyn Connector>,
    pub handle: Arc<dyn QueryHandle>,
    pub split: Option<Arc<dyn Split>>,
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
        let handle = connector.get_handle().into();
        let stats = connector_state.stats.clone();
        Self {
            connector_name,
            collection,
            connector,
            handle,
            split: None,
            stats,
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
    MuxLimit(u32),

    /// Sort records.
    Sort(Vec<Sort>),

    /// Basically like Sort -> Limit, but more memory efficient (holding only N records).
    TopN(Vec<Sort>, u32),
    MuxTopN(Vec<Sort>, u32),

    /// Group records by fields, and aggregate the grouped buckets.
    Summarize(Summarize),
    MuxSummarize(Summarize),

    /// Union results from another query.
    Union(Workflow),

    /// Join results from another query.
    Join(Join, Workflow),

    /// Returns 1 record with a field named "count" containing the number of records.
    Count,
    MuxCount,
}

#[derive(Debug, Clone)]
pub struct Workflow {
    pub steps: Vec<WorkflowStep>,

    /// Only the root workflow (not union or join for example) can currently have this set.
    partial_stream: Option<PartialStream>,
}

impl PartialEq for Workflow {
    fn eq(&self, other: &Self) -> bool {
        // Ignore partial stream, we only care about comparing the steps that make up the workflow.
        self.steps == other.steps
    }
}

fn rx_stream(mut rx: mpsc::Receiver<Log>) -> LogStream {
    Box::pin(stream! {
        while let Some(log) = rx.recv().await {
            yield log
        }
    })
}

fn rx_union_stream(rxs: Vec<mpsc::Receiver<Log>>) -> LogStream {
    let streams: Vec<LogStream> = rxs.into_iter().map(rx_stream).collect();
    Box::pin(select_all(streams))
}

fn stream_to_dynamic_filter_tx_stream(
    mut stream: LogStream,
    dynamic_filter_tx: watch::Sender<Option<FilterAst>>,
    field: impl Into<String>,
) -> LogStream {
    let field = field.into();
    let mut values = HashSet::new();

    Box::pin(stream! {
        while let Some(log) = stream.next().await {
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
    let logs = [count_to_log(count); 1];
    logs_iter_to_tx(logs, tx, "count").await;
}

async fn stream_to_tx(mut stream: LogStream, tx: mpsc::Sender<Log>, tag: &str) -> Result<()> {
    while let Some(log) = stream.next().await {
        if let Err(e) = tx.send(log).await {
            debug!("Closing {} step: {:?}", tag, e);
            break;
        }
    }
    Ok(())
}

async fn try_stream_to_tx(
    mut stream: LogTryStream,
    tx: mpsc::Sender<Log>,
    tag: &str,
) -> Result<()> {
    while let Some(log) = stream.next().await {
        if let Err(e) = tx.send(log.context(format!("tx {tag}"))?).await {
            debug!("Closing {} step: {:?}", tag, e);
            break;
        }
    }
    Ok(())
}

pub async fn logs_iter_to_tx<I>(logs: I, tx: mpsc::Sender<Log>, tag: &str)
where
    I: IntoIterator<Item = Log>,
{
    for log in logs {
        if let Err(e) = tx.send(log).await {
            debug!("Closing {} step: {:?}", tag, e);
            break;
        }
    }
}

async fn pipe(mut rx: LogStream, tx: mpsc::Sender<Log>) {
    while let Some(log) = rx.next().await {
        if tx.send(log).await.is_err() {
            break;
        }
    }
}

async fn pipe_task(rx: LogStream, tx: mpsc::Sender<Log>) -> Result<()> {
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
        rxs: Vec<mpsc::Receiver<Log>>,
        tx: mpsc::Sender<Log>,
        partial_stream: Option<PartialStream>,
        cancel: Arc<Notify>,
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
                split,
                dynamic_filter_rx,
                ..
            }) => {
                assert!(rxs.is_empty());

                if let Some(filter_rx) = dynamic_filter_rx {
                    if let Some(dynamic_filtered_handle) =
                        apply_dynamic_filter(connector.as_ref(), handle.as_ref(), filter_rx).await
                    {
                        handle = dynamic_filtered_handle;
                    }
                }

                let response = connector
                    .query(&collection, handle.as_ref(), split.as_deref())
                    .await?;
                match response {
                    QueryResponse::Logs(stream) => {
                        try_stream_to_tx(stream, tx, "scan").await?;
                    }
                    QueryResponse::Count(count) => {
                        count_to_tx(count, tx).await;
                    }
                }
            }
            WorkflowStep::Filter(ast) => {
                let stream = filter_stream(ast, rx_union_stream(rxs));
                stream_to_tx(stream, tx, "filter").await?;
            }
            WorkflowStep::Project(fields) => {
                let stream = project_stream(fields, rx_union_stream(rxs)).await;
                stream_to_tx(stream, tx, "project").await?;
            }
            WorkflowStep::Extend(fields) => {
                let stream = extend_stream(fields, rx_union_stream(rxs)).await;
                stream_to_tx(stream, tx, "extend").await?;
            }
            WorkflowStep::Limit(limit) | WorkflowStep::MuxLimit(limit) => {
                let stream = limit_stream(limit, rx_union_stream(rxs));
                stream_to_tx(stream, tx, "limit").await?;
            }
            WorkflowStep::Sort(sorts) => {
                let logs = sort_stream(sorts, rx_union_stream(rxs)).await?;
                logs_iter_to_tx(logs, tx, "sort").await;
            }
            WorkflowStep::MuxTopN(sorts, limit) if partial_stream.is_some() => {
                let (executor, config) = PartialTopNExecutor::new(sorts, limit);
                let stream_fut = execute_partial_stream(
                    Box::new(executor),
                    partial_stream.unwrap(),
                    rxs,
                    tx,
                    identity,
                    "partial mux top-n",
                );
                SORT_CONFIG.scope(config, stream_fut).await?;
            }
            WorkflowStep::TopN(sorts, limit) | WorkflowStep::MuxTopN(sorts, limit) => {
                let (stream, config) = topn_stream(sorts, limit, rx_union_stream(rxs)).await;
                SORT_CONFIG
                    .scope(config, stream_to_tx(stream, tx, "top-n"))
                    .await?;
            }
            WorkflowStep::MuxSummarize(config) if partial_stream.is_some() => {
                execute_partial_stream(
                    create_summarize_executor(config),
                    partial_stream.unwrap(),
                    rxs,
                    tx,
                    identity,
                    "mux summarize",
                )
                .await
                .context("partial mux summarize stream")?;
            }
            WorkflowStep::Summarize(config) | WorkflowStep::MuxSummarize(config) => {
                let executor = create_summarize_executor(config);
                let logs = executor.execute(rx_union_stream(rxs)).await?;
                logs_iter_to_tx(logs, tx, "summarize").await;
            }
            WorkflowStep::Union(workflow) => {
                assert!(rxs.is_empty());

                if workflow.steps.is_empty() {
                    return Ok(());
                }

                let (tasks, rx) = workflow.create_tasks(cancel.clone())?;
                tasks.push(spawn(pipe_task(rx, tx)));

                execute_tasks(tasks, cancel).await?;
            }
            WorkflowStep::Join(join, workflow) => {
                let input_stream = rx_union_stream(rxs);
                if workflow.steps.is_empty() {
                    if matches!(join.type_, JoinType::Left | JoinType::Outer) {
                        pipe(input_stream, tx).await;
                    }
                    return Ok(());
                }

                let WorkflowStep::Scan(scan) = &workflow.steps[0] else {
                    panic!("scan not as first step in join?");
                };
                let is_left_sending_dynamic_filter = scan.dynamic_filter_rx.is_some();
                let dynamic_filter_tx = scan.dynamic_filter_tx.clone();

                let (tasks, right_stream) = workflow.create_tasks(cancel.clone())?;

                tasks.push(spawn(async move {
                    let left_stream = input_stream;

                    let (left_stream, right_stream) = match dynamic_filter_tx {
                        Some(tx) if is_left_sending_dynamic_filter => (
                            stream_to_dynamic_filter_tx_stream(left_stream, tx, &join.on.0),
                            right_stream,
                        ),
                        Some(tx) => (
                            left_stream,
                            stream_to_dynamic_filter_tx_stream(right_stream, tx, &join.on.1),
                        ),
                        _ => (left_stream, right_stream),
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

                execute_tasks(tasks, cancel).await?;
            }
            WorkflowStep::MuxCount if partial_stream.is_some() => {
                execute_partial_stream(
                    Box::new(PartialMuxCountExecutor::default()),
                    partial_stream.unwrap(),
                    rxs,
                    tx,
                    |count| vec![count_to_log(count); 1],
                    "partial mux count",
                )
                .await?;
            }
            WorkflowStep::MuxCount => {
                let count = mux_count_stream(rx_union_stream(rxs)).await;
                count_to_tx(count, tx).await;
            }
            WorkflowStep::Count => {
                let count = count_stream(rx_union_stream(rxs)).await;
                count_to_tx(count, tx).await;
            }
        }

        Ok(())
    }

    #[inline]
    fn supports_partial_stream(&self) -> bool {
        matches!(
            self,
            WorkflowStep::MuxSummarize(..) | WorkflowStep::MuxCount
        )
    }

    #[inline]
    fn can_partial_passthrough(&self) -> bool {
        match self {
            Self::Scan(..)
            | Self::Project(..)
            | Self::Join(..)
            | Self::Summarize(..)
            | Self::MuxSummarize(..)
            | Self::Count
            | Self::MuxCount
            | Self::Union(..)
            | Self::MuxTopN(..) => false,

            Self::Filter(..)
            | Self::Extend(..)
            | Self::Limit(..)
            | Self::MuxLimit(..)
            | Self::Sort(..)
            | Self::TopN(..) => true,
        }
    }
}

#[instrument(skip_all)]
async fn execute_tasks(mut tasks: WorkflowTasks, cancel: Arc<Notify>) -> Result<()> {
    let start = Instant::now();
    let _guard = scopeguard::guard((), |_| {
        let duration = start.elapsed();
        info!(elapsed_time = ?duration, "Workflow execution time");
    });

    loop {
        tokio::select! {
            // First select cancel, and only then the tasks.
            biased;

            _ = cancel.notified() => {
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

    Ok(())
}

impl Workflow {
    pub fn new_with_partial_stream(
        steps: Vec<WorkflowStep>,
        partial_stream: Option<PartialStream>,
    ) -> Self {
        Self {
            steps,
            partial_stream,
        }
    }

    pub fn new(steps: Vec<WorkflowStep>) -> Self {
        Self::new_with_partial_stream(steps, None)
    }

    /// Get the last mux step that is actually able to passthrough.
    fn get_partial_stream_step_idx(&self) -> Option<usize> {
        self.partial_stream.as_ref()?;

        let mut partial_stream_step_idx = None;

        for (i, step) in self.steps.iter().enumerate() {
            if !step.supports_partial_stream() {
                continue;
            }

            let is_passthrough = self
                .steps
                .get(i + 1..)
                .unwrap_or(&[])
                .iter()
                .all(WorkflowStep::can_partial_passthrough);

            if is_passthrough {
                partial_stream_step_idx = Some(i);
            }
        }

        partial_stream_step_idx
    }

    fn create_tasks(self, cancel: Arc<Notify>) -> Result<(WorkflowTasks, LogStream)> {
        assert!(!self.steps.is_empty());

        let (mut tx, mut rx) = mpsc::channel(1);
        let mut mux_rxs: Vec<mpsc::Receiver<Log>> = Vec::new();
        let partial_stream_step_idx = self.get_partial_stream_step_idx();

        let tasks = FuturesUnordered::new();

        for (i, step) in self.steps.into_iter().enumerate() {
            debug!("Spawning step: {:?}", step);

            let rxs = if matches!(step, WorkflowStep::Scan(..) | WorkflowStep::Union(..)) {
                Vec::new()
            } else {
                std::mem::take(&mut mux_rxs)
            };

            let partial_stream = if partial_stream_step_idx == Some(i) {
                self.partial_stream.clone()
            } else {
                None
            };

            tasks.push(spawn(step.execute(rxs, tx, partial_stream, cancel.clone())));

            mux_rxs.push(rx);
            (tx, rx) = mpsc::channel(1);
        }

        Ok((tasks, rx_union_stream(mux_rxs)))
    }

    pub fn execute(self, cancel: Arc<Notify>) -> Result<LogTryStream> {
        if self.steps.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        let (tasks, mut stream) = self.create_tasks(cancel.clone())?;
        let mut task = spawn(execute_tasks(tasks, cancel));

        Ok(Box::pin(try_stream! {
            let task_alive = loop {
                tokio::select! {
                    log = stream.next() => {
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
                                while let Some(log) = stream.next().await {
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
