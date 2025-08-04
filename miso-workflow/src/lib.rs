use std::{iter, time::Instant};

use async_stream::try_stream;
use color_eyre::eyre::{Context, Result, eyre};
use flume::{Receiver, Sender};
use futures_util::{
    FutureExt, StreamExt,
    stream::{self, FuturesUnordered, select_all},
};
use kinded::Kinded;
use miso_workflow_types::{
    expr::Expr,
    join::{Join, JoinType},
    log::{Log, LogItem, LogIter, LogStream, LogTryStream},
    project::ProjectField,
    sort::Sort,
    summarize::Summarize,
};
use partial_stream::{add_partial_stream_id, build_partial_stream_id_done_log};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument};

use crate::{
    count::CountIter,
    filter::FilterIter,
    join::{DynamicFilterTx, join_rx},
    limit::LimitIter,
    partial_stream::{PartialStream, PartialStreamIter, UnionIter},
    project::ProjectIter,
    scan::{Scan, scan_rx},
    sort::sort_rx,
    spawn_thread::{ThreadRx, spawn},
    summarize::create_summarize_iter,
    topn::{PartialTopNIter, TopNIter},
};

use self::{cancel_iter::CancelIter, send_once::SendOnce};

mod cancel_iter;
mod count;
mod display;
pub mod filter;
pub mod interpreter;
pub mod join;
pub mod limit;
mod log_utils;
pub mod partial_stream;
pub mod project;
pub mod scan;
mod send_once;
pub mod serde_json_utils;
pub mod sort;
pub mod sortable_value;
mod spawn_thread;
pub mod summarize;
pub mod topn;

#[cfg(test)]
mod tests;

const MISO_METADATA_FIELD_NAME: &str = "_miso";

pub type AsyncTask = tokio::task::JoinHandle<Result<()>>;
type PipelineRunResult = Result<(LogIter, Vec<ThreadRx>, Vec<AsyncTask>)>;
type WorkflowRunResult = Result<(Vec<Receiver<Log>>, Vec<ThreadRx>, Vec<AsyncTask>)>;

#[derive(Kinded, Clone, Debug, PartialEq)]
pub enum WorkflowStep {
    /// Run a search query.
    Scan(Scan),

    /// Filter some records.
    Filter(Expr),

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

pub enum WorkflowRx {
    /// First step - A step that generates input logs.
    None,

    /// Continue doing something with the input logs in the same pipeline.
    Pipeline(LogIter),

    /// Aggregate multiple pipelines.
    MuxPipelines(Vec<Receiver<Log>>),
}

pub struct LogItemReceiverIter {
    rx: Receiver<LogItem>,
}

impl Iterator for LogItemReceiverIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok()
    }
}

pub struct LogReceiverIter {
    rx: Receiver<Log>,
}

impl Iterator for LogReceiverIter {
    type Item = LogItem;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx.recv().ok().map(LogItem::Log)
    }
}

fn rxs_to_iter(mut rxs: Vec<Receiver<Log>>) -> LogIter {
    if rxs.len() == 1 {
        Box::new(LogReceiverIter {
            rx: rxs.pop().unwrap(),
        })
    } else {
        Box::new(UnionIter::new(rxs))
    }
}

impl WorkflowRx {
    fn into_iter(self) -> LogIter {
        match self {
            WorkflowRx::None => panic!("first step doesn't have any input logs"),
            WorkflowRx::Pipeline(logs) => logs,
            WorkflowRx::MuxPipelines(rxs) => rxs_to_iter(rxs),
        }
    }
}

impl WorkflowStep {
    #[instrument(skip_all, fields(step = %self))]
    fn execute(
        self: WorkflowStep,
        rx: WorkflowRx,
        partial_stream: Option<PartialStream>,
        cancel: CancellationToken,
    ) -> PipelineRunResult {
        let mut threads = Vec::new();
        let mut async_tasks = Vec::new();

        let iter = match self {
            WorkflowStep::Scan(scan) => {
                assert!(matches!(rx, WorkflowRx::None));
                let (rx, task) = scan_rx(scan, cancel);
                async_tasks.push(task);
                Box::new(LogItemReceiverIter { rx })
            }
            WorkflowStep::Union(workflow) if workflow.steps.is_empty() => {
                Box::new(iter::empty()) as LogIter
            }
            WorkflowStep::Union(workflow) => {
                assert!(matches!(rx, WorkflowRx::None));
                let (rxs, inner_threads, inner_async_tasks) = workflow.create_pipelines(cancel)?;
                threads.extend(inner_threads);
                async_tasks.extend(inner_async_tasks);
                rxs_to_iter(rxs)
            }
            WorkflowStep::Filter(expr) => Box::new(FilterIter::new(rx.into_iter(), expr)),
            WorkflowStep::Project(fields) => {
                Box::new(ProjectIter::new_project(rx.into_iter(), fields))
            }
            WorkflowStep::Extend(fields) => {
                Box::new(ProjectIter::new_extend(rx.into_iter(), fields))
            }
            WorkflowStep::Limit(limit) | WorkflowStep::MuxLimit(limit) => {
                Box::new(LimitIter::new(rx.into_iter(), limit))
            }
            WorkflowStep::Sort(sorts) => {
                let (rx, thread) = sort_rx(rx.into_iter(), sorts, cancel);
                threads.push(thread);
                Box::new(LogItemReceiverIter { rx })
            }
            WorkflowStep::MuxTopN(sorts, limit) if partial_stream.is_some() => {
                Box::new(PartialStreamIter::new(
                    Box::new(PartialTopNIter::new(rx.into_iter(), sorts, limit)),
                    partial_stream.unwrap(),
                ))
            }
            WorkflowStep::TopN(sorts, limit) | WorkflowStep::MuxTopN(sorts, limit) => {
                Box::new(TopNIter::new(rx.into_iter(), sorts, limit as usize))
            }
            WorkflowStep::MuxSummarize(config) if partial_stream.is_some() => {
                Box::new(PartialStreamIter::new(
                    create_summarize_iter(rx.into_iter(), config),
                    partial_stream.unwrap(),
                ))
            }
            WorkflowStep::Summarize(config) | WorkflowStep::MuxSummarize(config) => {
                create_summarize_iter(rx.into_iter(), config)
            }
            WorkflowStep::Join(join, workflow) if workflow.steps.is_empty() => {
                if matches!(join.type_, JoinType::Left | JoinType::Outer) {
                    rx.into_iter()
                } else {
                    Box::new(iter::empty())
                }
            }
            WorkflowStep::Join(join, workflow) => {
                let WorkflowStep::Scan(scan) = &workflow.steps[0] else {
                    panic!("scan not as first step in join?");
                };
                let is_left_sending_dynamic_filter = scan.dynamic_filter_rx.is_some();
                let dynamic_filter_tx = scan.dynamic_filter_tx.clone().map(|tx| {
                    let field = if is_left_sending_dynamic_filter {
                        join.on.0.clone()
                    } else {
                        join.on.1.clone()
                    };
                    DynamicFilterTx::new(tx, is_left_sending_dynamic_filter, field)
                });

                let (right_rxs, inner_threads, inner_async_tasks) =
                    workflow.create_pipelines(cancel.clone())?;

                threads.extend(inner_threads);
                async_tasks.extend(inner_async_tasks);

                let (rx, join_threads) = join_rx(join, rx, right_rxs, dynamic_filter_tx, cancel);
                threads.extend(join_threads);

                Box::new(rx.into_iter().map(LogItem::Log))
            }
            WorkflowStep::MuxCount if partial_stream.is_some() => Box::new(PartialStreamIter::new(
                Box::new(CountIter::new_mux(rx.into_iter())),
                partial_stream.unwrap(),
            )),
            WorkflowStep::MuxCount => Box::new(CountIter::new_mux(rx.into_iter())),
            WorkflowStep::Count => Box::new(CountIter::new_simple(rx.into_iter())),
        };

        Ok((iter, threads, async_tasks))
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
            | Self::Union(..)
            | Self::MuxTopN(..)
            | Self::Sort(..) => false,

            Self::Filter(..)
            | Self::Extend(..)
            | Self::Limit(..)
            | Self::MuxLimit(..)
            | Self::TopN(..)
            | Self::Count
            | Self::MuxCount => true,
        }
    }
}

#[instrument(skip_all)]
fn prepare_execute_pipeline(
    pipeline: Vec<(WorkflowStep, Option<PartialStream>)>,
    rxs_opt: Option<Vec<Receiver<Log>>>,
    cancel: CancellationToken,
) -> PipelineRunResult {
    let mut workflow_rx = if let Some(rxs) = rxs_opt {
        WorkflowRx::MuxPipelines(rxs)
    } else {
        WorkflowRx::None
    };

    let mut threads = Vec::new();
    let mut async_tasks = Vec::new();
    for (step, partial_stream) in pipeline {
        let (iter, inner_threads, inner_async_tasks) = step
            .execute(workflow_rx, partial_stream, cancel.clone())
            .context("execute pipeline step")?;

        threads.extend(inner_threads);
        async_tasks.extend(inner_async_tasks);

        workflow_rx = WorkflowRx::Pipeline(iter);
    }

    let WorkflowRx::Pipeline(iter) = workflow_rx else {
        panic!("last rx must be of a pipeline");
    };

    Ok((iter, threads, async_tasks))
}

#[instrument(skip_all)]
fn execute_pipeline(iter: impl Iterator<Item = LogItem>, tx: Sender<Log>) -> Result<()> {
    for item in iter {
        let log = match item {
            LogItem::Err(e) => return Err(e),
            LogItem::Log(log) => log,
            LogItem::PartialStreamLog(log, id) => add_partial_stream_id(log, id),
            LogItem::PartialStreamDone(id) => build_partial_stream_id_done_log(id),
            LogItem::UnionSomePipelineDone => continue,
        };

        if let Err(e) = tx.send(log) {
            debug!("Closing pipeline {:?}", e);
            break;
        }
    }

    Ok(())
}

#[instrument(skip_all)]
async fn wait_for_threads_and_async_tasks(
    threads: Vec<ThreadRx>,
    async_tasks: Vec<AsyncTask>,
    cancel: CancellationToken,
) -> Result<()> {
    let start = Instant::now();
    let _guard = scopeguard::guard((), |_| {
        let duration = start.elapsed();
        info!(elapsed_time = ?duration, "Workflow execution time");
    });

    let mut futs = threads
        .into_iter()
        .map(|rx| async move { rx.await.map_err(|_| eyre!("thread done")) }.boxed())
        .chain(async_tasks.into_iter().map(|jh| {
            async move { jh.await.map_err(|e| eyre!("async task panic: {e:?}")) }.boxed()
        }))
        .collect::<FuturesUnordered<_>>();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Workflow cancelled");
                break;
            }
            fut_result = futs.next() => {
                let Some(join_result) = fut_result else {
                    break;
                };

                let result = join_result.context("join result")?;
                if let Err(e) = result {
                    cancel.cancel();
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

    fn create_pipelines(mut self, cancel: CancellationToken) -> WorkflowRunResult {
        assert!(!self.steps.is_empty());

        let partial_stream_step_idx = self.get_partial_stream_step_idx();
        let mut pipelines = Vec::new();
        let mut pipeline = Vec::new();
        let mut last_was_union = false;

        for (i, step) in self.steps.into_iter().enumerate() {
            let is_union = matches!(step, WorkflowStep::Union(..));
            if is_union || last_was_union {
                pipelines.push(std::mem::take(&mut pipeline));
            }

            let partial_stream = if partial_stream_step_idx == Some(i) {
                self.partial_stream.take()
            } else {
                None
            };

            last_was_union = is_union;
            pipeline.push((step, partial_stream));
        }

        if !pipeline.is_empty() {
            pipelines.push(pipeline);
        }

        let mut rxs = Vec::new();
        let mut threads = Vec::new();
        let mut async_tasks = Vec::new();

        for pipeline in pipelines {
            assert!(!pipeline.is_empty());

            let (tx, rx) = flume::bounded(1);

            let skip_rx = matches!(
                pipeline[0].0,
                WorkflowStep::Scan(..) | WorkflowStep::Union(..)
            );

            let rxs_opt = if skip_rx {
                rxs.push(rx);
                None
            } else {
                Some(std::mem::replace(&mut rxs, vec![rx]))
            };

            let (iter, inner_threads, inner_async_tasks) =
                prepare_execute_pipeline(pipeline, rxs_opt, cancel.clone())
                    .context("prepare execute pipeline")?;
            threads.extend(inner_threads);
            async_tasks.extend(inner_async_tasks);

            let iter = SendOnce::new(CancelIter::new(iter, cancel.clone()));
            threads.push(spawn(
                move || execute_pipeline(iter.take(), tx).context("pipe iter to tx"),
                "pipeline",
            ));
        }

        Ok((rxs, threads, async_tasks))
    }

    pub fn execute(self, cancel: CancellationToken) -> Result<LogTryStream> {
        if self.steps.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        let cancel_clone = cancel.clone();
        let (rxs, threads, async_tasks) = self
            .create_pipelines(cancel_clone)
            .context("create pipelines")?;

        let mut streams: Vec<_> = rxs.into_iter().map(|rx| rx.into_stream()).collect();
        let mut stream: LogStream = if streams.len() == 1 {
            Box::pin(streams.pop().unwrap())
        } else {
            Box::pin(select_all(streams))
        };

        let mut task = tokio::spawn(wait_for_threads_and_async_tasks(
            threads,
            async_tasks,
            cancel,
        ));

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
