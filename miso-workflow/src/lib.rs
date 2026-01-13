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
    expand::Expand,
    expr::Expr,
    field::Field,
    join::{Join, JoinType},
    log::{Log, LogItem, LogIter, LogStream, LogTryStream},
    project::ProjectField,
    sort::Sort,
    summarize::Summarize,
};
use partial_stream::{add_partial_stream_id, build_partial_stream_id_done_log};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, Span, debug, info, instrument};

use crate::{
    count::CountIter,
    expand::ExpandIter,
    filter::FilterIter,
    join::{DynamicFilterTx, join_rx},
    limit::LimitIter,
    partial_stream::{PartialStream, PartialStreamIter},
    project::ProjectIter,
    rename::RenameIter,
    scan::{Scan, scan_rx},
    sort::sort_rx,
    spawn_thread::{ThreadRx, spawn},
    summarize::create_summarize_iter,
    tee::{Tee, tee_creator},
    topn::{PartialTopNIter, TopNIter},
    union::UnionIter,
};

use self::log_iter_creator::{CancelIterCreator, IterCreator, fn_creator};

mod cancel_iter;
mod count;
pub mod display;
mod expand;
pub mod filter;
pub mod interpreter;
pub mod join;
pub mod limit;
mod log_iter_creator;
mod log_utils;
pub mod partial_stream;
pub mod project;
mod rename;
pub mod scan;
pub mod sort;
mod spawn_thread;
pub mod summarize;
pub mod tee;
pub mod topn;
mod type_tracker;
mod union;

#[cfg(test)]
mod tests;

pub const CHANNEL_CAPACITY: usize = 256;
const MISO_METADATA_FIELD_NAME: &str = "_miso";

pub type AsyncTask = tokio::task::JoinHandle<Result<()>>;
type PipelineRunResult = Result<(IterCreator, Vec<ThreadRx>, Vec<AsyncTask>)>;
type WorkflowRunResult = Result<(Vec<Receiver<Log>>, Vec<ThreadRx>, Vec<AsyncTask>)>;

#[derive(Kinded, Clone, Debug, PartialEq)]
pub enum WorkflowStep {
    /// Run a search query.
    Scan(Scan),

    /// Filter some records.
    Filter(Expr),

    /// Fully transform the input into different records.
    Project(Vec<ProjectField>),

    /// Same as project but keeps original fields, and adds new projected fields to the results.
    Extend(Vec<ProjectField>),

    /// Rename some of the fields in the input.
    /// Basically the same as Project, but keeps unspecified fields unaltered and can only rename.
    Rename(Vec<(/*from=*/ Field, /*to=*/ Field)>),

    /// Expand arrays / objects into multiple records.
    Expand(Expand),

    /// Limit to X amount of records.
    Limit(u64),
    MuxLimit(u64),

    /// Sort records.
    Sort(Vec<Sort>),

    /// Basically like Sort -> Limit, but more memory efficient (holding only N records).
    TopN(Vec<Sort>, u64),
    MuxTopN(Vec<Sort>, u64),

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

    /// Write logs to a sink while also forwarding them downstream.
    Tee(Tee),
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
    Pipeline(IterCreator),

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
    fn into_creator(self) -> IterCreator {
        match self {
            WorkflowRx::None => panic!("first step doesn't have any input logs"),
            WorkflowRx::Pipeline(creator) => creator,
            WorkflowRx::MuxPipelines(rxs) => fn_creator(move || rxs_to_iter(rxs)),
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

        let creator: IterCreator = match self {
            WorkflowStep::Scan(scan) => {
                assert!(matches!(rx, WorkflowRx::None));
                let (item_rx, task) = scan_rx(scan, cancel);
                async_tasks.push(task);
                fn_creator(move || Box::new(LogItemReceiverIter { rx: item_rx }))
            }
            WorkflowStep::Union(workflow) if workflow.steps.is_empty() => {
                fn_creator(|| Box::new(iter::empty()))
            }
            WorkflowStep::Union(workflow) => {
                assert!(matches!(rx, WorkflowRx::None));
                let (rxs, inner_threads, inner_async_tasks) = workflow.create_pipelines(cancel)?;
                threads.extend(inner_threads);
                async_tasks.extend(inner_async_tasks);
                fn_creator(move || rxs_to_iter(rxs))
            }
            WorkflowStep::Filter(expr) => {
                let prev = rx.into_creator();
                fn_creator(move || Box::new(FilterIter::new(prev.create(), expr)))
            }
            WorkflowStep::Project(fields) => {
                let prev = rx.into_creator();
                fn_creator(move || Box::new(ProjectIter::new_project(prev.create(), fields)))
            }
            WorkflowStep::Extend(fields) => {
                let prev = rx.into_creator();
                fn_creator(move || Box::new(ProjectIter::new_extend(prev.create(), fields)))
            }
            WorkflowStep::Rename(renames) => {
                let prev = rx.into_creator();
                fn_creator(move || Box::new(RenameIter::new(prev.create(), renames)))
            }
            WorkflowStep::Expand(expand) => {
                let prev = rx.into_creator();
                fn_creator(move || Box::new(ExpandIter::new(prev.create(), expand)))
            }
            WorkflowStep::Limit(limit) | WorkflowStep::MuxLimit(limit) => {
                let prev = rx.into_creator();
                fn_creator(move || Box::new(LimitIter::new(prev.create(), limit)))
            }
            WorkflowStep::Sort(sorts) => {
                let (item_rx, thread) = sort_rx(rx.into_creator(), sorts, cancel);
                threads.push(thread);
                fn_creator(move || Box::new(LogItemReceiverIter { rx: item_rx }))
            }
            WorkflowStep::MuxTopN(sorts, limit) if partial_stream.is_some() => {
                let prev = rx.into_creator();
                let partial_stream = partial_stream.unwrap();
                fn_creator(move || {
                    Box::new(PartialStreamIter::new(
                        Box::new(PartialTopNIter::new(prev.create(), sorts, limit)),
                        partial_stream,
                    ))
                })
            }
            WorkflowStep::TopN(sorts, limit) | WorkflowStep::MuxTopN(sorts, limit) => {
                let prev = rx.into_creator();
                fn_creator(move || Box::new(TopNIter::new(prev.create(), sorts, limit as usize)))
            }
            WorkflowStep::MuxSummarize(config) if partial_stream.is_some() => {
                let prev = rx.into_creator();
                let partial_stream = partial_stream.unwrap();
                fn_creator(move || {
                    Box::new(PartialStreamIter::new(
                        create_summarize_iter(prev.create(), config, true),
                        partial_stream,
                    ))
                })
            }
            WorkflowStep::MuxSummarize(config) => {
                let prev = rx.into_creator();
                fn_creator(move || create_summarize_iter(prev.create(), config, true))
            }
            WorkflowStep::Summarize(config) => {
                let prev = rx.into_creator();
                fn_creator(move || create_summarize_iter(prev.create(), config, false))
            }
            WorkflowStep::Join(join, workflow) if workflow.steps.is_empty() => {
                if matches!(join.type_, JoinType::Left | JoinType::Outer) {
                    rx.into_creator()
                } else {
                    fn_creator(|| Box::new(iter::empty()))
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

                let (join_out_rx, join_threads) =
                    join_rx(join, rx, right_rxs, dynamic_filter_tx, cancel);
                threads.extend(join_threads);

                fn_creator(move || Box::new(join_out_rx.into_iter().map(LogItem::Log)))
            }
            WorkflowStep::MuxCount if partial_stream.is_some() => {
                let prev = rx.into_creator();
                let partial_stream = partial_stream.unwrap();
                fn_creator(move || {
                    Box::new(PartialStreamIter::new(
                        Box::new(CountIter::new_mux(prev.create())),
                        partial_stream,
                    ))
                })
            }
            WorkflowStep::MuxCount => {
                let prev = rx.into_creator();
                fn_creator(move || Box::new(CountIter::new_mux(prev.create())))
            }
            WorkflowStep::Count => {
                let prev = rx.into_creator();
                fn_creator(move || Box::new(CountIter::new_simple(prev.create())))
            }
            WorkflowStep::Tee(tee) => {
                let (creator, task) = tee_creator(rx.into_creator(), tee);
                async_tasks.push(task);
                creator
            }
        };

        Ok((creator, threads, async_tasks))
    }

    #[inline]
    fn supports_partial_stream(&self) -> bool {
        matches!(
            self,
            // Ignore MuxLimit - limit doesn't have any state to be partially streamed, it simply
            // passes logs until reaching a limit.
            WorkflowStep::MuxSummarize(..) | WorkflowStep::MuxCount | WorkflowStep::MuxTopN(..)
        )
    }

    #[inline]
    fn can_partial_passthrough(&self) -> bool {
        match self {
            Self::Scan(..)
            | Self::Join(..)
            | Self::Summarize(..)
            | Self::MuxSummarize(..)
            | Self::Union(..)
            | Self::MuxTopN(..)
            | Self::Sort(..) => false,

            Self::Filter(..)
            | Self::Project(..)
            | Self::Extend(..)
            | Self::Rename(..)
            | Self::Expand(..)
            | Self::Limit(..)
            | Self::MuxLimit(..)
            | Self::TopN(..)
            | Self::Count
            | Self::MuxCount
            | Self::Tee(..) => true,
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
        let (creator, inner_threads, inner_async_tasks) = step
            .execute(workflow_rx, partial_stream, cancel.clone())
            .context("execute pipeline step")?;

        threads.extend(inner_threads);
        async_tasks.extend(inner_async_tasks);

        workflow_rx = WorkflowRx::Pipeline(creator);
    }

    let WorkflowRx::Pipeline(creator) = workflow_rx else {
        panic!("last rx must be of a pipeline");
    };

    Ok((creator, threads, async_tasks))
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
            debug!("Closing pipeline {:#}", e);
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

            let (tx, rx) = flume::bounded(CHANNEL_CAPACITY);

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

            let (creator, inner_threads, inner_async_tasks) =
                prepare_execute_pipeline(pipeline, rxs_opt, cancel.clone())
                    .context("prepare execute pipeline")?;
            threads.extend(inner_threads);
            async_tasks.extend(inner_async_tasks);

            let creator = CancelIterCreator::wrap(creator, cancel.clone());
            threads.push(spawn(
                move || execute_pipeline(creator.create(), tx).context("pipe iter to tx"),
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

        let mut task = tokio::spawn(
            wait_for_threads_and_async_tasks(threads, async_tasks, cancel)
                .instrument(Span::current()),
        );

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
