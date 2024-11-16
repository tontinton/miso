use std::sync::Arc;

use async_stream::stream;
use color_eyre::eyre::{bail, Context, Result};
use futures_util::{pin_mut, stream::FuturesUnordered, StreamExt};
use kinded::Kinded;
use serde_json::to_string;
use tokio::{select, spawn, sync::mpsc};
use tracing::debug;

use crate::{
    connector::{Connector, QueryHandle, Split},
    log::{Log, LogStream, LogTryStream},
    workflow::{
        filter::filter_stream, limit::limit_stream, project::project_stream, sort::sort_stream,
    },
};

use self::{filter::FilterAst, project::ProjectField, sort::Sort};

pub mod filter;
pub mod limit;
pub mod project;
pub mod sort;
pub mod vrl_utils;

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
pub struct Workflow {
    steps: Vec<WorkflowStep>,
}

fn rx_stream(mut rx: mpsc::Receiver<Log>) -> LogStream {
    Box::pin(stream! {
        while let Some(log) = rx.recv().await {
            yield log
        }
    })
}

async fn stream_to_tx(mut stream: LogTryStream, tx: mpsc::Sender<Log>, tag: &str) -> Result<()> {
    while let Some(log) = stream.next().await {
        if let Err(e) = tx.send(log.context(format!("tx {tag}"))?).await {
            debug!("Closing {} step: {}", tag, e);
            break;
        }
    }
    Ok(())
}

async fn logs_vec_to_tx(logs: Vec<Log>, tx: mpsc::Sender<Log>, tag: &str) -> Result<()> {
    for log in logs {
        if let Err(e) = tx.send(log).await {
            debug!("Closing {} step: {}", tag, e);
            break;
        }
    }
    Ok(())
}

impl Workflow {
    pub fn new(steps: Vec<WorkflowStep>) -> Self {
        Self { steps }
    }

    pub async fn execute(self) -> Result<()> {
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

                async move {
                    match step {
                        WorkflowStep::Scan(Scan {
                            collection,
                            connector,
                            splits,
                            handle,
                        }) => {
                            for (i, split) in splits.into_iter().enumerate() {
                                let stream = connector.query(
                                    &collection,
                                    split.as_ref(),
                                    handle.as_ref(),
                                )?;
                                stream_to_tx(stream, tx.clone(), &format!("scan({i})")).await?;
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
                        WorkflowStep::Sort(sort) => {
                            let logs = sort_stream(sort, rx_stream(rx.unwrap())).await?;
                            logs_vec_to_tx(logs, tx, "sort").await?;
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
