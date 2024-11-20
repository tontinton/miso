use std::sync::Arc;

use async_stream::stream;
use color_eyre::eyre::{bail, Context, Result};
use futures_util::{pin_mut, stream::FuturesUnordered, StreamExt};
use kinded::Kinded;
use serde_json::to_string;
use tokio::{select, spawn, sync::mpsc};
use topn::topn_stream;
use tracing::debug;

use crate::{
    connector::{Connector, QueryHandle, QueryResponse, Split},
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
pub mod topn;
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

    /// The number of records. Only works as the last step.
    Count,
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
                                let response = connector
                                    .query(&collection, split.as_ref(), handle.as_ref())
                                    .await?;
                                match response {
                                    QueryResponse::Logs(stream) => {
                                        stream_to_tx(stream, tx.clone(), &format!("scan({i})"))
                                            .await?
                                    }
                                    QueryResponse::Count(count) => println!("{}", count),
                                }
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
                            logs_vec_to_tx(logs, tx, "sort").await?;
                        }
                        WorkflowStep::TopN(sorts, limit) => {
                            let logs = topn_stream(sorts, limit, rx_stream(rx.unwrap())).await?;
                            logs_vec_to_tx(logs, tx, "top-n").await?;
                        }
                        WorkflowStep::Count => {
                            let mut rx = rx.unwrap();

                            let mut count = 0;
                            while rx.recv().await.is_some() {
                                count += 1;
                            }
                            println!("{}", count);
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
