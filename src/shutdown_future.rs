use std::future::Future;

use parking_lot::Mutex;
use tokio::{select, spawn, sync::watch, task::JoinHandle};
use tracing::{error, info};

#[derive(Debug)]
pub struct ShutdownFuture {
    task: Mutex<Option<JoinHandle<()>>>,
    shutdown_tx: watch::Sender<()>,
    tag: &'static str,
}

async fn shutdown_task<Fut>(fut: Fut, mut shutdown_rx: watch::Receiver<()>, tag: &str)
where
    Fut: Future<Output = ()> + Send + 'static,
{
    select! {
        _ = fut => {
            panic!("{}: Interval future unexpectedly finished.", tag);
        }
        _ = shutdown_rx.changed() => {
            info!("{}: Shutdown signal received. Stopping interval task.", tag);
        }
    }
}

impl ShutdownFuture {
    pub fn new<Fut>(fut: Fut, tag: &'static str) -> Self
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let task = spawn(shutdown_task(fut, shutdown_rx, tag));
        Self {
            task: Mutex::new(Some(task)),
            shutdown_tx,
            tag,
        }
    }

    pub async fn shutdown(&self) {
        let task = {
            let mut guard = self.task.lock();
            let Some(inner) = guard.take() else {
                error!("{}: Called shutdown() more than once", self.tag);
                return;
            };
            inner
        };

        if let Err(e) = self.shutdown_tx.send(()) {
            error!("{}: Failed to send shutdown to task: {:?}", self.tag, e);
        }
        if let Err(e) = task.await {
            error!("{}: Failed to join task: {:?}", self.tag, e);
        }
    }
}
