use std::{future::Future, time::Duration};

use tokio::{select, sync::watch, time::sleep};
use tracing::info;

/// Repeatedly runs `task_fn` at a given `interval` until the `shutdown_rx` receives a signal.
pub async fn run_at_interval<Fut, F>(
    mut task_fn: F,
    interval: Duration,
    mut shutdown_rx: watch::Receiver<()>,
    tag: &str,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    let interval_future = async {
        task_fn().await;
        loop {
            sleep(interval).await;
            task_fn().await;
        }
    };

    select! {
        _ = interval_future => {
            panic!("{}: interval future unexpectedly finished.", tag);
        }
        _ = shutdown_rx.changed() => {
            info!("{}: shutdown signal received. Stopping interval task.", tag);
        }
    }
}
