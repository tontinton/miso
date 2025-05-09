use std::{future::Future, time::Duration};

use tokio::time::sleep;

/// Repeatedly runs `task_fn` at a given `interval`.
pub async fn run_at_interval<Fut, F>(mut task_fn: F, interval: Duration)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    while task_fn().await {
        sleep(interval).await;
    }
}
