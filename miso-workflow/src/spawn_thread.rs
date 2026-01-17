use color_eyre::Result;
use tokio::sync::oneshot;
use tracing::{debug, info_span};

use miso_common::metrics::METRICS;

pub type ThreadRx = oneshot::Receiver<Result<()>>;

pub fn spawn<F>(f: F, tag: &'static str) -> ThreadRx
where
    F: FnOnce() -> Result<()>,
    F: Send + 'static,
{
    let (thread_tx, thread_rx) = oneshot::channel();
    let span = info_span!("thread", tag);

    std::thread::spawn(move || {
        let _span_guard = span.enter();

        METRICS.alive_threads.with_label_values(&[tag]).inc();
        let _guard = scopeguard::guard((), |_| {
            METRICS.alive_threads.with_label_values(&[tag]).dec();
        });

        if let Err(e) = thread_tx.send(f()) {
            debug!("Failed to send thread result ({tag}): {e:?}");
        }
    });
    thread_rx
}
