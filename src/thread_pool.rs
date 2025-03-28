use rayon::ThreadPool;
use tokio::sync::oneshot;

pub fn run_on_thread_pool<F, R>(thread_pool: &ThreadPool, task: F) -> oneshot::Receiver<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    thread_pool.spawn(move || {
        if tx.is_closed() {
            return;
        }
        let _ = tx.send(task());
    });
    rx
}
