use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio::time::{timeout, Duration, Instant};

#[derive(Clone, Debug)]
pub struct Watch<T> {
    inner: Arc<Mutex<Option<T>>>,
    notify: Arc<Notify>,
}

impl<T> Default for Watch<T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
            notify: Arc::new(Notify::new()),
        }
    }
}

impl<T> Watch<T> {
    pub fn set(&self, val: T) {
        let mut inner = self.inner.lock().unwrap();
        *inner = Some(val);
        self.notify.notify_waiters();
    }

    pub async fn wait_for(&self, dur: Duration) -> Option<T> {
        let deadline = Instant::now() + dur;

        loop {
            if let Some(val) = self.inner.lock().unwrap().take() {
                return Some(val);
            }

            if timeout(
                deadline.saturating_duration_since(Instant::now()),
                self.notify.notified(),
            )
            .await
            .is_err()
            {
                return None;
            }
        }
    }
}
