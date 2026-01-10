use miso_workflow_types::log::LogIter;
use tokio_util::sync::CancellationToken;

use crate::cancel_iter::CancelIter;

pub type IterCreator = Box<dyn LogIterCreator>;

/// Keep the LogIter non Send, by sending a struct implementing this trait (which is Send) to another thread,
/// and on it create the log iter.
pub trait LogIterCreator: Send {
    fn create(self: Box<Self>) -> LogIter;
}

struct FnLogIterCreator<F>(F);

impl<F> LogIterCreator for FnLogIterCreator<F>
where
    F: FnOnce() -> LogIter + Send,
{
    fn create(self: Box<Self>) -> LogIter {
        (self.0)()
    }
}

pub fn fn_creator<F>(f: F) -> IterCreator
where
    F: FnOnce() -> LogIter + Send + 'static,
{
    Box::new(FnLogIterCreator(f))
}

pub struct CancelIterCreator {
    inner: IterCreator,
    cancel: CancellationToken,
}

impl CancelIterCreator {
    pub fn wrap(inner: IterCreator, cancel: CancellationToken) -> IterCreator {
        Box::new(Self { inner, cancel })
    }
}

impl LogIterCreator for CancelIterCreator {
    fn create(self: Box<Self>) -> LogIter {
        Box::new(CancelIter::new(self.inner.create(), self.cancel))
    }
}
