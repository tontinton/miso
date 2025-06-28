use tokio_util::sync::CancellationToken;

pub struct CancelIter<I> {
    input: I,
    cancel: CancellationToken,
}

impl<I> CancelIter<I> {
    pub fn new(input: I, cancel: CancellationToken) -> Self {
        Self { input, cancel }
    }
}

impl<I: Iterator> Iterator for CancelIter<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cancel.is_cancelled() {
            None
        } else {
            self.input.next()
        }
    }
}
