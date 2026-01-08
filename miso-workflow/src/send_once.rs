/// A wrapper for one-shot moving of `!Send` types across threads.
/// Example: You want to move the last reference of an Rc<T> to another thread.
///
/// # Safety
/// You must ensure the inner value is only ever accessed on one thread.
pub struct SendOnce<T>(Option<T>);
unsafe impl<T> Send for SendOnce<T> {}

impl<T> SendOnce<T> {
    /// # Safety
    /// The inner value must only be accessed on one thread (the destination thread).
    pub unsafe fn new(value: T) -> Self {
        SendOnce(Some(value))
    }

    pub fn take(mut self) -> T {
        self.0.take().expect("SendOnce already taken")
    }
}
