use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::Poll;

use futures::{Stream, StreamExt};

/// A shared item that can be consumed exactly once.
pub struct TakeOnce<T>(Arc<Mutex<Option<T>>>);

impl<S> Clone for TakeOnce<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> TakeOnce<T> {
    /// Creates a new shared item.
    pub fn new(inner: T) -> Self {
        Self(Arc::new(Mutex::new(Some(inner))))
    }

    /// Checks whether the item was already taken.
    pub fn is_taken(&self) -> bool {
        self.lock().is_none()
    }

    /// Takes the item, making it inaccessible for other owners.
    pub fn take(&mut self) -> Option<T> {
        self.lock().take()
    }

    /// Gets locked access to the underlying option.
    fn lock(&self) -> MutexGuard<'_, Option<T>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A stream that can be retried as long as it hasn't been polled.
pub enum RetriableStream<S> {
    /// The stream has not been polled.
    /// Other owners of `S` can recover it by calling [`TakeOnce::take`].
    New(TakeOnce<S>),
    /// The stream has been polled at least once and is no longer retriable.
    ///
    /// This state is an optimization such that the stream does not have to lock a mutex
    /// on every poll.
    Used(S),
}

impl<S> RetriableStream<S> {
    /// Creates a new retriable stream.
    ///
    /// Returns `None` if the underlying item has already been consumed.
    pub fn new(inner: TakeOnce<S>) -> Option<Self> {
        if inner.is_taken() {
            None
        } else {
            Some(Self::New(inner))
        }
    }

    /// Marks the underlying stream as used and returns a mutable reference to it.
    ///
    /// After calling this function, other owners cannot recover the stream.
    /// It is no longer retriable.
    fn make_used(&mut self) -> Option<&mut S> {
        match self {
            Self::New(s) => {
                let inner = s.take()?;
                *self = Self::Used(inner);
                self.make_used() // recurse once
            }
            Self::Used(s) => Some(s),
        }
    }
}

impl<S: Stream + Unpin> Stream for RetriableStream<S> {
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut().make_used() {
            Some(s) => s.poll_next_unpin(cx),
            None => {
                debug_assert!(false, "stream was taken while streaming");
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::New(s) => s.lock().as_ref().map_or((0, None), |s| s.size_hint()),
            Self::Used(s) => s.size_hint(),
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream;

    use super::*;

    #[tokio::test]
    async fn test_stream_yields_all_items() {
        let inner = TakeOnce::new(stream::iter(vec![1, 2, 3]));
        let s = RetriableStream::new(inner.clone()).unwrap();
        let items: Vec<_> = s.collect().await;
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let inner = TakeOnce::new(stream::iter(Vec::<i32>::new()));
        let s = RetriableStream::new(inner.clone()).unwrap();
        let items: Vec<_> = s.collect().await;
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn test_not_retriable_after_use() {
        let inner = TakeOnce::new(stream::iter(vec![1]));
        let s = RetriableStream::new(inner.clone()).unwrap();
        let _: Vec<_> = s.collect().await;

        assert!(RetriableStream::new(inner).is_none());
    }

    #[tokio::test]
    async fn test_retriable_before_use() {
        let inner = TakeOnce::new(stream::iter(vec![1, 2]));

        // First stream is created but never polled — inner is still available.
        let _s1 = RetriableStream::new(inner.clone()).unwrap();

        // A second stream can be created because the first hasn't taken the inner yet.
        assert!(RetriableStream::new(inner).is_some());
    }

    #[tokio::test]
    async fn test_first_poll_consumes_inner() {
        let inner = TakeOnce::new(stream::iter(vec![1, 2]));

        let mut s1 = RetriableStream::new(inner.clone()).unwrap();
        // Polling the first item transitions s1 from Retriable -> Used.
        assert_eq!(s1.next().await, Some(1));

        // Now the inner is consumed; no new stream can be created.
        assert!(RetriableStream::new(inner).is_none());

        // But s1 still works.
        assert_eq!(s1.next().await, Some(2));
        assert_eq!(s1.next().await, None);
    }

    #[tokio::test]
    async fn test_size_hint_before_poll() {
        let inner = TakeOnce::new(stream::iter(vec![1, 2, 3]));
        let s = RetriableStream::new(inner).unwrap();
        assert_eq!(s.size_hint(), (3, Some(3)));
    }

    #[tokio::test]
    async fn test_size_hint_after_poll() {
        let inner = TakeOnce::new(stream::iter(vec![1, 2, 3]));
        let mut s = RetriableStream::new(inner).unwrap();
        s.next().await;
        // After consuming one item, the stream is Used and delegates to the inner iter.
        assert_eq!(s.size_hint(), (2, Some(2)));
    }

    #[test]
    fn test_clone_shares_inner() {
        let a = TakeOnce::new(stream::iter(vec![1]));
        let b = a.clone();
        // Both point to the same Arc.
        assert!(Arc::ptr_eq(&a.0, &b.0));
    }
}
