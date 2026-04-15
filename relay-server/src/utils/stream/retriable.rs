use std::sync::{Arc, Mutex, PoisonError};

use futures::{Stream, StreamExt};

pub struct Retriable<S>(Arc<Mutex<Option<S>>>);

impl<S> Clone for Retriable<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<S> Retriable<S> {
    pub fn new(inner: S) -> Self {
        Self(Arc::new(Mutex::new(Some(inner))))
    }

    fn retriable(&self) -> Option<&Self> {
        match self.0.lock().as_deref() {
            Ok(Some(_)) => Some(self),
            _ => None,
        }
    }
}

pub enum RetriableStream<S> {
    Retriable(Retriable<S>),
    Used(S),
}

impl<S> RetriableStream<S> {
    /// Creates a new retriable stream.
    ///
    /// Returns `None` if the underlying item has already been consumed.
    pub fn new(inner: &Retriable<S>) -> Option<Self> {
        inner
            .retriable()
            .map(|inner| Self::Retriable(Retriable::<S>::clone(inner)))
    }

    fn get_mut(&mut self) -> &mut S {
        match self {
            Self::Retriable(s) => {
                let inner =
                    s.0.lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take()
                        .expect("should be retriable");
                *self = Self::Used(inner);
                self.get_mut() // recurse once
            }
            Self::Used(s) => s,
        }
    }
}

impl<S: Stream + Unpin> Stream for RetriableStream<S> {
    type Item = S::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let s = self.get_mut().get_mut();
        s.poll_next_unpin(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Retriable(s) => {
                s.0.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .as_ref()
                    .expect("should be retriable")
                    .size_hint()
            }
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
        let retriable = Retriable::new(stream::iter(vec![1, 2, 3]));
        let s = RetriableStream::new(&retriable).unwrap();
        let items: Vec<_> = s.collect().await;
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let retriable = Retriable::new(stream::iter(Vec::<i32>::new()));
        let s = RetriableStream::new(&retriable).unwrap();
        let items: Vec<_> = s.collect().await;
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn test_not_retriable_after_use() {
        let retriable = Retriable::new(stream::iter(vec![1]));
        let s = RetriableStream::new(&retriable).unwrap();
        let _: Vec<_> = s.collect().await;

        assert!(RetriableStream::new(&retriable).is_none());
    }

    #[tokio::test]
    async fn test_retriable_before_use() {
        let retriable = Retriable::new(stream::iter(vec![1, 2]));

        // First stream is created but never polled — inner is still available.
        let _s1 = RetriableStream::new(&retriable).unwrap();

        // A second stream can be created because the first hasn't taken the inner yet.
        assert!(RetriableStream::new(&retriable).is_some());
    }

    #[tokio::test]
    async fn test_first_poll_consumes_inner() {
        let retriable = Retriable::new(stream::iter(vec![1, 2]));

        let mut s1 = RetriableStream::new(&retriable).unwrap();
        // Polling the first item transitions s1 from Retriable -> Used.
        assert_eq!(s1.next().await, Some(1));

        // Now the inner is consumed; no new stream can be created.
        assert!(RetriableStream::new(&retriable).is_none());

        // But s1 still works.
        assert_eq!(s1.next().await, Some(2));
        assert_eq!(s1.next().await, None);
    }

    #[tokio::test]
    async fn test_size_hint_before_poll() {
        let retriable = Retriable::new(stream::iter(vec![1, 2, 3]));
        let s = RetriableStream::new(&retriable).unwrap();
        assert_eq!(s.size_hint(), (3, Some(3)));
    }

    #[tokio::test]
    async fn test_size_hint_after_poll() {
        let retriable = Retriable::new(stream::iter(vec![1, 2, 3]));
        let mut s = RetriableStream::new(&retriable).unwrap();
        s.next().await;
        // After consuming one item, the stream is Used and delegates to the inner iter.
        assert_eq!(s.size_hint(), (2, Some(2)));
    }

    #[test]
    fn test_clone_shares_inner() {
        let retriable = Retriable::new(stream::iter(vec![1]));
        let clone = retriable.clone();
        // Both point to the same Arc.
        assert!(Arc::ptr_eq(&retriable.0, &clone.0));
    }
}
