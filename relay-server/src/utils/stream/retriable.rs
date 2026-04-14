use futures::{Stream, StreamExt};
use tokio::sync::oneshot;

/// Wrapper for streams that makes them retriable, as long as they haven't been polled.
pub enum RetriableStream<S> {
    Retriable(Recoverable<S>),
    Taken(S),
}

impl<S> RetriableStream<S> {
    /// Creates a retriable stream and returns the recovery handle.
    pub fn create(inner: S) -> (Self, oneshot::Receiver<S>) {
        let (recoverable, rx) = Recoverable::create(inner);
        (Self::Retriable(recoverable), rx)
    }

    fn get_mut(&mut self) -> &mut S {
        match self {
            Self::Retriable(s) => {
                *self = Self::Taken(s.take().expect("should be recoverable"));
                self.get_mut() // recurse once
            }
            Self::Taken(s) => s,
        }
    }
    fn as_ref(&self) -> &S {
        match self {
            Self::Retriable(recoverable) => {
                &recoverable.0.as_ref().expect("should be recoverable").0
            }
            Self::Taken(s) => s,
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
        self.as_ref().size_hint()
    }
}

/// A wrapper that sends the contained data back to its creator on destruction, unless it's been used.
pub struct Recoverable<T>(Option<(T, oneshot::Sender<T>)>);

impl<T> Recoverable<T> {
    fn create(inner: T) -> (Self, oneshot::Receiver<T>) {
        let (tx, rx) = oneshot::channel();
        (Self(Some((inner, tx))), rx)
    }

    fn take(&mut self) -> Option<T> {
        self.0.take().map(|(inner, _)| inner)
    }
}

impl<T> Drop for Recoverable<T> {
    fn drop(&mut self) {
        if let Some((inner, tx)) = self.0.take() {
            let _ = tx.send(inner);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[tokio::test]
    async fn recoverable_returns_value_on_drop() {
        let (recoverable, rx) = Recoverable::create(42);
        drop(recoverable);
        assert_eq!(rx.await.unwrap(), 42);
    }

    #[tokio::test]
    async fn recoverable_take_prevents_recovery() {
        let (mut recoverable, rx) = Recoverable::create(42);
        assert_eq!(recoverable.take(), Some(42));
        drop(recoverable);
        assert!(rx.await.is_err());
    }

    #[tokio::test]
    async fn retriable_stream_polls_items() {
        let inner = stream::iter(vec![1, 2, 3]);
        let (mut retriable, _rx) = RetriableStream::create(inner);
        assert_eq!(retriable.next().await, Some(1));
        assert_eq!(retriable.next().await, Some(2));
        assert_eq!(retriable.next().await, Some(3));
        assert_eq!(retriable.next().await, None);
    }

    #[tokio::test]
    async fn retriable_stream_recovers_on_drop_before_poll() {
        let inner = stream::iter(vec![1, 2, 3]);
        let (retriable, rx) = RetriableStream::create(inner);
        drop(retriable);
        let mut recovered = rx.await.unwrap();
        assert_eq!(recovered.next().await, Some(1));
    }

    #[tokio::test]
    async fn retriable_stream_not_recoverable_after_poll() {
        let inner = stream::iter(vec![1, 2, 3]);
        let (mut retriable, rx) = RetriableStream::create(inner);
        // Polling transitions to Taken state.
        assert_eq!(retriable.next().await, Some(1));
        drop(retriable);
        assert!(rx.await.is_err());
    }
}
