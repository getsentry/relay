use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use futures::{Stream, StreamExt};

/// A stream adapter that emits chunks with a fixed size.
///
/// All emitted [`Bytes`] have `chunk_size` bytes except for the final chunk, which may be smaller.
pub struct Rechunk<S, E> {
    inner: S,
    chunk_size: usize,
    buffer: BytesMut,
    error: Option<E>,
}

impl<S, E> Rechunk<S, E> {
    /// Creates a new stream adapter that emits chunks of `chunk_size`.
    pub fn new(inner: S, chunk_size: NonZeroUsize) -> Self {
        Self {
            inner,
            chunk_size: chunk_size.get(),
            buffer: BytesMut::new(),
            error: None,
        }
    }
}

impl<S, E> Rechunk<S, E>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    fn flush_remaining(&mut self) -> Poll<Option<Result<Bytes, E>>> {
        if self.buffer.is_empty() {
            Poll::Ready(None)
        } else {
            let chunk = std::mem::take(&mut self.buffer);
            debug_assert!(
                chunk.len() < self.chunk_size,
                "buffer must be smaller than chunk size at this point"
            );
            Poll::Ready(Some(Ok(chunk.freeze())))
        }
    }
}

impl<S, E> Stream for Rechunk<S, E>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // While there is buffered data, flush it.
        if this.buffer.len() >= this.chunk_size {
            let chunk = this.buffer.split_to(this.chunk_size);
            return Poll::Ready(Some(Ok(chunk.freeze())));
        }

        if let Some(error) = this.error.take() {
            // The presence of an error indicates that all data was flushed.
            debug_assert!(this.buffer.is_empty());
            return Poll::Ready(Some(Err(error)));
        }

        match this.inner.poll_next_unpin(cx) {
            Poll::Ready(None) => this.flush_remaining(),
            Poll::Ready(Some(Err(e))) => {
                if this.buffer.is_empty() {
                    Poll::Ready(Some(Err(e)))
                } else {
                    this.error = Some(e);
                    this.flush_remaining()
                }
            }
            Poll::Ready(Some(Ok(bytes))) => {
                this.buffer.put(bytes);
                if this.buffer.len() >= this.chunk_size {
                    let chunk = this.buffer.split_to(this.chunk_size);
                    Poll::Ready(Some(Ok(chunk.freeze())))
                } else {
                    cx.waker().wake_by_ref(); // not called by inner stream, it was ready.
                    Poll::Pending
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt as _;
    use futures::stream;

    use super::*;

    fn bytes(value: &'static [u8]) -> Bytes {
        Bytes::from_static(value)
    }

    async fn collect_chunks(
        chunks: Vec<Result<Bytes, &'static str>>,
        chunk_size: usize,
    ) -> Vec<Result<Bytes, &'static str>> {
        Rechunk::new(stream::iter(chunks), NonZeroUsize::new(chunk_size).unwrap())
            .map(|a| a)
            .collect::<Vec<_>>()
            .await
    }

    #[tokio::test]
    async fn test_splits_large_chunks() {
        let chunks = collect_chunks(vec![Ok(bytes(b"abcdefg"))], 3).await;

        assert_eq!(
            chunks,
            vec![Ok(bytes(b"abc")), Ok(bytes(b"def")), Ok(bytes(b"g"))]
        );
    }

    #[tokio::test]
    async fn test_combines_small_chunks() {
        let chunks = collect_chunks(
            vec![Ok(bytes(b"ab")), Ok(bytes(b"c")), Ok(bytes(b"defg"))],
            3,
        )
        .await;

        assert_eq!(
            chunks,
            vec![Ok(bytes(b"abc")), Ok(bytes(b"def")), Ok(bytes(b"g"))]
        );
    }

    #[tokio::test]
    async fn test_yields_exact_chunks_unchanged() {
        let chunks = collect_chunks(vec![Ok(bytes(b"ab")), Ok(bytes(b"cd"))], 2).await;

        assert_eq!(chunks, vec![Ok(bytes(b"ab")), Ok(bytes(b"cd"))]);
    }

    #[tokio::test]
    async fn test_skips_empty_chunks() {
        let chunks = collect_chunks(
            vec![
                Ok(bytes(b"")),
                Ok(bytes(b"ab")),
                Ok(bytes(b"")),
                Ok(bytes(b"cde")),
                Ok(bytes(b"")),
            ],
            2,
        )
        .await;

        assert_eq!(
            chunks,
            vec![Ok(bytes(b"ab")), Ok(bytes(b"cd")), Ok(bytes(b"e"))]
        );
    }

    #[tokio::test]
    async fn test_empty_stream() {
        let chunks = collect_chunks(Vec::new(), 2).await;

        assert!(chunks.is_empty());
    }

    #[tokio::test]
    async fn test_propagates_errors() {
        let chunks = collect_chunks(vec![Ok(bytes(b"ab")), Err("failed")], 2).await;

        assert_eq!(chunks, vec![Ok(bytes(b"ab")), Err("failed")]);
    }

    #[tokio::test]
    async fn test_forwards_incomplete_chunk_on_error() {
        let chunks = collect_chunks(vec![Ok(bytes(b"a")), Err("failed")], 2).await;

        assert_eq!(chunks, vec![Ok(bytes(b"a")), Err("failed")]);
    }
}
