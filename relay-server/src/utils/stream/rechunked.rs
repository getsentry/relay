use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt};

/// A stream adapter that emits chunks with a fixed size.
///
/// All emitted [`Bytes`] have `chunk_size` bytes except for the final chunk, which may be smaller.
pub struct Rechunk<S> {
    inner: S,
    chunk_size: usize,
    buffer: BytesMut,
    current: Option<Bytes>,
    done: bool,
}

impl<S> Rechunk<S> {
    /// Creates a new stream adapter that emits chunks of `chunk_size`.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is zero.
    pub fn new(inner: S, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk size must be greater than zero");

        Self {
            inner,
            chunk_size,
            buffer: BytesMut::with_capacity(chunk_size),
            current: None,
            done: false,
        }
    }
}

impl<S, E> Stream for Rechunk<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = Result<Bytes, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if this.done {
                if this.buffer.is_empty() {
                    return Poll::Ready(None);
                }

                return Poll::Ready(Some(Ok(this.buffer.split().freeze())));
            }

            if this.buffer.len() == this.chunk_size {
                return Poll::Ready(Some(Ok(this.buffer.split_to(this.chunk_size).freeze())));
            }

            if let Some(mut current) = this.current.take() {
                if current.is_empty() {
                    continue;
                }

                if this.buffer.is_empty() && current.len() >= this.chunk_size {
                    let chunk = current.split_to(this.chunk_size);
                    if !current.is_empty() {
                        this.current = Some(current);
                    }
                    return Poll::Ready(Some(Ok(chunk)));
                }

                let remaining = this.chunk_size - this.buffer.len();
                let part = current.split_to(remaining.min(current.len()));
                this.buffer.extend_from_slice(&part);

                if !current.is_empty() {
                    this.current = Some(current);
                }

                continue;
            }

            match this.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    this.current = Some(bytes);
                }
                Poll::Ready(Some(Err(error))) => {
                    this.buffer.clear();
                    this.current = None;
                    this.done = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    this.done = true;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
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
        Rechunk::new(stream::iter(chunks), chunk_size)
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
    async fn test_drops_incomplete_chunk_on_error() {
        let chunks = collect_chunks(vec![Ok(bytes(b"a")), Err("failed")], 2).await;

        assert_eq!(chunks, vec![Err("failed")]);
    }

    #[test]
    #[should_panic(expected = "chunk size must be greater than zero")]
    fn test_rejects_zero_chunk_size() {
        let _ = Rechunk::new(stream::empty::<Result<Bytes, &'static str>>(), 0);
    }
}
