use bytes::{Bytes, BytesMut};
#[cfg(any(feature = "processing", test))]
use futures::TryStreamExt;
use futures::{Stream, StreamExt, future, stream};

/// Holds back the last nonempty chunk until the source has successfully reached EOF.
///
/// This makes consumers that stop at a declared content length observe errors in the stream's
/// trailer. Only one chunk of lookahead is retained, and the source is not polled until needed.
#[cfg(any(feature = "processing", test))]
pub fn withhold_last_chunk<S, E>(stream: S) -> impl Stream<Item = Result<Bytes, E>> + Send
where
    S: Stream<Item = Result<Bytes, E>> + Send + Unpin,
    E: Send,
{
    let stream = stream
        .try_filter(|chunk| future::ready(!chunk.is_empty()))
        .fuse();
    stream::try_unfold((stream, None), |(mut stream, mut pending)| async move {
        while let Some(next) = stream.try_next().await? {
            if let Some(previous) = pending.replace(next) {
                return Ok(Some((previous, (stream, pending))));
            }
        }
        Ok(pending.map(|last| (last, (stream, None))))
    })
}

/// Peeks `n` bytes into a `stream`, returning the peeked bytes together with the stream containing
/// all the bytes of the original `stream`.
pub async fn peek_n<S, E>(
    stream: S,
    n: usize,
) -> Result<(Bytes, impl Stream<Item = Result<Bytes, E>> + Send), E>
where
    S: Stream<Item = Result<Bytes, E>> + Send,
    E: Send,
{
    let mut stream = Box::pin(stream);
    let mut head = BytesMut::with_capacity(n);
    while head.len() < n {
        match stream.next().await {
            Some(Ok(chunk)) => head.extend_from_slice(&chunk),
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }
    let mut head = head.freeze();
    let replay = stream::once(future::ready(Ok(head.clone()))).chain(stream);
    head.truncate(n);
    Ok((head, replay))
}

/// The result of [`split_by_size`].
pub enum SizeSplit<S> {
    /// The stream is less than limit and hence buffered into memory.
    Small(Bytes),
    /// The stream exceeds limit and hence is not buffered.
    Large(S),
}

/// Buffers a stream if its size is less than `limit` bytes, else return the stream.
pub async fn split_by_size<S, E>(
    stream: S,
    limit: usize,
) -> Result<SizeSplit<impl Stream<Item = Result<Bytes, E>> + Send>, E>
where
    S: Stream<Item = Result<Bytes, E>> + Send,
    E: Send,
{
    let (head, stream) = peek_n(stream, limit).await?;
    if head.len() < limit {
        Ok(SizeSplit::Small(head))
    } else {
        Ok(SizeSplit::Large(stream))
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[tokio::test]
    async fn last_chunk_waits_for_eof_and_propagates_errors() {
        let (sender, source) = futures::channel::mpsc::unbounded::<io::Result<Bytes>>();
        let mut stream = withhold_last_chunk(source).boxed();

        sender
            .unbounded_send(Ok(Bytes::from_static(b"first")))
            .unwrap();
        assert!(futures::poll!(stream.next()).is_pending());
        sender.unbounded_send(Ok(Bytes::new())).unwrap();
        assert!(futures::poll!(stream.next()).is_pending());
        sender
            .unbounded_send(Ok(Bytes::from_static(b"last")))
            .unwrap();
        assert_eq!(stream.try_next().await.unwrap().unwrap(), "first");
        assert!(futures::poll!(stream.next()).is_pending());

        sender
            .unbounded_send(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad checksum",
            )))
            .unwrap();
        assert_eq!(
            stream.try_next().await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn last_chunk_is_released_on_successful_eof() {
        for chunks in [vec![], vec![""], vec!["", "first", "", "last", ""]] {
            let expected: String = chunks.concat();
            let source = stream::iter(
                chunks
                    .into_iter()
                    .map(|chunk| Ok::<_, io::Error>(Bytes::from(chunk))),
            );
            let chunks: Vec<Bytes> = withhold_last_chunk(source).try_collect().await.unwrap();
            assert_eq!(chunks.concat(), expected.as_bytes());
        }
    }

    #[tokio::test]
    async fn empty_stream_still_validates_its_trailer() {
        let source = stream::iter([
            Ok(Bytes::new()),
            Err(io::Error::new(io::ErrorKind::InvalidData, "bad checksum")),
        ]);
        let mut stream = withhold_last_chunk(source).boxed();
        assert_eq!(
            stream.try_next().await.unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(stream.next().await.is_none());
    }
}
