use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, future, stream};

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
    let head = head.freeze();
    let replay = stream::once(future::ready(Ok(head.clone()))).chain(stream);
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
