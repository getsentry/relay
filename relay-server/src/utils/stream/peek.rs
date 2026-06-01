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
