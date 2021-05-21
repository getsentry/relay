use bytes::{Bytes, BytesMut};
use futures::{Async, Poll, Stream};
use smallvec::SmallVec;

use crate::extractors::SharedPayload;

/// A request body adapter that peeks the first line of a multi-line body.
///
/// `PeekLine` is a future created from a request's [`Payload`]. It is especially designed to be
/// used together with [`SharedPayload`](crate::extractors::SharedPayload), since it polls an
/// underlying payload and places the polled chunks back into the payload when completing.
///
/// If the payload does not contain a newline, the entire payload is returned by the future. To
/// contrain this, use `limit` to set a maximum size returned by the future.
///
/// Resolves to `Some(Bytes)` if a newline is found within the size limit, or the entire payload is
/// smaller than the size limit. Otherwise, resolves to `None`. Any errors on the underlying stream
/// are returned without change.
pub struct PeekLine {
    payload: SharedPayload,
    chunks: SmallVec<[Bytes; 3]>,
    len: usize,
    limit: Option<usize>,
}

impl PeekLine {
    /// Creates a new peek line future from the given payload.
    pub fn new(payload: SharedPayload) -> Self {
        Self {
            payload,
            chunks: SmallVec::new(),
            len: 0,
            limit: None,
        }
    }

    /// Adds a maximum size of the future return value.
    ///
    /// Note that the underlying stream may return more data than the configured limit. The future
    /// will still never resolve more than the limit set.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    fn len_exceeded(&self, len: usize) -> bool {
        if let Some(limit) = self.limit {
            if len > limit {
                return true;
            }
        }

        false
    }

    fn revert_chunks(&mut self) {
        // unread in reverse order
        while let Some(chunk) = self.chunks.pop() {
            self.payload.unread_data(chunk);
        }
    }

    fn concatenate(&self, mut len: usize) -> Bytes {
        let mut bytes = BytesMut::with_capacity(len);
        for chunk in &self.chunks {
            let remaining = if len > chunk.len() { chunk.len() } else { len };
            bytes.extend_from_slice(&chunk[..remaining]);
            len -= remaining;
        }
        bytes.freeze()
    }

    fn finish(&mut self, len: usize) -> Async<Option<Bytes>> {
        if len == 0 {
            return Async::Ready(None);
        }

        let line = if len == 0 || self.len_exceeded(len) {
            None
        } else {
            Some(self.concatenate(len))
        };

        self.revert_chunks();
        Async::Ready(line)
    }
}

impl futures::Future for PeekLine {
    type Item = Option<Bytes>;
    type Error = <SharedPayload as Stream>::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let chunk = match self.payload.poll() {
                Ok(Async::Ready(Some(chunk))) => chunk,
                Ok(Async::Ready(None)) => return Ok(self.finish(self.len)),
                result => return result,
            };

            self.chunks.push(chunk.clone());
            if let Some(pos) = chunk.iter().position(|b| *b == b'\n') {
                return Ok(self.finish(self.len + pos));
            }

            self.len += chunk.len();
            if self.len_exceeded(self.len) {
                self.revert_chunks();
                return Ok(Async::Ready(None));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use relay_test::TestRequest;

    #[test]
    fn test_empty() {
        relay_test::setup();

        let request = TestRequest::with_state(())
            .set_payload("".to_string())
            .finish();

        let opt =
            relay_test::block_fn(move || PeekLine::new(SharedPayload::get(&request))).unwrap();
        assert_eq!(opt, None);
    }

    #[test]
    fn test_one_line() {
        relay_test::setup();

        let request = TestRequest::with_state(())
            .set_payload("test".to_string())
            .finish();

        let opt =
            relay_test::block_fn(move || PeekLine::new(SharedPayload::get(&request))).unwrap();
        assert_eq!(opt, Some("test".into()));
    }

    #[test]
    fn test_linebreak() {
        relay_test::setup();

        let request = TestRequest::with_state(())
            .set_payload("test\ndone".to_string())
            .finish();

        let opt =
            relay_test::block_fn(move || PeekLine::new(SharedPayload::get(&request))).unwrap();
        assert_eq!(opt, Some("test".into()));
    }

    #[test]
    fn test_limit_satisfied() {
        relay_test::setup();

        let payload = "test\ndone";
        let request = TestRequest::with_state(())
            .set_payload(payload.to_string())
            .finish();

        let opt =
            relay_test::block_fn(move || PeekLine::new(SharedPayload::get(&request)).limit(4))
                .unwrap();
        assert_eq!(opt, Some("test".into()));
    }

    #[test]
    fn test_limit_exceeded() {
        relay_test::setup();

        let payload = "test\ndone";
        let request = TestRequest::with_state(())
            .set_payload(payload.to_string())
            .finish();

        let opt =
            relay_test::block_fn(move || PeekLine::new(SharedPayload::get(&request)).limit(3))
                .unwrap();
        assert_eq!(opt, None);
    }

    // NB: Repeat polls cannot be tested unfortunately, since `Payload::set_read_buffer_capacity`
    // does not take effect in test requests, and the sender returned by `Payload::new` does not
    // have a public interface.
}
