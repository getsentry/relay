use actix_web::HttpRequest;
use bytes::Bytes;
use futures01::{Async, Future, Poll, Stream};
use smallvec::SmallVec;

use crate::extractors::{Decoder, SharedPayload};

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
///
/// [`Payload`]: actix_web::dev::Payload
pub struct PeekLine {
    payload: SharedPayload,
    decoder: Decoder,
    chunks: SmallVec<[Bytes; 3]>,
}

impl PeekLine {
    /// Creates a new peek line future from the given payload.
    ///
    /// Note that the underlying stream may return more data than the configured limit. The future
    /// will still never resolve more than the limit set.
    pub fn new<S>(request: &HttpRequest<S>, limit: usize) -> Self {
        Self {
            payload: SharedPayload::get(request),
            decoder: Decoder::new(request, limit),
            chunks: SmallVec::new(),
        }
    }

    fn revert_chunks(&mut self) {
        // unread in reverse order
        while let Some(chunk) = self.chunks.pop() {
            self.payload.unread_data(chunk);
        }
    }

    fn finish(&mut self, overflow: bool) -> std::io::Result<Option<Bytes>> {
        let buffer = self.decoder.finish()?;

        let line = match buffer.iter().position(|b| *b == b'\n') {
            Some(pos) => Some(buffer.slice_to(pos)),
            None if !overflow => Some(buffer),
            None => None,
        };

        self.revert_chunks();
        Ok(line.filter(|line| !line.is_empty()))
    }
}

impl Future for PeekLine {
    type Item = Option<Bytes>;
    type Error = <SharedPayload as Stream>::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let chunk = match self.payload.poll()? {
                Async::Ready(Some(chunk)) => chunk,
                Async::Ready(None) => return Ok(Async::Ready(self.finish(false)?)),
                Async::NotReady => return Ok(Async::NotReady),
            };

            self.chunks.push(chunk.clone());
            if self.decoder.decode(chunk)? {
                return Ok(Async::Ready(self.finish(true)?));
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

        let opt = relay_test::block_fn(move || PeekLine::new(&request, 10)).unwrap();
        assert_eq!(opt, None);
    }

    #[test]
    fn test_one_line() {
        relay_test::setup();

        let request = TestRequest::with_state(())
            .set_payload("test".to_string())
            .finish();

        let opt = relay_test::block_fn(move || PeekLine::new(&request, 10)).unwrap();
        assert_eq!(opt, Some("test".into()));
    }

    #[test]
    fn test_linebreak() {
        relay_test::setup();

        let request = TestRequest::with_state(())
            .set_payload("test\ndone".to_string())
            .finish();

        let opt = relay_test::block_fn(move || PeekLine::new(&request, 10)).unwrap();
        assert_eq!(opt, Some("test".into()));
    }

    #[test]
    fn test_limit_satisfied() {
        relay_test::setup();

        let payload = "test\ndone";
        let request = TestRequest::with_state(())
            .set_payload(payload.to_string())
            .finish();

        // NOTE: Newline fits into the size limit.
        let opt = relay_test::block_fn(move || PeekLine::new(&request, 5)).unwrap();
        assert_eq!(opt, Some("test".into()));
    }

    #[test]
    fn test_limit_exceeded() {
        relay_test::setup();

        let payload = "test\ndone";
        let request = TestRequest::with_state(())
            .set_payload(payload.to_string())
            .finish();

        // NOTE: newline is not found within the size limit. even though the payload would fit,
        // according to the doc comment we return `None`.
        let opt = relay_test::block_fn(move || PeekLine::new(&request, 4)).unwrap();
        assert_eq!(opt, None);
    }

    // NB: Repeat polls cannot be tested unfortunately, since `Payload::set_read_buffer_capacity`
    // does not take effect in test requests, and the sender returned by `Payload::new` does not
    // have a public interface.
}
