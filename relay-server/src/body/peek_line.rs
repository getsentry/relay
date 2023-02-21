use actix_web::error::PayloadError;
use actix_web::HttpRequest;
use bytes::Bytes;
use smallvec::SmallVec;

use crate::extractors::{Decoder, SharedPayload};

/// Peeks the first line of a multi-line body without consuming it.
///
/// This function returns a future to the request [`Payload`]. It is especially designed to be
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
pub async fn peek_line<S>(
    request: &HttpRequest<S>,
    limit: usize,
) -> Result<Option<Bytes>, PayloadError> {
    let mut payload = SharedPayload::get(request);
    let mut decoder = Decoder::new(request, limit);
    let mut chunks = SmallVec::<[_; 3]>::new();
    let mut overflow = false;

    while let (Some(chunk), false) = (payload.chunk().await?, overflow) {
        overflow = decoder.decode(&chunk)?;
        chunks.push(chunk);
    }

    let buffer = decoder.finish()?;

    let line = match buffer.iter().position(|b| *b == b'\n') {
        Some(pos) => Some(buffer.slice_to(pos)),
        None if !overflow => Some(buffer),
        None => None,
    };

    // unread in reverse order
    while let Some(chunk) = chunks.pop() {
        payload.unread_data(chunk);
    }

    Ok(line.filter(|line| !line.is_empty()))
}

/*
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
*/
