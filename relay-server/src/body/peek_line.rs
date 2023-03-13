use actix_web::error::PayloadError;
use actix_web::HttpRequest;
use bytes::Bytes;
use smallvec::SmallVec;

use crate::extractors::{Decoder, SharedPayload};

/// Peeks the first line of a multi-line body without consuming it.
///
/// This function returns a future to the request [`Payload`]. It is especially designed to be used
/// together with [`SharedPayload`], since it polls an underlying payload and places the polled
/// chunks back into the payload when completing.
///
/// If the payload does not contain a newline, the entire payload is returned by the future. To
/// contrain this, use `limit` to set a maximum size returned by the future.
///
/// Resolves to `Some(Bytes)` if a newline is found within the size limit, or the entire payload is
/// smaller than the size limit. Otherwise, resolves to `None`. Any errors on the underlying stream
/// are returned without change.
///
/// # Cancel Safety
///
/// This function is _not_ cancellation safe. If canceled, partially read data may not be put back
/// into the request. Additionally, it is not safe to read the body after an error has been returned
/// since data may have been partially consumed.
///
/// [`Payload`]: actix_web::dev::Payload
/// [`SharedPayload`]: crate::extractors::SharedPayload
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

#[cfg(test)]
mod tests {
    use relay_test::TestRequest;

    use super::*;

    #[tokio::test]
    async fn test_empty() {
        relay_test::setup();

        let request = TestRequest::with_state(())
            .set_payload("".to_string())
            .finish();

        let opt = peek_line(&request, 10).await.unwrap();
        assert_eq!(opt, None);
    }

    #[tokio::test]
    async fn test_one_line() {
        relay_test::setup();

        let request = TestRequest::with_state(())
            .set_payload("test".to_string())
            .finish();

        let opt = peek_line(&request, 10).await.unwrap();
        assert_eq!(opt, Some("test".into()));
    }

    #[tokio::test]
    async fn test_linebreak() {
        relay_test::setup();

        let request = TestRequest::with_state(())
            .set_payload("test\ndone".to_string())
            .finish();

        let opt = peek_line(&request, 10).await.unwrap();
        assert_eq!(opt, Some("test".into()));
    }

    #[tokio::test]
    async fn test_limit_satisfied() {
        relay_test::setup();

        let payload = "test\ndone";
        let request = TestRequest::with_state(())
            .set_payload(payload.to_string())
            .finish();

        // NOTE: Newline fits into the size limit.
        let opt = peek_line(&request, 5).await.unwrap();
        assert_eq!(opt, Some("test".into()));
    }

    #[tokio::test]
    async fn test_limit_exceeded() {
        relay_test::setup();

        let payload = "test\ndone";
        let request = TestRequest::with_state(())
            .set_payload(payload.to_string())
            .finish();

        // NOTE: newline is not found within the size limit. even though the payload would fit,
        // according to the doc comment we return `None`.
        let opt = peek_line(&request, 4).await.unwrap();
        assert_eq!(opt, None);
    }

    #[tokio::test]
    async fn test_shared_payload() {
        relay_test::setup();

        let request = TestRequest::with_state(())
            .set_payload("test\ndone".to_string())
            .finish();

        peek_line(&request, 10).await.unwrap();

        let mut payload = SharedPayload::get(&request);
        let chunk = payload.chunk().await.unwrap();
        assert_eq!(chunk.as_deref(), Some(b"test\ndone".as_slice()));
    }
}
