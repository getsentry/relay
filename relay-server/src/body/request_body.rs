use actix_web::{error::PayloadError, HttpRequest};
use bytes::Bytes;

use crate::extractors::{Decoder, SharedPayload};
use crate::utils;

/// Reads the body of a Relay endpoint request.
///
/// If the body exceeds the given `limit` during streaming or decompression, an error is returned.
pub async fn request_body<S>(req: &HttpRequest<S>, limit: usize) -> Result<Bytes, PayloadError> {
    if let Some(length) = utils::get_content_length(req) {
        if length > limit {
            return Err(PayloadError::Overflow);
        }
    }

    let mut payload = SharedPayload::get(req);
    let mut decoder = Decoder::new(req, limit);

    while let Some(encoded) = payload.chunk().await? {
        if decoder.decode(&encoded)? {
            return Err(PayloadError::Overflow);
        }
    }

    Ok(decoder.finish()?)
}
