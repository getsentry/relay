use actix_web::{HttpMessage, HttpResponse, ResponseError, error::PayloadError};
use base64::{self, DecodeError};
use bytes::{Bytes, BytesMut};
use flate2::read::ZlibDecoder;
use futures::{Future, Poll, Stream};
use http::{StatusCode, header::CONTENT_LENGTH};
use serde::de::DeserializeOwned;
use serde_json::{self, error::Error as JsonError};

/// A set of errors that can occur during parsing json payloads
#[derive(Fail, Debug)]
pub enum EncodedJsonPayloadError {
    /// Payload size is bigger than limit
    #[fail(display = "payload size is too large")]
    Overflow,
    /// Base64 Decode error
    #[fail(display = "base64 decode error: {}", _0)]
    Decode(#[cause] DecodeError),
    /// Deserialize error
    #[fail(display = "json deserialize error: {}", _0)]
    Deserialize(#[cause] JsonError),
    /// Payload error
    #[fail(display = "error that occur during reading payload: {}", _0)]
    Payload(#[cause] PayloadError),
}

impl ResponseError for EncodedJsonPayloadError {
    fn error_response(&self) -> HttpResponse {
        match *self {
            EncodedJsonPayloadError::Overflow => HttpResponse::new(StatusCode::PAYLOAD_TOO_LARGE),
            _ => HttpResponse::new(StatusCode::BAD_REQUEST),
        }
    }
}

impl From<PayloadError> for EncodedJsonPayloadError {
    fn from(err: PayloadError) -> EncodedJsonPayloadError {
        EncodedJsonPayloadError::Payload(err)
    }
}

impl From<DecodeError> for EncodedJsonPayloadError {
    fn from(err: DecodeError) -> EncodedJsonPayloadError {
        EncodedJsonPayloadError::Decode(err)
    }
}

impl From<JsonError> for EncodedJsonPayloadError {
    fn from(err: JsonError) -> EncodedJsonPayloadError {
        EncodedJsonPayloadError::Deserialize(err)
    }
}

/// Request payload decoder and json parser that resolves to a deserialized `T` value.
///
/// The payload can either be raw JSON, or base64 encoded gzipped (zlib) JSON.
///
/// Returns error:
///
/// * content type is not `application/json`
/// * content length is greater than the configured limit (defaults to 256k)
///
pub struct EncodedJsonBody<T, U: DeserializeOwned> {
    limit: usize,
    req: Option<T>,
    fut: Option<Box<Future<Item = U, Error = EncodedJsonPayloadError>>>,
}

impl<T, U: DeserializeOwned> EncodedJsonBody<T, U> {
    /// Create `JsonBody` for request.
    pub fn new(req: T) -> Self {
        EncodedJsonBody {
            limit: 262_144,
            req: Some(req),
            fut: None,
        }
    }

    /// Change max size of payload. By default max size is 256Kb
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }
}

impl<T, U: DeserializeOwned + 'static> Future for EncodedJsonBody<T, U>
where
    T: HttpMessage + Stream<Item = Bytes, Error = PayloadError> + 'static,
{
    type Item = U;
    type Error = EncodedJsonPayloadError;

    fn poll(&mut self) -> Poll<U, EncodedJsonPayloadError> {
        if let Some(req) = self.req.take() {
            if let Some(len) = req.headers().get(CONTENT_LENGTH) {
                if let Ok(s) = len.to_str() {
                    if let Ok(len) = s.parse::<usize>() {
                        if len > self.limit {
                            return Err(EncodedJsonPayloadError::Overflow);
                        }
                    } else {
                        return Err(EncodedJsonPayloadError::Overflow);
                    }
                }
            }

            let limit = self.limit;
            let fut = req.from_err()
                .fold(BytesMut::new(), move |mut body, chunk| {
                    if (body.len() + chunk.len()) > limit {
                        Err(EncodedJsonPayloadError::Overflow)
                    } else {
                        body.extend_from_slice(&chunk);
                        Ok(body)
                    }
                })
                .and_then(|body| {
                    if body.starts_with(b"{") {
                        Ok(serde_json::from_slice(&body)?)
                    } else {
                        // TODO: Switch to a streaming decoder
                        // see https://github.com/alicemaz/rust-base64/pull/56
                        let binary_body = base64::decode(&body)?;
                        let decode_stream = ZlibDecoder::new(binary_body.as_slice());
                        Ok(serde_json::from_reader(decode_stream)?)
                    }
                });
            self.fut = Some(Box::new(fut));
        }

        self.fut
            .as_mut()
            .expect("EncodedJsonBody cannot be used multiple times")
            .poll()
    }
}
