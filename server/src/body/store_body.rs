use std::borrow::Cow;
use std::io::{self, Read};

use actix::ResponseFuture;
use actix_web::http::{header, StatusCode};
use actix_web::HttpRequest;
use actix_web::{error::PayloadError, HttpMessage, HttpResponse, ResponseError};
use base64::DecodeError;
use bytes::{Bytes, BytesMut};
use failure::Fail;
use flate2::read::ZlibDecoder;
use futures::prelude::*;
use url::form_urlencoded;

use crate::actors::outcome::DiscardReason;

/// A set of errors that can occur during parsing json payloads
#[derive(Fail, Debug)]
pub enum StorePayloadError {
    /// Payload size is bigger than limit
    #[fail(display = "payload reached its size limit")]
    Overflow,

    /// A payload length is unknown.
    #[fail(display = "payload length is unknown")]
    UnknownLength,

    /// Base64 Decode error
    #[fail(display = "failed to base64 decode payload")]
    Decode(#[cause] DecodeError),

    /// zlib decode error
    #[fail(display = "failed to decode zlib payload")]
    Zlib(#[cause] io::Error),

    /// Internal Payload streaming error
    #[fail(display = "failed to read request payload")]
    Payload(#[cause] PayloadError),
}

impl StorePayloadError {
    /// Returns the outcome discard reason for this payload error.
    pub fn discard_reason(&self) -> DiscardReason {
        match self {
            StorePayloadError::Overflow => DiscardReason::PayloadTooLarge,
            StorePayloadError::UnknownLength => DiscardReason::UnknownPayloadLength,
            StorePayloadError::Decode(_) => DiscardReason::InvalidPayloadFormat,
            StorePayloadError::Zlib(_) => DiscardReason::InvalidPayloadFormat,
            StorePayloadError::Payload(_) => DiscardReason::InvalidPayloadFormat,
        }
    }
}

impl ResponseError for StorePayloadError {
    fn error_response(&self) -> HttpResponse {
        match self {
            StorePayloadError::Overflow => HttpResponse::new(StatusCode::PAYLOAD_TOO_LARGE),
            _ => HttpResponse::new(StatusCode::BAD_REQUEST),
        }
    }
}

impl From<PayloadError> for StorePayloadError {
    fn from(err: PayloadError) -> StorePayloadError {
        match err {
            PayloadError::Overflow => StorePayloadError::Overflow,
            PayloadError::UnknownLength => StorePayloadError::UnknownLength,
            other => StorePayloadError::Payload(other),
        }
    }
}

/// Future that resolves to a complete store endpoint body.
pub struct StoreBody {
    limit: usize,
    length: Option<usize>,

    // These states are mutually exclusive, and only separate options due to borrowing
    // problems:
    result: Option<Result<Bytes, StorePayloadError>>,
    fut: Option<ResponseFuture<Bytes, StorePayloadError>>,
    stream: Option<<HttpRequest as HttpMessage>::Stream>,
}

impl StoreBody {
    /// Create `StoreBody` for request.
    pub fn new<S>(req: &HttpRequest<S>) -> Self {
        if let Some(body) = data_from_querystring(req) {
            StoreBody {
                limit: 262_144,
                length: Some(body.len()),
                stream: None,
                result: Some(decode_bytes(body.as_bytes())),
                fut: None,
            }
        } else {
            let length = match get_content_length(req) {
                Ok(x) => x,
                Err(e) => return StoreBody::err(e),
            };

            StoreBody {
                limit: 262_144,
                length,
                result: None,
                stream: Some(req.payload()),
                fut: None,
            }
        }
    }

    /// Change max size of payload. By default max size is 256Kb
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    fn err(e: StorePayloadError) -> Self {
        StoreBody {
            stream: None,
            limit: 262_144,
            result: Some(Err(e)),
            fut: None,
            length: None,
        }
    }
}

impl Future for StoreBody {
    type Item = Bytes;
    type Error = StorePayloadError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(result) = self.result.take() {
            return result.map(Async::Ready);
        }

        if let Some(ref mut fut) = self.fut {
            return fut.poll();
        }

        if let Some(len) = self.length.take() {
            if len > self.limit {
                return Err(StorePayloadError::Overflow);
            }
        }

        let limit = self.limit;
        let future = self
            .stream
            .take()
            .expect("Can not be used second time")
            .from_err()
            .fold(BytesMut::with_capacity(8192), move |mut body, chunk| {
                if (body.len() + chunk.len()) > limit {
                    Err(StorePayloadError::Overflow)
                } else {
                    body.extend_from_slice(&chunk);
                    Ok(body)
                }
            })
            .and_then(|body| decode_bytes(body.freeze()));

        self.fut = Some(Box::new(future));

        self.poll()
    }
}

fn data_from_querystring<S>(req: &HttpRequest<S>) -> Option<Cow<'_, str>> {
    if req.method() != "GET" {
        return None;
    }

    let (_, value) = form_urlencoded::parse(req.query_string().as_bytes())
        .find(|(key, _)| key == "sentry_data")?;

    Some(value)
}

fn decode_bytes<B: Into<Bytes> + AsRef<[u8]>>(body: B) -> Result<Bytes, StorePayloadError> {
    if body.as_ref().starts_with(b"{") {
        return Ok(body.into());
    }

    // TODO: Switch to a streaming decoder
    // see https://github.com/alicemaz/rust-base64/pull/56
    let binary_body = base64::decode(&body).map_err(StorePayloadError::Decode)?;
    let mut decode_stream = ZlibDecoder::new(binary_body.as_slice());
    let mut bytes = vec![];
    decode_stream
        .read_to_end(&mut bytes)
        .map_err(StorePayloadError::Zlib)?;

    Ok(Bytes::from(bytes))
}

fn get_content_length<S>(req: &HttpRequest<S>) -> Result<Option<usize>, StorePayloadError> {
    if let Some(l) = req.headers().get(header::CONTENT_LENGTH) {
        if let Ok(s) = l.to_str() {
            if let Ok(l) = s.parse::<usize>() {
                Ok(Some(l))
            } else {
                Err(StorePayloadError::UnknownLength)
            }
        } else {
            Err(StorePayloadError::UnknownLength)
        }
    } else {
        Ok(None)
    }
}
