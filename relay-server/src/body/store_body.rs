use std::borrow::Cow;
use std::io::{self, Read};

use actix::ResponseFuture;
use actix_web::http::StatusCode;
use actix_web::{error::PayloadError, HttpRequest, HttpResponse, ResponseError};
use base64::DecodeError;
use bytes::{Bytes, BytesMut};
use failure::Fail;
use flate2::read::ZlibDecoder;
use futures::prelude::*;
use url::form_urlencoded;

use relay_statsd::metric;

use crate::actors::outcome::DiscardReason;
use crate::extractors::SharedPayload;
use crate::statsd::RelayHistograms;
use crate::utils;

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
            StorePayloadError::Overflow => DiscardReason::TooLarge,
            StorePayloadError::UnknownLength => DiscardReason::Payload,
            StorePayloadError::Decode(_) => DiscardReason::Payload,
            StorePayloadError::Zlib(_) => DiscardReason::Payload,
            StorePayloadError::Payload(_) => DiscardReason::Payload,
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

    // These states are mutually exclusive:
    result: Option<Result<Bytes, StorePayloadError>>,
    fut: Option<ResponseFuture<Bytes, StorePayloadError>>,
    stream: Option<SharedPayload>,
}

impl StoreBody {
    /// Create `StoreBody` for request.
    pub fn new<S>(req: &HttpRequest<S>, limit: usize) -> Self {
        if let Some(body) = data_from_querystring(req) {
            return StoreBody {
                limit,
                stream: None,
                result: Some(decode_bytes(body.as_bytes())),
                fut: None,
            };
        }

        if let Some(length) = utils::get_content_length(req) {
            if length > limit {
                return Self::err(StorePayloadError::Overflow);
            }
        }

        StoreBody {
            limit,
            result: None,
            fut: None,
            stream: Some(SharedPayload::get(req)),
        }
    }

    fn err(e: StorePayloadError) -> Self {
        StoreBody {
            limit: 0,
            result: Some(Err(e)),
            fut: None,
            stream: None,
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

        let limit = self.limit;
        let future = self
            .stream
            .take()
            .expect("Can not be used second time")
            .map_err(StorePayloadError::from)
            .fold(BytesMut::with_capacity(8192), move |mut body, chunk| {
                if (body.len() + chunk.len()) > limit {
                    Err(StorePayloadError::Overflow)
                } else {
                    body.extend_from_slice(&chunk);
                    Ok(body)
                }
            })
            .and_then(|body| {
                metric!(histogram(RelayHistograms::RequestSizeBytesRaw) = body.len() as u64);
                let decoded = decode_bytes(body.freeze())?;
                metric!(
                    histogram(RelayHistograms::RequestSizeBytesUncompressed) = decoded.len() as u64
                );
                Ok(decoded)
            });

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
    if binary_body.starts_with(b"{") {
        return Ok(binary_body.into());
    }

    let mut decode_stream = ZlibDecoder::new(binary_body.as_slice());
    let mut bytes = vec![];
    decode_stream
        .read_to_end(&mut bytes)
        .map_err(StorePayloadError::Zlib)?;

    Ok(Bytes::from(bytes))
}
