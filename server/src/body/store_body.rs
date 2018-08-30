use std::io::{self, Read};

use actix::ResponseFuture;
use actix_web::http::{header, StatusCode};
use actix_web::{error::PayloadError, HttpMessage, HttpResponse, ResponseError};
use base64::{self, DecodeError};
use bytes::{Bytes, BytesMut};
use flate2::read::ZlibDecoder;
use futures::prelude::*;

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

    /// Interal Payload streaming error
    #[fail(display = "failed to read request payload")]
    Payload(#[cause] PayloadError),
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
pub struct StoreBody<T: HttpMessage> {
    limit: usize,
    length: Option<usize>,
    stream: Option<T::Stream>,
    err: Option<StorePayloadError>,
    fut: Option<ResponseFuture<Bytes, StorePayloadError>>,
}

impl<T: HttpMessage> StoreBody<T> {
    /// Create `StoreBody` for request.
    pub fn new(req: &T) -> StoreBody<T> {
        let mut len = None;
        if let Some(l) = req.headers().get(header::CONTENT_LENGTH) {
            if let Ok(s) = l.to_str() {
                if let Ok(l) = s.parse::<usize>() {
                    len = Some(l)
                } else {
                    return Self::err(StorePayloadError::UnknownLength);
                }
            } else {
                return Self::err(StorePayloadError::UnknownLength);
            }
        }

        StoreBody {
            limit: 262_144,
            length: len,
            stream: Some(req.payload()),
            fut: None,
            err: None,
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
            fut: None,
            err: Some(e),
            length: None,
        }
    }
}

impl<T> Future for StoreBody<T>
where
    T: HttpMessage + 'static,
{
    type Item = Bytes;
    type Error = StorePayloadError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(ref mut fut) = self.fut {
            return fut.poll();
        }

        if let Some(err) = self.err.take() {
            return Err(err);
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
            .and_then(|body| {
                if body.starts_with(b"{") {
                    return Ok(body);
                }

                // TODO: Switch to a streaming decoder
                // see https://github.com/alicemaz/rust-base64/pull/56
                let binary_body = base64::decode(&body).map_err(StorePayloadError::Decode)?;
                let mut decode_stream = ZlibDecoder::new(binary_body.as_slice());
                let mut bytes = vec![];
                decode_stream
                    .read_to_end(&mut bytes)
                    .map_err(StorePayloadError::Zlib)?;

                Ok(BytesMut::from(bytes))
            })
            .map(|body| body.freeze());

        self.fut = Some(Box::new(future));

        self.poll()
    }
}
