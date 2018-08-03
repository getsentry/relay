use std::io::{self, Read};
use std::str;

use actix_web::{error::PayloadError, HttpMessage, HttpResponse, ResponseError};
use base64::{self, DecodeError};
use bytes::{Bytes, BytesMut};
use flate2::read::ZlibDecoder;
use futures::{Future, Poll, Stream};
use http::{header::CONTENT_LENGTH, StatusCode};
use serde_json::error::Error as JsonError;

use semaphore_aorta::{EventV8, EventVariant};

/// A set of errors that can occur during parsing json payloads
#[derive(Fail, Debug)]
pub enum EncodedEventPayloadError {
    /// Payload size is bigger than limit
    #[fail(display = "payload size is too large")]
    Overflow,
    /// Base64 Decode error
    #[fail(display = "base64 decode error: {}", _0)]
    Decode(#[cause] DecodeError, Option<Bytes>),
    /// zlib decode error
    #[fail(display = "zlib decode error: {}", _0)]
    Zlib(#[cause] io::Error, Option<Bytes>),
    /// Deserialize error
    #[fail(display = "json deserialize error: {}", _0)]
    Deserialize(#[cause] JsonError, Option<Bytes>),
    /// Payload error
    #[fail(display = "error that occur during reading payload: {}", _0)]
    Payload(#[cause] PayloadError),
}

impl EncodedEventPayloadError {
    /// Returns the body of the error if available.
    pub fn body(&self) -> Option<&[u8]> {
        match self {
            EncodedEventPayloadError::Overflow => None,
            EncodedEventPayloadError::Decode(_, ref body) => body.as_ref().map(|x| &x[..]),
            EncodedEventPayloadError::Zlib(_, ref body) => body.as_ref().map(|x| &x[..]),
            EncodedEventPayloadError::Deserialize(_, ref body) => body.as_ref().map(|x| &x[..]),
            EncodedEventPayloadError::Payload(_) => None,
        }
    }

    /// Returns the body of the error as utf-8 string
    pub fn utf8_body(&self) -> Option<&str> {
        self.body().and_then(|val| str::from_utf8(val).ok())
    }
}

impl ResponseError for EncodedEventPayloadError {
    fn error_response(&self) -> HttpResponse {
        match self {
            EncodedEventPayloadError::Overflow => HttpResponse::new(StatusCode::PAYLOAD_TOO_LARGE),
            _ => HttpResponse::new(StatusCode::BAD_REQUEST),
        }
    }
}

impl From<PayloadError> for EncodedEventPayloadError {
    fn from(err: PayloadError) -> EncodedEventPayloadError {
        EncodedEventPayloadError::Payload(err)
    }
}

/// Request payload decoder and json parser that resolves to a deserialized `T` value.
///
/// The payload can either be raw JSON, or base64 encoded gzipped (zlib) JSON.
pub struct EncodedEvent<T> {
    limit: usize,
    req: Option<T>,
    fut: Option<Box<Future<Item = EventVariant, Error = EncodedEventPayloadError>>>,
}

impl<T> EncodedEvent<T> {
    /// Create `JsonBody` for request.
    pub fn new(req: T) -> Self {
        EncodedEvent {
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

fn decode_event(body: BytesMut) -> Result<EventVariant, EncodedEventPayloadError> {
    Ok(EventVariant::SentryV8(Box::new(
        EventV8::from_json_bytes(&body)
            .map_err(|err| EncodedEventPayloadError::Deserialize(err, Some(body.freeze())))?,
    )))
}

impl<T, S> Future for EncodedEvent<T>
where
    T: HttpMessage<Stream = S>,
    S: Stream<Item = Bytes, Error = PayloadError> + Sized + 'static,
{
    type Item = EventVariant;
    type Error = EncodedEventPayloadError;

    fn poll(&mut self) -> Poll<EventVariant, EncodedEventPayloadError> {
        if let Some(req) = self.req.take() {
            if let Some(len) = req.headers().get(CONTENT_LENGTH) {
                if let Ok(s) = len.to_str() {
                    if let Ok(len) = s.parse::<usize>() {
                        if len > self.limit {
                            return Err(EncodedEventPayloadError::Overflow);
                        }
                    } else {
                        return Err(EncodedEventPayloadError::Overflow);
                    }
                }
            }

            let limit = self.limit;
            let fut = req
                .payload()
                .from_err()
                .fold(BytesMut::new(), move |mut body, chunk| {
                    if (body.len() + chunk.len()) > limit {
                        Err(EncodedEventPayloadError::Overflow)
                    } else {
                        body.extend_from_slice(&chunk);
                        Ok(body)
                    }
                })
                .and_then(|body| {
                    if body.starts_with(b"{") {
                        decode_event(body)
                    } else {
                        // TODO: Switch to a streaming decoder
                        // see https://github.com/alicemaz/rust-base64/pull/56
                        let binary_body = base64::decode(&body).map_err(|err| {
                            EncodedEventPayloadError::Decode(err, Some(body.freeze()))
                        })?;
                        let mut decode_stream = ZlibDecoder::new(binary_body.as_slice());
                        let mut bytes = vec![];
                        decode_stream
                            .read_to_end(&mut bytes)
                            .map_err(|err| EncodedEventPayloadError::Zlib(err, None))?;
                        decode_event(BytesMut::from(bytes))
                    }
                });
            self.fut = Some(Box::new(fut));
        }

        self.fut
            .as_mut()
            .expect("EncodedEvent cannot be used multiple times")
            .poll()
    }
}
