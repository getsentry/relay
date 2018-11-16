use actix::ResponseFuture;
use actix_web::http::{header, StatusCode};
use actix_web::{error::PayloadError, HttpMessage, HttpResponse, ResponseError};
use bytes::{Bytes, BytesMut};
use futures::prelude::*;

/// A set of errors that can occur during parsing json payloads
#[derive(Fail, Debug)]
pub enum ForwardPayloadError {
    /// Payload size is bigger than limit
    #[fail(display = "payload reached its size limit")]
    Overflow,

    /// A payload length is unknown.
    #[fail(display = "payload length is unknown")]
    UnknownLength,

    /// Interal Payload streaming error
    #[fail(display = "failed to read request payload")]
    Payload(#[cause] PayloadError),
}

impl ResponseError for ForwardPayloadError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ForwardPayloadError::Overflow => HttpResponse::new(StatusCode::PAYLOAD_TOO_LARGE),
            _ => HttpResponse::new(StatusCode::BAD_REQUEST),
        }
    }
}

impl From<PayloadError> for ForwardPayloadError {
    fn from(err: PayloadError) -> ForwardPayloadError {
        match err {
            PayloadError::Overflow => ForwardPayloadError::Overflow,
            PayloadError::UnknownLength => ForwardPayloadError::UnknownLength,
            other => ForwardPayloadError::Payload(other),
        }
    }
}

/// Future that resolves to a complete store endpoint body.
pub struct ForwardBody<T: HttpMessage> {
    limit: usize,
    length: Option<usize>,
    stream: Option<T::Stream>,
    err: Option<ForwardPayloadError>,
    fut: Option<ResponseFuture<Bytes, ForwardPayloadError>>,
}

impl<T: HttpMessage> ForwardBody<T> {
    /// Create `ForwardBody` for request.
    pub fn new(req: &T) -> ForwardBody<T> {
        let mut len = None;
        if let Some(l) = req.headers().get(header::CONTENT_LENGTH) {
            if let Ok(s) = l.to_str() {
                if let Ok(l) = s.parse::<usize>() {
                    len = Some(l)
                } else {
                    return Self::err(ForwardPayloadError::UnknownLength);
                }
            } else {
                return Self::err(ForwardPayloadError::UnknownLength);
            }
        }

        ForwardBody {
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

    fn err(e: ForwardPayloadError) -> Self {
        ForwardBody {
            stream: None,
            limit: 262_144,
            fut: None,
            err: Some(e),
            length: None,
        }
    }
}

impl<T> Future for ForwardBody<T>
where
    T: HttpMessage + 'static,
{
    type Item = Bytes;
    type Error = ForwardPayloadError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(ref mut fut) = self.fut {
            return fut.poll();
        }

        if let Some(err) = self.err.take() {
            return Err(err);
        }

        if let Some(len) = self.length.take() {
            if len > self.limit {
                return Err(ForwardPayloadError::Overflow);
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
                    Err(ForwardPayloadError::Overflow)
                } else {
                    body.extend_from_slice(&chunk);
                    Ok(body)
                }
            }).map(|x| x.freeze());

        self.fut = Some(Box::new(future));

        self.poll()
    }
}
