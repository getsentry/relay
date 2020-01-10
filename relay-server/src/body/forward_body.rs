use actix::ResponseFuture;
use actix_web::{error::PayloadError, http::StatusCode, HttpMessage, HttpResponse, ResponseError};
use bytes::{Bytes, BytesMut};
use failure::Fail;
use futures::prelude::*;

use crate::utils;

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
    stream: Option<T::Stream>,
    err: Option<ForwardPayloadError>,
    fut: Option<ResponseFuture<Bytes, ForwardPayloadError>>,
}

impl<T: HttpMessage> ForwardBody<T> {
    /// Create `ForwardBody` for request.
    pub fn new(req: &T, limit: usize) -> ForwardBody<T> {
        // Check the content length first. If we detect an overflow from the content length header,
        // keep the payload in the request to drain it correctly in the `ReadRequestMiddleware`.
        if let Some(length) = utils::get_content_length(req) {
            if length > limit {
                return Self::err(ForwardPayloadError::Overflow);
            }
        }

        ForwardBody {
            limit,
            stream: Some(req.payload()),
            err: None,
            fut: None,
        }
    }

    fn err(e: ForwardPayloadError) -> Self {
        ForwardBody {
            limit: 0,
            stream: None,
            fut: None,
            err: Some(e),
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

        let limit = self.limit;
        let body = Some(BytesMut::with_capacity(8192));

        let future = self
            .stream
            .take()
            .expect("Can not be used second time")
            .map_err(ForwardPayloadError::from)
            .fold(body, move |body_opt, chunk| {
                Ok::<_, ForwardPayloadError>(body_opt.and_then(|mut body| {
                    if (body.len() + chunk.len()) > limit {
                        None
                    } else {
                        body.extend_from_slice(&chunk);
                        Some(body)
                    }
                }))
            })
            .and_then(|bytes_opt| match bytes_opt {
                Some(bytes) => Ok(bytes.freeze()),
                None => Err(ForwardPayloadError::Overflow),
            });

        self.fut = Some(Box::new(future));

        self.poll()
    }
}
