use actix::ResponseFuture;
use actix_web::{error::PayloadError, HttpRequest};
use bytes::{Bytes, BytesMut};
use futures::prelude::*;

use crate::extractors::SharedPayload;
use crate::utils;

/// Future that resolves to a complete store endpoint body.
pub struct RequestBody {
    limit: usize,
    stream: Option<SharedPayload>,
    err: Option<PayloadError>,
    fut: Option<ResponseFuture<Bytes, PayloadError>>,
}

impl RequestBody {
    /// Create `ForwardBody` for request.
    pub fn new<S>(req: &HttpRequest<S>, limit: usize) -> Self {
        if let Some(length) = utils::get_content_length(req) {
            if length > limit {
                return RequestBody {
                    limit: 0,
                    stream: None,
                    fut: None,
                    err: Some(PayloadError::Overflow),
                };
            }
        }

        RequestBody {
            limit,
            stream: Some(SharedPayload::get(req)),
            err: None,
            fut: None,
        }
    }
}

impl Future for RequestBody {
    type Item = Bytes;
    type Error = PayloadError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(ref mut fut) = self.fut {
            return fut.poll();
        }

        if let Some(err) = self.err.take() {
            return Err(err);
        }

        let limit = self.limit;
        let future = self
            .stream
            .take()
            .expect("Can not be used second time")
            .fold(BytesMut::with_capacity(8192), move |mut body, chunk| {
                if (body.len() + chunk.len()) > limit {
                    Err(PayloadError::Overflow)
                } else {
                    body.extend_from_slice(&chunk);
                    Ok(body)
                }
            })
            .map(BytesMut::freeze);

        self.fut = Some(Box::new(future));

        self.poll()
    }
}
