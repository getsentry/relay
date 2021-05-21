use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

use actix_web::dev::Payload;
use actix_web::{FromRequest, HttpMessage, HttpRequest};
use bytes::Bytes;
use futures::{Async, Poll, Stream};

/// A shared reference to an actix request payload.
///
/// The reference to the stream can be cloned and be polled repeatedly, but it is not thread-safe.
/// When the payload is exhaused, it no longer returns any data from.
///
/// To obtain a reference, call [`SharedPayload::get`]. The first time, this takes the body out of
/// the request. Subsequent calls to `request.payload()` or `request.body()` will return empty. This
/// type also implements [`FromRequest`] for the use in actix request handlers.
#[derive(Clone, Debug)]
pub struct SharedPayload {
    inner: Payload,
    done: Rc<AtomicBool>,
}

impl SharedPayload {
    /// Extracts the shared request payload from the given request.
    pub fn get<S>(request: &HttpRequest<S>) -> Self {
        let mut extensions = request.extensions_mut();

        if let Some(payload) = extensions.get::<Self>() {
            return payload.clone();
        }

        let payload = Self {
            inner: request.payload(),
            done: Rc::new(AtomicBool::new(false)),
        };

        extensions.insert(payload.clone());
        payload
    }

    /// Puts unused data back into the payload to be consumed again.
    ///
    /// The chunk will be prepended to the stream. When called multiple times, data will be polled
    /// in reverse order from unreading.
    pub fn unread_data(&mut self, chunk: Bytes) {
        self.inner.unread_data(chunk);
    }
}

impl<S> FromRequest<S> for SharedPayload {
    type Config = ();
    type Result = Self;

    fn from_request(request: &HttpRequest<S>, _config: &Self::Config) -> Self::Result {
        Self::get(request)
    }
}

impl Stream for SharedPayload {
    type Item = <Payload as Stream>::Item;
    type Error = <Payload as Stream>::Error;

    #[inline]
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if self.done.load(Ordering::Relaxed) {
            relay_log::info!("done");
            return Ok(Async::Ready(None));
        }

        let poll = self.inner.poll();

        // Fuse the stream on error. Subsequent polls will never be ready
        if matches!(poll, Ok(Async::Ready(None)) | Err(_)) {
            self.done.store(true, Ordering::Relaxed);
        }

        poll
    }
}
