use actix_web::dev::Payload;
use actix_web::{FromRequest, HttpMessage, HttpRequest};
use futures::{Poll, Stream};

/// A shared reference to an actix request payload.
///
/// The reference to the stream can be cloned and be polled repeatedly, but it is not thread-safe.
/// When the payload is exhaused, it no longer returns any data from.
///
/// To obtain a reference, call [`SharedPayload::get`]. The first time, this takes the body out of
/// the request. Subsequent calls to `request.payload()` or `request.body()` will return empty. This
/// type also implements [`FromRequest`] for the use in actix request handlers.
#[derive(Clone)]
pub struct SharedPayload(Payload);

impl SharedPayload {
    /// Extracts the shared clone of the request payload from the given request.
    pub fn get<S>(request: &HttpRequest<S>) -> Payload {
        Self::extract(request).into_inner()
    }

    /// Unwraps the shared clone of the request payload.
    pub fn into_inner(self) -> Payload {
        self.0
    }
}

impl<S> FromRequest<S> for SharedPayload {
    type Config = ();
    type Result = Self;

    fn from_request(request: &HttpRequest<S>, _config: &Self::Config) -> Self::Result {
        let mut extensions = request.extensions_mut();

        if let Some(payload) = extensions.get::<Self>() {
            return payload.clone();
        }

        let payload = Self(request.payload());
        extensions.insert(payload.clone());
        payload
    }
}

impl Stream for SharedPayload {
    type Item = <Payload as Stream>::Item;
    type Error = <Payload as Stream>::Error;

    #[inline]
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.0.poll()
    }
}
