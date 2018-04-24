use futures::{future, Future};
use actix_web::{FromRequest, HttpMessage, HttpRequest};
use actix_web::dev::JsonBody;
use actix_web::error::{Error, JsonPayloadError, ResponseError};
use sentry_types::protocol::latest::Event;
use sentry_types::{Auth, AuthParseError};

/// Holds an event and the associated auth header.
#[derive(Debug)]
pub struct StoreRequest {
    auth: Auth,
    payload: Event<'static>,
}

impl StoreRequest {
    /// Returns the sentry protocol auth for this request.
    pub fn auth(&self) -> &Auth {
        &self.auth
    }

    /// Returns a reference to the sentry event.
    pub fn event(&self) -> &Event<'static> {
        &self.payload
    }

    /// Converts the object into the event
    pub fn into_event(self) -> Event<'static> {
        self.payload
    }
}

#[derive(Fail, Debug)]
pub enum BadStoreRequest {
    #[fail(display = "missing x-sentry-auth header")]
    MissingAuth,
    #[fail(display = "bad x-sentry-auth header")]
    BadAuth(#[fail(cause)] AuthParseError),
    #[fail(display = "bad JSON payload")]
    BadJson(#[fail(cause)] JsonPayloadError),
}

impl ResponseError for BadStoreRequest {}

impl<S> FromRequest<S> for StoreRequest
where
    S: 'static,
{
    type Config = ();
    type Result = Box<Future<Item = Self, Error = Error>>;

    #[inline]
    fn from_request(req: &HttpRequest<S>, cfg: &Self::Config) -> Self::Result {
        let req = req.clone();
        let auth = match req.headers()
            .get("x-sentry-auth")
            .and_then(|x| x.to_str().ok())
        {
            Some(auth) => match auth.parse::<Auth>() {
                Ok(val) => val,
                Err(err) => return Box::new(future::err(BadStoreRequest::BadAuth(err).into())),
            },
            None => return Box::new(future::err(BadStoreRequest::MissingAuth.into())),
        };
        Box::new(
            JsonBody::new(req.clone())
                .limit(262_144)
                .map_err(|e| BadStoreRequest::BadJson(e).into())
                .map(|payload| StoreRequest {
                    auth: auth,
                    payload: payload,
                }),
        )
    }
}
