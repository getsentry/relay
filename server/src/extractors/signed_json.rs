use actix::prelude::*;
use actix_web::{Error, FromRequest, HttpMessage, HttpRequest, HttpResponse, ResponseError};
use futures::prelude::*;
use sentry::Hub;
use sentry_actix::ActixWebHubExt;
use serde::de::DeserializeOwned;

use semaphore_common::{PublicKey, RelayId};

use actors::keys::GetPublicKey;
use service::ServiceState;
use utils::ApiErrorResponse;

#[derive(Debug)]
pub struct SignedJson<T> {
    pub inner: T,
    pub public_key: PublicKey,
}

#[derive(Fail, Debug)]
enum SignatureError {
    #[fail(display = "invalid relay signature")]
    BadSignature,
    #[fail(display = "missing header: {}", _0)]
    MissingHeader(&'static str),
    #[fail(display = "malformed header: {}", _0)]
    MalformedHeader(&'static str),
    #[fail(display = "Unknown relay id")]
    UnknownRelay,
}

impl ResponseError for SignatureError {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::Unauthorized().json(&ApiErrorResponse::from_fail(self))
    }
}

impl<T: DeserializeOwned + 'static> FromRequest<ServiceState> for SignedJson<T> {
    type Config = ();
    type Result = ResponseFuture<Self, Error>;

    fn from_request(req: &HttpRequest<ServiceState>, _cfg: &Self::Config) -> Self::Result {
        macro_rules! extract_header {
            ($name:expr) => {
                tryf!(
                    req.headers()
                        .get($name)
                        .ok_or(SignatureError::MissingHeader($name))
                        .and_then(|value| value
                            .to_str()
                            .map_err(|_| SignatureError::MalformedHeader($name)))
                )
            };
        }

        let relay_id: RelayId = tryf!(
            extract_header!("X-Sentry-Relay-Id")
                .parse()
                .map_err(|_| SignatureError::MalformedHeader("X-Sentry-Relay-Id"))
        );

        Hub::from_request(req).configure_scope(|scope| {
            scope.set_tag("relay_id", format!("{}", relay_id.to_string())); // Dump out header value even if not string
        });

        let relay_sig = extract_header!("X-Sentry-Relay-Signature").to_owned();

        let future = req
            .state()
            .key_manager()
            .send(GetPublicKey { relay_id })
            .map_err(Error::from)
            .and_then(|result| {
                result?
                    .public_key
                    .ok_or_else(|| Error::from(SignatureError::UnknownRelay))
            })
            .join(req.body().map_err(Error::from))
            .and_then(move |(public_key, body)| {
                public_key
                    .unpack(&body, &relay_sig, None)
                    .map(|inner| SignedJson { inner, public_key })
                    .map_err(|_| Error::from(SignatureError::BadSignature))
            });

        Box::new(future)
    }
}
