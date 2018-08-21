use actix::ResponseFuture;

use actix_web::Error;
use actix_web::FromRequest;
use actix_web::HttpMessage;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::ResponseError;

use futures::Future;

use serde::de::DeserializeOwned;

use sentry;

use semaphore_aorta::ApiErrorResponse;
use semaphore_aorta::PublicKey;
use semaphore_aorta::RelayId;

use actors::keys::GetPublicKeys;
use service::ServiceState;

pub struct SignedJson<T> {
    inner: T,
    public_key: PublicKey,
}

impl<T> SignedJson<T> {
    pub fn into_inner(self) -> T {
        self.inner
    }

    pub fn public_key(&self) -> &PublicKey {
        &self.public_key
    }
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

        sentry::configure_scope(|scope| {
            scope.set_tag("relay_id", format!("{}", relay_id.simple())); // Dump out header value even if not string
        });

        let relay_sig = extract_header!("X-Sentry-Relay-Signature").to_owned();
        let raw_body = req.body();

        Box::new(
            req.state()
                .key_manager()
                .send(GetPublicKeys {
                    relay_ids: vec![relay_id.clone()],
                })
                .map_err(Error::from)
                .and_then(|public_keys_response| public_keys_response.map_err(Error::from))
                .and_then(move |mut public_keys| {
                    public_keys
                        .public_keys
                        .remove(&relay_id)
                        .expect("Relay ID not in response")
                        .ok_or(SignatureError::UnknownRelay)
                        .map_err(Error::from)
                })
                .and_then(move |public_key| {
                    Box::new(raw_body.map_err(Error::from).and_then(move |body| {
                        public_key
                            .unpack(&body, &relay_sig, None)
                            .map_err(|_| SignatureError::BadSignature)
                            .map_err(Error::from)
                            .map(|inner| SignedJson {
                                inner,
                                public_key,
                            })
                    }))
                }),
        )
    }
}
