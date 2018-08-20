use actix::ResponseFuture;

use actix_web::http::Method;
use actix_web::Error;
use actix_web::FromRequest;
use actix_web::HttpMessage;
use actix_web::HttpRequest;
use actix_web::HttpResponse;
use actix_web::Json;
use actix_web::ResponseError;

use futures::Future;

use serde::de::DeserializeOwned;

use sentry;

use semaphore_aorta::ApiErrorResponse;
use semaphore_aorta::RelayId;

use actors::keys::{GetPublicKey, GetPublicKeyResult};
use extractors::CurrentServiceState;
use service::ServiceApp;
use service::ServiceState;

struct Signed<T>(pub T);

impl<T> Signed<T> {
    pub fn into_inner(self) -> T {
        self.0
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

impl<T: DeserializeOwned + 'static> FromRequest<ServiceState> for Signed<T> {
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
                .send(GetPublicKey {
                    relay_ids: vec![relay_id],
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
                .and_then(move |public_key| -> Self::Result {
                    Box::new(raw_body.map_err(Error::from).and_then(move |body| {
                        public_key
                            .unpack(&body, &relay_sig, None)
                            .map_err(|_| SignatureError::BadSignature)
                            .map_err(Error::from)
                            .map(Signed)
                    }))
                }),
        )
    }
}

#[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
fn get_public_keys(
    (state, body): (CurrentServiceState, Signed<GetPublicKey>),
) -> Box<Future<Item = Json<GetPublicKeyResult>, Error = Error>> {
    let res = state.key_manager().send(body.into_inner());

    Box::new(
        res.map_err(Error::from)
            .and_then(|x| x.map_err(Error::from).map(|x| Json(x))),
    )
}

// #[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
// fn get_project_configs(
//     (state, body): (CurrentServiceState, Signed<String>),
// ) -> Box<Future<Item = Json, Error = Error>> {
//     unimplemented!()
// }

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/publickeys/", |r| {
        r.method(Method::POST).with(get_public_keys);
    })
}
