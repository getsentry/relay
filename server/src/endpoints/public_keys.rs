use ::actix::ResponseFuture;
use actix_web::{http::Method, Error, Json};
use futures::prelude::*;

use crate::actors::keys::{GetPublicKeys, GetPublicKeysResult};
use crate::extractors::{CurrentServiceState, SignedJson};
use crate::service::ServiceApp;

#[allow(clippy::needless_pass_by_value)]
fn get_public_keys(
    state: CurrentServiceState,
    body: SignedJson<GetPublicKeys>,
) -> ResponseFuture<Json<GetPublicKeysResult>, Error> {
    let future = state
        .key_cache()
        .send(body.inner)
        .map_err(Error::from)
        .and_then(|x| x.map_err(Error::from).map(Json));

    Box::new(future)
}

/// Registers the Relay public keys endpoint.
///
/// Note that this has nothing to do with Sentry public keys, which refer to the public key portion
/// of a DSN used for authenticating event submission. This endpoint is for Relay's public keys,
/// which authenticate entire Relays.
pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/publickeys/", |r| {
        r.method(Method::POST).with(get_public_keys);
    })
}
