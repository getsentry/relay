use actix::ResponseFuture;
use actix_web::{Error, Json};
use futures::prelude::*;

use crate::actors::keys::{GetRelays, GetRelaysResult};
use crate::extractors::{CurrentServiceState, SignedJson};
use crate::service::ServiceApp;

#[allow(clippy::needless_pass_by_value)]
fn get_public_keys(
    state: CurrentServiceState,
    body: SignedJson<GetRelays>,
) -> ResponseFuture<Json<GetRelaysResult>, Error> {
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
        r.name("relay-publickeys");
        r.post().with(get_public_keys);
    })
}
