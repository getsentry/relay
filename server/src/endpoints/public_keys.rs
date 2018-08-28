use actix::ResponseFuture;
use actix_web::{http::Method, Error, Json};
use futures::prelude::*;

use actors::keys::{GetPublicKeys, GetPublicKeysResult};
use extractors::{CurrentServiceState, SignedJson};
use service::ServiceApp;

#[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
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

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/publickeys/", |r| {
        r.method(Method::POST).with(get_public_keys);
    })
}
