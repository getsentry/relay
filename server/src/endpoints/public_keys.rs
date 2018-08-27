use actix_web::{http::Method, Error, Json};
use futures::prelude::*;

use actors::keys::{GetPublicKeys, GetPublicKeysResult};
use extractors::{CurrentServiceState, SignedJson};
use service::ServiceApp;

#[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
fn get_public_keys(
    state: CurrentServiceState,
    body: SignedJson<GetPublicKeys>,
) -> Box<Future<Item = Json<GetPublicKeysResult>, Error = Error>> {
    Box::new(and_then!({
        let result = await!(state.key_cache().send(body.inner));
        let body = await!(result);
        Ok(Json(body))
    }))
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/publickeys/", |r| {
        r.method(Method::POST).with(get_public_keys);
    })
}
