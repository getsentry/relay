use actix_web::{http::Method, Error, Json};
use futures::Future;

use actors::keys::{GetPublicKeys, GetPublicKeysResult};
use extractors::CurrentServiceState;
use extractors::SignedJson;
use service::ServiceApp;

#[cfg_attr(feat = "cargo-clippy", allow(needless_pass_by_value))]
fn get_public_keys(
    (state, body): (CurrentServiceState, SignedJson<GetPublicKeys>),
) -> Box<Future<Item = Json<GetPublicKeysResult>, Error = Error>> {
    let res = state.key_manager().send(body.inner);

    Box::new(
        res.map_err(Error::from)
            .and_then(|x| x.map_err(Error::from).map(|x| Json(x))),
    )
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/publickeys/", |r| {
        r.method(Method::POST).with(get_public_keys);
    })
}
