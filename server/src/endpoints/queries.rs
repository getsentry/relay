use actix_web::http::Method;
use actix_web::Error;
use actix_web::Json;

use futures::Future;

use actors::keys::{GetPublicKey, GetPublicKeyResult};
use extractors::CurrentServiceState;
use service::ServiceApp;

#[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
fn get_public_keys(
    (state, body): (CurrentServiceState, Json<GetPublicKey>),
) -> Box<Future<Item = Json<GetPublicKeyResult>, Error = Error>> {
    let res = state.key_manager().send(body.into_inner());

    Box::new(
        res.map_err(Error::from)
            .and_then(|x| x.map_err(Error::from).map(|x| Json(x))),
    )
}

// #[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
// fn get_project_configs(
//     (state, body): (CurrentServiceState, Json<String>),
// ) -> Box<Future<Item = Json, Error = Error>> {
//     unimplemented!()
// }

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/0/relays/publickeys/", |r| {
        r.method(Method::POST).with(get_public_keys);
    })
}
