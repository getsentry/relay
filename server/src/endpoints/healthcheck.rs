//! A simple healthcheck endpoint for the relay.

use actix::prelude::*;
use actix_web::{http::Method, Error, HttpResponse};
use futures::prelude::*;

use crate::extractors::CurrentServiceState;
use crate::service::ServiceApp;

use crate::actors::upstream::IsAuthenticated;

#[derive(Serialize)]
struct HealthcheckResponse {
    is_healthy: bool,
}

#[cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]
fn healthcheck(state: CurrentServiceState) -> ResponseFuture<HttpResponse, Error> {
    Box::new(
        state
            .upstream_relay()
            .send(IsAuthenticated)
            .map_err(|_| ())
            .and_then(move |is_authenticated| {
                if is_authenticated {
                    Ok(HttpResponse::Ok().json(HealthcheckResponse { is_healthy: true }))
                } else {
                    Err(())
                }
            })
            .or_else(|_| {
                Ok(HttpResponse::ServiceUnavailable()
                    .json(HealthcheckResponse { is_healthy: false }))
            }),
    )
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.scope("/api/relay", |scope| {
        scope
            .resource("/healthcheck/", |r| {
                r.method(Method::GET).with(healthcheck);
            })
            // never forward /api/relay, as that prefix is used for stuff like healthchecks
            .default_resource(|r| r.f(|_| HttpResponse::NotFound()))
    })
}
