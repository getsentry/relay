//! A simple healthcheck endpoint for the relay.

use ::actix::prelude::*;
use actix_web::{http::Method, Error, HttpResponse, Scope};
use futures::prelude::*;
use serde::Serialize;

use crate::extractors::CurrentServiceState;
use crate::service::ServiceState;

use crate::actors::upstream::IsAuthenticated;

#[derive(Serialize)]
struct HealthcheckResponse {
    is_healthy: bool,
}

#[allow(clippy::needless_pass_by_value)]
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

pub fn configure_scope(scope: Scope<ServiceState>) -> Scope<ServiceState> {
    scope.resource("/healthcheck/", |r| {
        r.method(Method::GET).with(healthcheck);
    })
}
