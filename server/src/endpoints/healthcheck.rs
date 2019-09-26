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

impl HealthcheckResponse {
    fn healthy() -> Self {
        Self { is_healthy: true }
    }

    fn unhealthy() -> Self {
        Self { is_healthy: false }
    }

    fn into_response(self) -> HttpResponse {
        if self.is_healthy {
            HttpResponse::Ok().json(self)
        } else {
            HttpResponse::ServiceUnavailable().json(self)
        }
    }
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
                    Ok(HealthcheckResponse::healthy().into_response())
                } else {
                    Err(())
                }
            })
            .or_else(|_| Ok(HealthcheckResponse::unhealthy().into_response())),
    )
}

#[cfg(feature = "processing")]
#[allow(clippy::needless_pass_by_value)]
fn healthcheck_processing(state: CurrentServiceState) -> ResponseFuture<HttpResponse, Error> {
    if state.config().processing_enabled() {
        healthcheck(state)
    } else {
        Box::new(futures::future::ok(
            HealthcheckResponse::unhealthy().into_response(),
        ))
    }
}

pub fn configure_scope(scope: Scope<ServiceState>) -> Scope<ServiceState> {
    let scope = scope.resource("/healthcheck/", |r| {
        r.method(Method::GET).with(healthcheck);
    });

    #[cfg(feature = "processing")]
    let scope = scope.resource("/healthcheck_processing/", |r| {
        r.method(Method::GET).with(healthcheck_processing);
    });
    scope
}
