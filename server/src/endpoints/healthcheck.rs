//! A simple healthcheck endpoint for the relay.
use ::actix::prelude::*;
use actix_web::{http::Method, Error, HttpResponse, Scope};
use futures::prelude::*;
use serde::Serialize;

use crate::extractors::CurrentServiceState;
use crate::service::ServiceState;

use crate::actors::healthcheck::IsHealthy;

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

fn healthcheck_impl(
    state: CurrentServiceState,
    message: IsHealthy,
) -> ResponseFuture<HttpResponse, Error> {
    Box::new(
        state
            .healthcheck()
            .send(message)
            .map_err(|_| ())
            .flatten()
            .and_then(move |is_healthy| {
                if !is_healthy {
                    Err(())
                } else {
                    Ok(HealthcheckResponse::healthy().into_response())
                }
            })
            .or_else(|()| Ok(HealthcheckResponse::unhealthy().into_response())),
    )
}

fn readiness_healthcheck(state: CurrentServiceState) -> ResponseFuture<HttpResponse, Error> {
    healthcheck_impl(state, IsHealthy::Readiness)
}

fn liveness_healthcheck(state: CurrentServiceState) -> ResponseFuture<HttpResponse, Error> {
    healthcheck_impl(state, IsHealthy::Liveness)
}

pub fn configure_scope(scope: Scope<ServiceState>) -> Scope<ServiceState> {
    scope
        .resource("/healthcheck/ready/", |r| {
            r.method(Method::GET).with(readiness_healthcheck);
        })
        .resource("/healthcheck/live/", |r| {
            r.method(Method::GET).with(liveness_healthcheck)
        })
}
