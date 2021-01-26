//! A simple healthcheck endpoint for the relay.
use ::actix::prelude::*;
use actix_web::{Error, HttpResponse};
use futures::prelude::*;
use serde::Serialize;

use crate::extractors::CurrentServiceState;
use crate::service::ServiceApp;

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

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/relay/healthcheck/ready/", |r| {
        r.name("internal-healthcheck-ready");
        r.get().with(readiness_healthcheck);
    })
    .resource("/api/relay/healthcheck/live/", |r| {
        r.name("internal-healthcheck-live");
        r.get().with(liveness_healthcheck);
    })
    // live check is also used to check network connectivity by downstream relays.
    // It must have the same url as Sentry (hence two urls doing the same thing)
    .resource("/api/0/relays/live/", |r| {
        r.name("external-healthcheck-live");
        r.get().with(liveness_healthcheck);
    })
}
