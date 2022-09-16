//! A simple health check endpoint for the relay.

use ::actix::prelude::*;
use actix_web::{Error, HttpResponse};
use futures::{FutureExt, TryFutureExt};
use futures01::prelude::*;
use serde::Serialize;

use crate::service::ServiceApp;

use crate::actors::health_check::{HealthCheck, IsHealthy};

#[derive(Serialize)]
struct HealthCheckResponse {
    is_healthy: bool,
}

impl HealthCheckResponse {
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

fn health_check_impl(message: IsHealthy) -> ResponseFuture<HttpResponse, Error> {
    let fut = async move {
        let addr = HealthCheck::from_registry();
        addr.send(message).await
    };

    Box::new(
        fut.boxed_local()
            .compat()
            .map_err(|_| ())
            .and_then(move |is_healthy| {
                if !is_healthy {
                    Err(())
                } else {
                    Ok(HealthCheckResponse::healthy().into_response())
                }
            })
            .or_else(|()| Ok(HealthCheckResponse::unhealthy().into_response())),
    )
}

fn readiness_health_check(_: ()) -> ResponseFuture<HttpResponse, Error> {
    health_check_impl(IsHealthy::Readiness)
}

fn liveness_health_check(_: ()) -> ResponseFuture<HttpResponse, Error> {
    health_check_impl(IsHealthy::Liveness)
}

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.resource("/api/relay/healthcheck/ready/", |r| {
        r.name("internal-healthcheck-ready");
        r.get().with(readiness_health_check);
    })
    .resource("/api/relay/healthcheck/live/", |r| {
        r.name("internal-healthcheck-live");
        r.get().with(liveness_health_check);
    })
    // live check is also used to check network connectivity by downstream relays.
    // It must have the same url as Sentry (hence two urls doing the same thing)
    .resource("/api/0/relays/live/", |r| {
        r.name("external-healthcheck-live");
        r.get().with(liveness_health_check);
    })
}
