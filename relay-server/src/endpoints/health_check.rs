//! A simple health check endpoint for the relay.

use actix_web::{App, Error, HttpResponse};
use futures::TryFutureExt;
use serde::Serialize;

use crate::actors::health_check::{HealthCheck, IsHealthy};
use crate::service::ServiceState;

#[derive(Serialize)]
struct Status {
    is_healthy: bool,
}

async fn health_check(message: IsHealthy) -> Result<HttpResponse, Error> {
    Ok(match HealthCheck::from_registry().send(message).await {
        Ok(true) => HttpResponse::Ok().json(Status { is_healthy: true }),
        _ => HttpResponse::ServiceUnavailable().json(Status { is_healthy: false }),
    })
}

pub fn configure_app(app: App<ServiceState>) -> App<ServiceState> {
    app.resource("/api/relay/healthcheck/ready/", |r| {
        r.name("internal-healthcheck-ready");
        r.get()
            .with_async(|()| Box::pin(health_check(IsHealthy::Readiness)).compat());
    })
    .resource("/api/relay/healthcheck/live/", |r| {
        r.name("internal-healthcheck-live");
        r.get()
            .with_async(|()| Box::pin(health_check(IsHealthy::Liveness)).compat());
    })
    // live check is also used to check network connectivity by downstream relays.
    // It must have the same url as Sentry (hence two urls doing the same thing)
    .resource("/api/0/relays/live/", |r| {
        r.name("external-healthcheck-live");
        r.get()
            .with_async(|()| Box::pin(health_check(IsHealthy::Liveness)).compat());
    })
}
