//! A simple health check endpoint for the relay.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Serialize;
use tokio::time::Instant;

use crate::service::ServiceState;
use crate::services::health_check::{IsHealthy, IsHealthyWrapper};
use crate::statsd::RelayTimers;

#[derive(Serialize)]
struct Status {
    is_healthy: bool,
}

pub async fn handle(state: ServiceState, Path(kind): Path<IsHealthy>) -> impl IntoResponse {
    let received = Instant::now();

    let wrapper = IsHealthyWrapper { kind, received };
    let result = state.health_check().send(wrapper).await;

    relay_statsd::metric!(
        timer(RelayTimers::HealthcheckEndpointCheckDuration) = received.elapsed(),
        kind = kind.variant()
    );
    match result {
        Ok(true) => (StatusCode::OK, axum::Json(Status { is_healthy: true })),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(Status { is_healthy: false }),
        ),
    }
}

pub async fn handle_live(state: ServiceState) -> impl IntoResponse {
    handle(state, Path(IsHealthy::Liveness)).await
}
