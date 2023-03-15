//! A simple health check endpoint for the relay.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::actors::health_check::{HealthCheck, IsHealthy};

#[derive(Serialize)]
struct Status {
    is_healthy: bool,
}

async fn health_check(Path(kind): Path<IsHealthy>) -> impl IntoResponse {
    match HealthCheck::from_registry().send(kind).await {
        Ok(true) => (StatusCode::OK, Json(Status { is_healthy: true })),
        _ => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Status { is_healthy: false }),
        ),
    }
}

pub fn routes<S>() -> Router<S> {
    Router::new()
        // r.name("internal-healthcheck-ready");
        .route("/api/relay/healthcheck/:kind/", get(health_check))
        // r.name("internal-healthcheck-live");
        .route("/api/0/relays/:kind/", get(health_check))
        .with_state(())
}
