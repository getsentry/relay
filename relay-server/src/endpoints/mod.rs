//! Web server endpoints.
//!
//! This module contains implementations for all supported relay endpoints, as well as a generic
//! `forward` endpoint that sends unknown requests to the upstream.

// mod attachments;
mod common;
// mod envelope;
mod events;
// mod forward;
mod health_check;
// mod minidump;
mod outcomes;
mod project_configs;
mod public_keys;
// mod security_report;
mod statics;
mod store;
// mod unreal;

use axum::routing::any;
use axum::Router;

use crate::service::ServiceState;

pub fn routes() -> Router<ServiceState> {
    Router::<ServiceState>::new()
        // Internal routes pointing to /api/relay
        .merge(health_check::routes())
        .merge(events::routes())
        .route("/api/relay/*path", any(statics::not_found))
        // Web API routes pointing to /api/0
        .merge(project_configs::routes())
        .merge(public_keys::routes())
        .merge(outcomes::routes())
        // Ingestion routes pointing to /api/<project_id>/
        .merge(store::routes())
    // .merge(envelope::configure_app)
    // .merge(security_report::configure_app)
    // .merge(minidump::configure_app)
    // .merge(attachments::configure_app)
    // .merge(unreal::configure_app)
    // // `forward` must be last as it creates a wildcard proxy
    // .merge(forward::configure_app)
}
