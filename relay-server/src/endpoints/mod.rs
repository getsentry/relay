//! Web server endpoints.
//!
//! This module contains implementations for all supported relay endpoints, as well as a generic
//! `forward` endpoint that sends unknown requests to the upstream.

mod attachments;
mod common;
mod envelope;
mod events;
mod forward;
mod health_check;
mod minidump;
mod outcomes;
mod project_configs;
mod public_keys;
mod security_report;
mod statics;
mod store;
mod unreal;

use axum::routing::{any, get, post, Router};

use crate::service::ServiceState;

#[rustfmt::skip]
pub fn routes() -> Router<ServiceState> {
    // Relay-internal routes pointing to /api/relay/
    let internal_routes = Router::new()
        .route("/api/relay/healthcheck/:kind/", get(health_check::handle))
        .route("/api/relay/events/:event_id/", get(events::handle))
        .route("/api/relay/*not_found", any(statics::not_found));

    // Sentry Web API routes pointing to /api/0/relays/
    let web_routes = Router::new()
        .route("/api/0/relays/projectconfigs/", post(project_configs::handle))
        .route("/api/0/relays/publickeys/", post(public_keys::handle))
        .route("/api/0/relays/outcomes/", post(outcomes::handle))
        .route("/api/0/relays/:kind/", get(health_check::handle));

    // Ingestion routes pointing to /api/:project_id/
    let store_routes = Router::new()
        // Legacy store path that is missing the project parameter.
        .route("/api/store/", post(store::handle).get(store::handle))
        .route("/api/:project_id/store/", post(store::handle).get(store::handle))
        .route("/api/:project_id/envelope/", post(envelope::handle))
        .route("/api/:project_id/security/", post(security_report::handle))
        .route("/api/:project_id/csp-report/", post(security_report::handle))
        // No mandatory trailing slash here because people already use it like this.
        .route("/api/:project_id/minidump", post(minidump::handle))
        .route("/api/:project_id/minidump/", post(minidump::handle))
        .route("/api/:project_id/events/:event_id/attachments/", post(attachments::handle))
        .route("/api/:project_id/unreal/:sentry_key/", post(unreal::handle))
        .route_layer(common::cors());

    Router::new()
        .merge(internal_routes)
        .merge(web_routes)
        .merge(store_routes)
        // The "/api/" path is special as it is actually a web UI endpoint
        .route("/api/", any(statics::not_found))
        // Forward all other routes to the upstream
        .fallback(forward::handle)
}
