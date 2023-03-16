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

use axum::routing::{any, get, post};
use axum::Router;

use crate::service::ServiceState;

pub fn routes() -> Router<ServiceState> {
    // Relay-internal routes pointing to /api/relay/
    let internal_routes = Router::new()
        .route("/healthcheck/:kind/", get(health_check::handle)) // r.name("internal-healthcheck-ready");
        .route("/events/:event_id/", get(events::handle)) // r.name("internal-events");
        .fallback(statics::not_found);

    // Sentry Web API routes pointing to /api/0/relays/
    let web_routes = Router::new()
        .route("/projectconfigs/", post(project_configs::handle)) // r.name("relay-projectconfigs");
        .route("/publickeys/", post(public_keys::handle)) // r.name("relay-publickeys");
        .route("/outcomes/", post(outcomes::handle)) // r.name("relay-outcomes");
        .route("/:kind/", get(health_check::handle)); // r.name("internal-healthcheck-live");

    // Ingestion routes pointing to /api/:project_id/
    let store_routes = Router::new()
        .route("/store/", post(store::handle).get(store::handle)) // r.name("store-default");
        .route("/envelope/", post(envelope::handle)) // r.name("store-envelope");
        .route("/security/", post(security_report::handle)) // r.name("store-security-report");
        .route("/csp-report/", post(security_report::handle)) // r.name("store-security-report");
        // No mandatory trailing slash here because people already use it like this.
        .route("/minidump", post(minidump::handle)) // r.name("store-minidump");
        .route("/events/:event_id/attachments/", post(attachments::handle)) // r.name("store-attachment");
        .route("/unreal/:sentry_key/", post(unreal::handle)) // r.name("store-unreal");
        .route_layer(common::cors());

    Router::new()
        .nest("/api/relay", internal_routes)
        .nest("/api/0/relays", web_routes)
        .nest("/api/:project_id", store_routes)
        // Legacy store path that is missing the project parameter.
        .route(
            "/api/store/",
            post(store::handle).get(store::handle).layer(common::cors()),
        ) // r.name("store-legacy");
        // The "/api/" path is special as it is actually a web UI endpoint
        .route("/api/", any(statics::not_found))
        // Forward all other routes to the upstream
        .fallback(forward::handle)
}
