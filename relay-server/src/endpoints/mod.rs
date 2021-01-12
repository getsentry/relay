//! Web server endpoints.
//!
//! This module contains implementations for all supported relay endpoints, as well as a generic
//! `forward` endpoint that sends unknown requests to the upstream.

use crate::service::ServiceApp;

mod attachments;
mod common;
mod envelope;
mod events;
mod forward;
mod healthcheck;
mod js_loader;
mod minidump;
mod outcomes;
mod project_configs;
mod public_keys;
mod security_report;
mod statics;
mod store;
mod unreal;

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app
        // Internal routes pointing to /api/relay
        .configure(healthcheck::configure_app)
        .configure(events::configure_app)
        .handler("/api/relay", statics::not_found)
        // Web API routes pointing to /api/0
        .configure(project_configs::configure_app)
        .configure(public_keys::configure_app)
        .configure(outcomes::configure_app)
        // Ingestion routes pointing to /api/<project_id>/
        .configure(store::configure_app)
        .configure(envelope::configure_app)
        .configure(security_report::configure_app)
        .configure(minidump::configure_app)
        .configure(attachments::configure_app)
        .configure(unreal::configure_app)
        .configure(js_loader::configure_app)
        // `forward` must be last as it creates a wildcard proxy
        .configure(forward::configure_app)
}
