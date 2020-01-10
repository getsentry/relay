//! Web server endpoints.
//!
//! This module contains implementations for all supported relay endpoints, as well as a generic
//! `forward` endpoint that sends unknown requests to the upstream.

use actix_web::HttpResponse;

use crate::service::ServiceApp;

mod attachments;
mod common;
mod events;
mod forward;
mod healthcheck;
mod minidump;
mod project_configs;
mod public_keys;
mod security_report;
mod store;
mod unreal;

pub fn configure_app(app: ServiceApp) -> ServiceApp {
    app.scope("/api/relay", |mut scope| {
        scope = healthcheck::configure_scope(scope);
        scope = events::configure_scope(scope);

        // never forward /api/relay, as that prefix is used for stuff like healthchecks
        scope.default_resource(|r| r.f(|_| HttpResponse::NotFound()))
    })
    .configure(project_configs::configure_app)
    .configure(public_keys::configure_app)
    .configure(store::configure_app)
    .configure(security_report::configure_app)
    .configure(minidump::configure_app)
    .configure(attachments::configure_app)
    .configure(unreal::configure_app)
    // `forward` must be last as it creates a wildcard proxy
    .configure(forward::configure_app)
}
