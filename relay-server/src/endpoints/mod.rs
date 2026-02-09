//! Web server endpoints.
//!
//! This module contains implementations for all supported relay endpoints, as well as a generic
//! `forward` endpoint that sends unknown requests to the upstream.

mod attachments;
mod autoscaling;
mod batch_metrics;
mod batch_outcomes;
mod common;
mod envelope;
mod forward;
mod health_check;
mod integrations;
mod minidump;
mod monitor;
mod nel;
#[cfg(sentry)]
mod playstation;
mod project_configs;
mod public_keys;
mod register;
mod security_report;
mod statics;
mod store;
mod unreal;
mod upload;

use axum::extract::DefaultBodyLimit;
use axum::routing::{Router, any, get, post};
use relay_config::Config;

use crate::middlewares;
use crate::service::ServiceState;

/// Size limit for internal batch endpoints.
const BATCH_JSON_BODY_LIMIT: usize = 50_000_000; // 50 MB

/// All of Relay's routes.
///
/// This includes [`public_routes`] as well as [`internal_routes`].
pub fn all_routes(config: &Config) -> Router<ServiceState> {
    public_routes_raw(config).merge(internal_routes(config))
}

/// Relay's internal routes.
///
/// Routes which do not need to be exposed.
#[rustfmt::skip]
pub fn internal_routes(_: &Config) -> Router<ServiceState>{
    Router::new()
        .route("/api/relay/healthcheck/{kind}/", get(health_check::handle))
        .route("/api/relay/autoscaling/", get(autoscaling::handle))
        // Fallback route, but with a name, and just on `/api/relay/*`.
        .route("/api/relay/{*not_found}", any(statics::not_found))
}

/// Relay's public routes.
///
/// Routes which are public API and must be exposed.
pub fn public_routes(config: &Config) -> Router<ServiceState> {
    // Exclude internal routes, they must be configured separately.
    public_routes_raw(config).route("/api/relay/{*not_found}", any(statics::not_found))
}

#[rustfmt::skip]
fn public_routes_raw(config: &Config) -> Router<ServiceState> {
    // Sentry Web API routes pointing to /api/0/relays/
    let web_routes = Router::new()
        .route("/api/0/relays/projectconfigs/", post(project_configs::handle))
        .route("/api/0/relays/publickeys/", post(public_keys::handle))
        .route("/api/0/relays/register/challenge/", post(register::challenge))
        .route("/api/0/relays/register/response/", post(register::response))
        // Network connectivity check for downstream Relays, same as the internal health check.
        .route("/api/0/relays/live/", get(health_check::handle_live))
        .route_layer(DefaultBodyLimit::max(crate::constants::MAX_JSON_SIZE));

    let batch_routes = Router::new()
        .route("/api/0/relays/outcomes/", post(batch_outcomes::handle))
        .route("/api/0/relays/metrics/", post(batch_metrics::handle))
        .route_layer(DefaultBodyLimit::max(BATCH_JSON_BODY_LIMIT));

    // Ingestion routes pointing to /api/:project_id/
    let store_routes = Router::new()
        // Legacy store path that is missing the project parameter.
        .route("/api/store/", store::route(config))
        // cron monitor level routes.  These are user facing APIs and as such support trailing slashes.
        .route("/api/{project_id}/cron/{monitor_slug}/{sentry_key}", monitor::route(config))
        .route("/api/{project_id}/cron/{monitor_slug}/{sentry_key}/", monitor::route(config))
        .route("/api/{project_id}/cron/{monitor_slug}", monitor::route(config))
        .route("/api/{project_id}/cron/{monitor_slug}/", monitor::route(config))

        .route("/api/{project_id}/store/", store::route(config))
        .route("/api/{project_id}/envelope/", envelope::route(config))
        .route("/api/{project_id}/security/", security_report::route(config))
        .route("/api/{project_id}/csp-report/", security_report::route(config))
        .route("/api/{project_id}/nel/", nel::route(config))
        // No mandatory trailing slash here because people already use it like this.
        .route("/api/{project_id}/minidump", minidump::route(config))
        .route("/api/{project_id}/minidump/", minidump::route(config))
        .route("/api/{project_id}/events/{event_id}/attachments/", post(attachments::handle))
        .route("/api/{project_id}/unreal/{sentry_key}/", unreal::route(config))
        .route("/api/{project_id}/upload/", upload::route(config))
        .route("/api/{project_id}/upload", upload::route(config));

    #[cfg(sentry)]
    let store_routes = store_routes.route("/api/{project_id}/playstation/", playstation::route(config));
    let store_routes = store_routes.route_layer(middlewares::cors());

    // Integration routes.
    //
    // For integrations we want to be lenient on trailing `/`, as they are often manually
    // configured by users or protocols may force a specific variant.
    let integration_routes = Router::new()
        .nest("/api/{project_id}/integration/otlp", integrations::otlp::routes(config))
        .nest("/api/{project_id}/integration/vercel", integrations::vercel::routes(config))
        .route_layer(middlewares::cors());

    // NOTE: If you add a new (non-experimental) route here, please also list it in
    // https://github.com/getsentry/sentry-docs/blob/master/docs/product/relay/operating-guidelines.mdx

    Router::new()
        .merge(web_routes)
        .merge(batch_routes)
        .merge(store_routes)
        .merge(integration_routes)
        // Forward all other API routes to the upstream. This will 404 for non-API routes.
        .fallback(forward::forward)
}
