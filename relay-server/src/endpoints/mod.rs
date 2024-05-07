//! Web server endpoints.
//!
//! This module contains implementations for all supported relay endpoints, as well as a generic
//! `forward` endpoint that sends unknown requests to the upstream.

mod attachments;
mod batch_metrics;
mod batch_outcomes;
mod common;
#[cfg(feature = "dashboard")]
mod dashboard;
mod envelope;
mod events;
mod forward;
mod health_check;
#[cfg(feature = "dashboard")]
mod logs;
mod minidump;
mod monitor;
mod nel;
mod project_configs;
mod public_keys;
mod security_report;
mod spans;
mod statics;
#[cfg(feature = "dashboard")]
mod stats;
mod store;
mod unreal;

use axum::extract::DefaultBodyLimit;
use axum::routing::{any, get, post, Router};
use bytes::Bytes;
use relay_config::Config;

use crate::middlewares;
use crate::service::ServiceState;

/// Size limit for internal batch endpoints.
const BATCH_JSON_BODY_LIMIT: usize = 50_000_000; // 50 MB

#[rustfmt::skip]
pub fn routes<B>(config: &Config) -> Router<ServiceState, B>
where
    B: axum::body::HttpBody + Send + 'static,
    B::Data: Send + Into<Bytes>,
    B::Error: Into<axum::BoxError>,
{
    #[cfg(feature = "dashboard")]
    let dashboard = Router::new().route("/dashboard/",get(dashboard::index_handle))
        .route("/dashboard/*file", get(dashboard::handle));
    // Relay-internal routes pointing to /api/relay/
    let internal_routes = Router::new()
        .route("/api/relay/healthcheck/:kind/", get(health_check::handle))
        .route("/api/relay/events/:event_id/", get(events::handle));
    #[cfg(feature = "dashboard")]
    let internal_routes = internal_routes
        .route("/api/relay/logs/", get(logs::handle))
        .route("/api/relay/stats/", get(stats::handle));
    let internal_routes = internal_routes
        // Fallback route, but with a name, and just on `/api/relay/*`.
        .route("/api/relay/*not_found", any(statics::not_found));

    // Sentry Web API routes pointing to /api/0/relays/
    let web_routes = Router::new()
        .route("/api/0/relays/projectconfigs/", post(project_configs::handle))
        .route("/api/0/relays/publickeys/", post(public_keys::handle))
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
        .route("/api/:project_id/cron/:monitor_slug/:sentry_key", monitor::route(config))
        .route("/api/:project_id/cron/:monitor_slug/:sentry_key/", monitor::route(config))
        .route("/api/:project_id/cron/:monitor_slug", monitor::route(config))
        .route("/api/:project_id/cron/:monitor_slug/", monitor::route(config))

        .route("/api/:project_id/store/", store::route(config))
        .route("/api/:project_id/envelope/", envelope::route(config))
        .route("/api/:project_id/security/", security_report::route(config))
        .route("/api/:project_id/csp-report/", security_report::route(config))
        .route("/api/:project_id/nel/", nel::route(config))
        // No mandatory trailing slash here because people already use it like this.
        .route("/api/:project_id/minidump", minidump::route(config))
        .route("/api/:project_id/minidump/", minidump::route(config))
        .route("/api/:project_id/events/:event_id/attachments/", attachments::route(config))
        .route("/api/:project_id/unreal/:sentry_key/", unreal::route(config))
        .route("/api/:project_id/spans/", spans::route(config))
        .route_layer(middlewares::cors());

    let router = Router::new();
    #[cfg(feature = "dashboard")]
    let router = router.merge(dashboard);

    router.merge(internal_routes)
        .merge(web_routes)
        .merge(batch_routes)
        .merge(store_routes)
        // Forward all other API routes to the upstream. This will 404 for non-API routes.
        .fallback(forward::forward)
}
