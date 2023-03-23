use std::time::Duration;

use axum::http::{HeaderName, Method};
use tower_http::cors::{Any, CorsLayer};

/// Creates a preconfigured CORS middleware builder for store requests.
///
/// To configure CORS, register endpoints using `resource()` and finalize by calling `register()`,
/// which returns an App. This configures POST as allowed method, allows default sentry headers, and
/// exposes the return headers.
pub fn cors() -> CorsLayer {
    CorsLayer::new()
        // This should also contain GET for the /store/ endpoint. Axum emits a correct "allow"
        // header for this. In practice, this is not an issue, so we can be more restrictive.
        .allow_methods(Method::POST)
        .allow_headers([
            HeaderName::from_static("x-sentry-auth"),
            HeaderName::from_static("x-requested-with"),
            HeaderName::from_static("x-forwarded-for"),
            HeaderName::from_static("origin"),
            HeaderName::from_static("referer"),
            HeaderName::from_static("accept"),
            HeaderName::from_static("content-type"),
            HeaderName::from_static("authentication"),
            HeaderName::from_static("authorization"),
            HeaderName::from_static("content-encoding"),
            HeaderName::from_static("transfer-encoding"),
        ])
        .allow_origin(Any)
        .expose_headers([
            HeaderName::from_static("x-sentry-error"),
            HeaderName::from_static("x-sentry-rate-limits"),
            HeaderName::from_static("retry-after"),
        ])
        .max_age(Duration::from_secs(3600))
}
