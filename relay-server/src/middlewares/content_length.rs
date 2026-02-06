use axum::RequestExt;
use axum::extract::{MatchedPath, Request};
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;

use crate::statsd::RelayDistributions;

/// A middleware that logs request Content-Length as a statsd distribution metric.
///
/// Use this with [`axum::middleware::from_fn`].
pub async fn content_length(mut request: Request, next: Next) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());

    let (has_content_length, content_length) = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(|len| ("true", len))
        .unwrap_or(("false", 0));

    let response = next.run(request).await;

    relay_statsd::metric!(
        distribution(RelayDistributions::ContentLength) = content_length,
        has_content_length = has_content_length,
        route = route,
        status_code = response.status().as_str(),
    );

    response
}
