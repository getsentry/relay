use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
use axum::RequestExt;
use std::time::Instant;

use crate::extractors::ReceivedAt;
use crate::statsd::{RelayCounters, RelayTimers};

/// A middleware that logs web request timings as statsd metrics.
///
/// Use this with [`axum::middleware::from_fn`].
pub async fn metrics(mut request: Request, next: Next) -> Response {
    let request_start = Instant::now();

    let received_at = ReceivedAt::now();
    request.extensions_mut().insert(received_at);

    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());
    let method = request.method().clone();

    relay_statsd::metric!(
        counter(RelayCounters::Requests) += 1,
        route = route,
        method = method.as_str(),
    );

    let response = next.run(request).await;

    relay_statsd::metric!(
        timer(RelayTimers::RequestsDuration) = request_start.elapsed(),
        route = route,
        method = method.as_str(),
    );
    relay_statsd::metric!(
        counter(RelayCounters::ResponsesStatusCodes) += 1,
        status_code = response.status().as_str(),
        route = route,
        method = method.as_str(),
    );

    response
}
