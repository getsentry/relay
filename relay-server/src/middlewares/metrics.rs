use axum::RequestExt;
use axum::extract::{MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use http::header;
use relay_cogs::AppFeature;
use relay_config::HttpEncoding;
use relay_system::{MonitoredFuture, RawMetrics};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::extractors::ReceivedAt;
use crate::service::ServiceState;
use crate::statsd::{RelayCounters, RelayTimers};

/// A middleware that logs web request timings as statsd metrics.
///
/// Use this with [`axum::middleware::from_fn_with_state`].
pub async fn metrics(
    State(state): State<ServiceState>,
    mut request: Request,
    next: Next,
) -> Response {
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
        content_encoding = content_encoding_tag(&request),
    );

    let metrics = Default::default();
    let _cogs = state.cogs().timed_with(
        relay_cogs::ResourceId::Relay,
        AppFeature::UnattributedRequest,
        CpuClock(Arc::clone(&metrics)),
    );

    let response = MonitoredFuture::wrap_with_metrics(next.run(request), metrics).await;

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

pub(super) fn content_encoding_tag(request: &Request) -> &'static str {
    request
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(HttpEncoding::parse)
        .and_then(|enc| enc.name())
        .unwrap_or("other")
}

struct CpuClock(Arc<RawMetrics>);

impl relay_cogs::time::Clock for CpuClock {
    fn now(&self) -> relay_cogs::time::Instant {
        // Note that this measurement is slightly off, as the metric only updates at the end of the
        // poll and not during the poll. So if there is a measurement started during a poll, the start
        // of that measurement is before whatever happened during the latest poll starting the measurement.
        //
        // Since the main purpose here is to estimate time and collect the _total_ duration of the request
        // in CPU time, this is acceptable.
        let duration = Duration::from_nanos(self.0.total_duration_ns.load(Ordering::Relaxed));
        relay_cogs::time::Instant::from_duration(duration)
    }
}
