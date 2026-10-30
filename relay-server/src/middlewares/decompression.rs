use axum::RequestExt;
use axum::body::Body;
use axum::extract::{MatchedPath, Request};
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;
use tower::ServiceExt;
use tower_http::decompression::{DecompressionBody, RequestDecompression};

use crate::middlewares::metrics;
use crate::utils::DecompressionMetrics;

/// Decompresses request bodies and records the ratio of bytes read after and before decompression.
///
/// Use this with [`axum::middleware::from_fn`].
pub async fn decompress(mut request: Request, next: Next) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());
    let content_encoding = metrics::content_encoding_tag(&request);
    let metrics = DecompressionMetrics::new(route.to_owned().into(), content_encoding);
    let request = request.map(|body| metrics.count_encoded(body));
    let decompression =
        RequestDecompression::new(next.map_request(|request: Request<DecompressionBody<_>>| {
            request.map(|body| Body::new(metrics.count_decoded(body)))
        }));
    let Ok(response) = decompression.oneshot(request).await;
    response.map(Body::new)
}

/// Map request middleware that removes empty content encoding headers.
///
/// This is to be used along with [`decompress`].
pub fn remove_empty_encoding(mut request: Request) -> Request {
    if let header::Entry::Occupied(entry) = request.headers_mut().entry(header::CONTENT_ENCODING)
        && should_ignore_encoding(entry.get().as_bytes())
    {
        entry.remove();
    }

    request
}

/// Returns `true` if this content-encoding value should be ignored.
fn should_ignore_encoding(value: &[u8]) -> bool {
    // sentry-ruby/5.x sends an empty string
    // sentry.java.android/2.0.0 sends "UTF-8"
    value == b"" || value.eq_ignore_ascii_case(b"utf-8")
}
