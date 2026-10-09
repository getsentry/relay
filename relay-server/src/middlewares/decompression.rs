use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use axum::RequestExt;
use axum::body::Body;
use axum::extract::{MatchedPath, Request};
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::Frame;
use tower::ServiceExt;
use tower_http::decompression::{DecompressionBody, RequestDecompression};

use crate::middlewares::metrics;
use crate::statsd::RelayDistributions;

/// Decompresses request bodies and records the ratio of bytes read after and before decompression.
///
/// Use this with [`axum::middleware::from_fn`].
pub async fn decompress(mut request: Request, next: Next) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());
    let content_encoding = metrics::content_encoding_tag(&request);

    let encoded_size = Arc::new(AtomicU64::new(0));
    let request = request.map(|body| body.map_frame(count_bytes(Arc::clone(&encoded_size))));

    let decoded_size = Arc::new(AtomicU64::new(0));
    let decompression =
        RequestDecompression::new(next.map_request(|request: Request<DecompressionBody<_>>| {
            request.map(|body| Body::new(body.map_frame(count_bytes(Arc::clone(&decoded_size)))))
        }));
    let Ok(response) = decompression.oneshot(request).await;

    relay_statsd::metric!(
        distribution(RelayDistributions::DecompressionRatio) = decoded_size.load(Ordering::Relaxed)
            as f64
            / encoded_size.load(Ordering::Relaxed).max(1) as f64,
        route = route,
        content_encoding = content_encoding,
    );

    response.map(Body::new)
}

fn count_bytes(count: Arc<AtomicU64>) -> impl FnMut(Frame<Bytes>) -> Frame<Bytes> {
    move |frame| {
        if let Some(data) = frame.data_ref() {
            count.fetch_add(data.len() as u64, Ordering::Relaxed);
        }
        frame
    }
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
