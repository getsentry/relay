use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use axum::RequestExt;
use axum::body::{Body, HttpBody};
use axum::extract::{MatchedPath, Request};
use axum::http::header;
use axum::middleware::Next;
use axum::response::Response;
use bytes::Buf;
use hyper::body::{Frame, SizeHint};
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
    let request = request.map(|body| CountingBody {
        inner: body,
        count: Arc::clone(&encoded_size),
    });

    let decoded_size = Arc::new(AtomicU64::new(0));
    let decompression =
        RequestDecompression::new(next.map_request(|request: Request<DecompressionBody<_>>| {
            request.map(|body| {
                Body::new(CountingBody {
                    inner: body,
                    count: Arc::clone(&decoded_size),
                })
            })
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

struct CountingBody<B> {
    inner: B,
    count: Arc<AtomicU64>,
}

impl<B: HttpBody + Unpin> HttpBody for CountingBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_frame(cx);
        if let Poll::Ready(Some(Ok(frame))) = &poll
            && let Some(data) = frame.data_ref()
        {
            this.count
                .fetch_add(data.remaining() as u64, Ordering::Relaxed);
        }
        poll
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
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
