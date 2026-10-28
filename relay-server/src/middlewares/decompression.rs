use std::borrow::Cow;
use std::convert::Infallible;
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
use bytes::{Buf, Bytes};
use http_body_util::Empty;
use hyper::body::{Frame, SizeHint};
use tower::{ServiceExt, service_fn};
use tower_http::BoxError;
use tower_http::decompression::{DecompressionBody, RequestDecompression};

use crate::middlewares::metrics;
use crate::statsd::RelayDistributions;

/// Decompresses request bodies and records the ratio of bytes read after and before decompression.
///
/// Use this with [`axum::middleware::from_fn`].
pub async fn decompress(mut request: Request, next: Next) -> Response {
    let matched_path = request.extract_parts::<MatchedPath>().await;
    let route = matched_path.as_ref().map_or("unknown", |m| m.as_str());
    match decompress_request(request, route.to_owned().into()).await {
        Ok(request) => next.run(request.map(Body::new)).await,
        Err(response) => *response,
    }
}

/// Returns a lazily decoded request whose body records its ratio on completion or drop.
pub(crate) async fn decompress_request<B>(
    request: Request<B>,
    route: Cow<'static, str>,
) -> Result<Request<impl HttpBody<Data = Bytes, Error = BoxError> + Send + Unpin>, Box<Response>>
where
    B: HttpBody<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Into<BoxError> + Send + 'static,
{
    let content_encoding = metrics::content_encoding_tag(&request);

    let encoded_size = Arc::new(AtomicU64::new(0));
    let request = request.map(|body| CountingBody {
        inner: body,
        count: Arc::clone(&encoded_size),
        metrics: None,
    });

    let decoded_size = Arc::new(AtomicU64::new(0));
    let mut metrics = Some(DecompressionMetrics {
        encoded_size,
        decoded_size: Arc::clone(&decoded_size),
        route,
        content_encoding,
    });
    // Tower exposes request decoding through a service. Hand the decoded request to
    // the consumer directly; the empty response keeps response boxing off the payload.
    let mut decoded_request = None;
    let capture = service_fn(|request: Request<DecompressionBody<_>>| {
        decoded_request = Some(request.map(|body| CountingBody {
            inner: body,
            count: Arc::clone(&decoded_size),
            metrics: metrics.take(),
        }));
        std::future::ready(Ok::<_, Infallible>(http::Response::new(
            Empty::<Bytes>::new(),
        )))
    });
    let Ok(response) = RequestDecompression::new(capture).oneshot(request).await;
    match decoded_request {
        Some(request) => Ok(request),
        None => Err(Box::new(response.map(Body::new))),
    }
}

/// Records the final observed counts, including partial reads when a body is dropped.
struct DecompressionMetrics {
    encoded_size: Arc<AtomicU64>,
    decoded_size: Arc<AtomicU64>,
    route: Cow<'static, str>,
    content_encoding: &'static str,
}

impl Drop for DecompressionMetrics {
    fn drop(&mut self) {
        relay_statsd::metric!(
            distribution(RelayDistributions::DecompressionRatio) =
                self.decoded_size.load(Ordering::Relaxed) as f64
                    / self.encoded_size.load(Ordering::Relaxed).max(1) as f64,
            route = self.route.as_ref(),
            content_encoding = self.content_encoding,
        );
    }
}

struct CountingBody<B> {
    inner: B,
    count: Arc<AtomicU64>,
    metrics: Option<DecompressionMetrics>,
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
        if matches!(poll, Poll::Ready(None | Some(Err(_)))) {
            this.metrics.take();
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

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use http_body_util::BodyExt;

    use super::*;

    #[test]
    fn decompression_metric_follows_body_lifetime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for case in ["eof", "error", "partial", "unread"] {
            let mut retained = None;
            let captures = relay_statsd::with_capturing_test_client(|| {
                runtime.block_on(async {
                    let last = if case == "error" {
                        Err(std::io::Error::other("source failed"))
                    } else {
                        Ok(Bytes::from_static(b" world"))
                    };
                    let source = futures::stream::iter([Ok(Bytes::from_static(b"hello")), last]);
                    let request = Request::new(Body::from_stream(source));
                    let request = decompress_request(request, "/test".into()).await.unwrap();
                    let mut stream = request.into_body().into_data_stream();
                    match case {
                        "eof" | "error" => {
                            assert_eq!(stream.next().await.unwrap().unwrap(), "hello");
                            if case == "error" {
                                assert!(stream.next().await.unwrap().is_err());
                            } else {
                                assert_eq!(stream.next().await.unwrap().unwrap(), " world");
                                assert!(stream.next().await.is_none());
                                assert!(stream.next().await.is_none());
                            }
                            // Completion must record while the body is still alive.
                            retained = Some(stream);
                        }
                        "partial" => assert_eq!(stream.next().await.unwrap().unwrap(), "hello"),
                        "unread" => {}
                        _ => unreachable!(),
                    }
                });
            });
            let ratio = if case == "unread" { 0 } else { 1 };
            assert_eq!(
                captures,
                [format!(
                    "requests.decompression_ratio:{ratio}|d|#route:/test,content_encoding:other"
                )],
                "{case}"
            );
            let captures = relay_statsd::with_capturing_test_client(|| drop(retained));
            assert!(captures.is_empty(), "{case} recorded twice");
        }
    }
}
