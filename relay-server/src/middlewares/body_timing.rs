use crate::statsd::RelayTimers;
use axum::body::{Body, HttpBody};
use axum::http::Request;
use hyper::body::Frame;
use relay_statsd::metric;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;
use tower::{Layer, Service};

/// Middleware layer that wraps the request [`Body`] to measure body reading duration.
///
/// Metrics are tagged with a completion status to track whether the body:
/// - was read to completion ("completed")
/// - failed during reading ("failed")
/// - was dropped before completion ("dropped")
///
/// The reported size reflects the actual number of bytes read, which may differ from
/// the `Content-Length` header. For bodies that fail or are dropped mid-reading,
/// the size represents only the bytes successfully read before the interruption.
#[derive(Clone, Debug)]
pub struct BodyTimingLayer;

impl BodyTimingLayer {
    pub fn new() -> Self {
        Self {}
    }
}

impl<S> Layer<S> for BodyTimingLayer {
    type Service = BodyTiming<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BodyTiming::new(inner)
    }
}

#[derive(Debug, Copy, Clone)]
pub struct BodyTiming<S> {
    inner: S,
}

impl<S> BodyTiming<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> Service<Request<Body>> for BodyTiming<S>
where
    S: Service<Request<Body>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let (parts, body) = req.into_parts();
        let timed_body = Body::new(TimedBody::new(body, parts.uri.to_string()));
        let request = Request::from_parts(parts, timed_body);
        self.inner.call(request)
    }
}

#[derive(Debug)]
struct TimedBody {
    inner: Body,
    reading_started_at: Option<Instant>,
    size: u32,
    route: String,
}

impl TimedBody {
    fn new<T>(inner: Body, route: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            inner,
            reading_started_at: None,
            size: 0,
            route: route.into(),
        }
    }

    fn finish_timing(&mut self, status: &str) {
        if let Some(started_at) = self.reading_started_at.take() {
            self.emit_metric(started_at.elapsed(), status);
        }
    }

    fn emit_metric(&self, duration: Duration, status: &str) {
        metric!(
            timer(RelayTimers::BodyReading) = duration,
            route = &self.route,
            size = size_category(self.size),
            status = status
        )
    }
}

impl hyper::body::Body for TimedBody {
    type Data = <Body as HttpBody>::Data;
    type Error = <Body as HttpBody>::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.reading_started_at.is_none() {
            self.reading_started_at = Some(Instant::now());
        }
        let pinned = Pin::new(&mut self.inner);
        let poll_result = pinned.poll_frame(cx);

        if let Poll::Ready(Some(Ok(frame))) = &poll_result {
            if let Some(data) = frame.data_ref() {
                self.size += data.len() as u32;
            }
        }

        match &poll_result {
            Poll::Ready(None) => self.finish_timing("completed"),
            Poll::Ready(Some(Err(_))) => self.finish_timing("failed"),
            _ => {}
        }

        poll_result
    }
}

impl Drop for TimedBody {
    fn drop(&mut self) {
        if let Some(started_at) = self.reading_started_at.take() {
            let duration = started_at.elapsed();
            self.emit_metric(duration, "dropped");
        }
    }
}

fn size_category(size: u32) -> &'static str {
    match size {
        0..1_000 => "<1KB",
        1_000..100_000 => "<100KB",
        100_000..1_000_000 => "<1MB",
        1_000_000..100_000_000 => "<100MB",
        _ => ">=100MB",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::HttpBody;
    use futures::task::noop_waker_ref;
    use relay_statsd::with_capturing_test_client;

    struct ErrorBody;

    impl hyper::body::Body for ErrorBody {
        type Data = bytes::Bytes;
        type Error = <Body as HttpBody>::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(Some(Err(axum::Error::new("error"))))
        }
    }

    #[test]
    fn test_empty_body() {
        let captures = with_capturing_test_client(|| {
            let waker = noop_waker_ref();
            let mut cx = Context::from_waker(waker);

            let empty_body = Body::from(vec![]);
            let mut timed_body = TimedBody::new(empty_body, "example/route".to_string());
            let pinned = Pin::new(&mut timed_body);

            let _ = pinned.poll_frame(&mut cx);
        });
        assert_eq!(
            captures,
            ["body.reading.duration:0|ms|#route:example/route,size:<1KB,status:completed"]
        );
    }

    #[test]
    fn test_body() {
        let captures = with_capturing_test_client(|| {
            let waker = noop_waker_ref();
            let mut cx = Context::from_waker(waker);

            let body = Body::new("cool test".to_string());
            let mut timed_body = TimedBody::new(body, "example/route".to_string());
            let mut pinned = Pin::new(&mut timed_body);

            let _ = pinned.as_mut().poll_frame(&mut cx);
            let _ = pinned.as_mut().poll_frame(&mut cx);
        });
        assert_eq!(
            captures,
            ["body.reading.duration:0|ms|#route:example/route,size:<1KB,status:completed"]
        );
    }

    #[test]
    fn test_dropped_while_reading() {
        let captures = with_capturing_test_client(|| {
            let waker = noop_waker_ref();
            let mut cx = Context::from_waker(waker);

            let body = Body::new("just calling this once".to_string());
            let mut timed_body = TimedBody::new(body, "example/route".to_string());
            let pinned = Pin::new(&mut timed_body);

            let _ = pinned.poll_frame(&mut cx);
        });
        assert_eq!(
            captures,
            ["body.reading.duration:0|ms|#route:example/route,size:<1KB,status:dropped"]
        )
    }

    #[test]
    fn test_dropped_before_reading() {
        let captures = with_capturing_test_client(|| {
            let body = Body::new("dropped".to_string());
            let _ = TimedBody::new(body, "example/route".to_string());
        });
        assert_eq!(captures.len(), 0);
    }

    #[test]
    fn test_failed_body() {
        let captures = with_capturing_test_client(|| {
            let waker = noop_waker_ref();
            let mut cx = Context::from_waker(waker);

            let body = Body::new(ErrorBody {});
            let mut timed_body = TimedBody::new(body, "example/route".to_string());

            let pinned = Pin::new(&mut timed_body);
            let _ = pinned.poll_frame(&mut cx);
        });
        assert_eq!(
            captures,
            ["body.reading.duration:0|ms|#route:example/route,size:<1KB,status:failed"]
        )
    }

    #[test]
    fn test_large_body() {
        let captures = with_capturing_test_client(|| {
            let waker = noop_waker_ref();
            let mut cx = Context::from_waker(waker);

            let data = (0..2000).map(|i| i as u8).collect::<Vec<u8>>();

            let body = Body::from(data);
            let mut timed_body = TimedBody::new(body, "example/route".to_string());

            let mut pinned = Pin::new(&mut timed_body);
            while let Poll::Ready(Some(Ok(_))) = pinned.as_mut().poll_frame(&mut cx) {}
        });
        assert_eq!(
            captures,
            ["body.reading.duration:0|ms|#route:example/route,size:<100KB,status:completed"]
        )
    }

    #[test]
    fn test_size_category() {
        assert_eq!(size_category(10), "<1KB");
        assert_eq!(size_category(10_000), "<100KB");
        assert_eq!(size_category(99_999), "<100KB");
        assert_eq!(size_category(1_000_000), "<100MB");
        assert_eq!(size_category(50_000_000), "<100MB");
        assert_eq!(size_category(100_000_000), ">=100MB")
    }
}
