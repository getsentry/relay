use crate::statsd::RelayTimers;
use axum::body::{Body, HttpBody};
use axum::http::Request;
use hyper::body::Frame;
use relay_statsd::metric;
use std::fmt::{Display, Formatter};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::Instant;
use tower::{Layer, Service};

/// Middleware layer that wraps the request [`Body`] so that we can measure
/// how long reading the body took.
#[derive(Clone)]
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

struct TimedBody {
    inner: Body,
    reading_started_at: Option<Instant>,
    size: usize,
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
        if matches!(poll_result, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            if let Some(started_at) = self.reading_started_at.take() {
                let duration = started_at.elapsed();

                metric!(
                    timer(RelayTimers::BodyReading) = duration,
                    route = &self.route,
                    size = &format!("{}", SizeBracket::bracket(self.size)),
                    status = match poll_result {
                        Poll::Ready(Some(Err(_))) => "failed",
                        _ => "completed",
                    }
                );
            }
        }
        if let Poll::Ready(Some(Ok(v))) = &poll_result {
            if let Some(data) = v.data_ref() {
                self.size += data.len();
            }
        }
        poll_result
    }
}

enum SizeBracket {
    LessThan1K,
    LessThan10K,
    LessThan100K,
    Greater100K,
}

impl SizeBracket {
    fn bracket(size: usize) -> Self {
        match size {
            0..1_000 => SizeBracket::LessThan1K,
            1_000..10_000 => SizeBracket::LessThan10K,
            10_000..100_000 => SizeBracket::LessThan100K,
            _ => SizeBracket::Greater100K,
        }
    }
}

impl Display for SizeBracket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                SizeBracket::LessThan1K => "<1K",
                SizeBracket::LessThan10K => "<10K",
                SizeBracket::LessThan100K => "<100K",
                SizeBracket::Greater100K => ">100K",
            }
        )
    }
}

impl Drop for TimedBody {
    fn drop(&mut self) {
        if let Some(started_at) = self.reading_started_at.take() {
            let duration = started_at.elapsed();
            metric!(
                timer(RelayTimers::BodyReading) = duration,
                route = &self.route,
                size = &format!("{}", SizeBracket::bracket(self.size)),
                status = "dropped"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::HttpBody;
    use futures::task::noop_waker_ref;
    use relay_statsd::with_capturing_test_client;

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
            ["body.reading.duration:0|ms|#route:example/route,size:<1K,status:completed"]
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
            ["body.reading.duration:0|ms|#route:example/route,size:<1K,status:completed"]
        );
    }

    #[test]
    fn test_dropped_while_reading() {
        let captures = with_capturing_test_client(|| {
            let waker = noop_waker_ref();
            let mut cx = Context::from_waker(waker);

            let body = Body::new("long body so it will drop".to_string());
            let mut timed_body = TimedBody::new(body, "example/route".to_string());
            let mut pinned = Pin::new(&mut timed_body);

            let _ = pinned.as_mut().poll_frame(&mut cx);
        });
        assert_eq!(
            captures,
            ["body.reading.duration:0|ms|#route:example/route,size:<1K,status:dropped"]
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
}
