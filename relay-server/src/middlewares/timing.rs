use crate::statsd::RelayTimers;
use axum::body::{Body, HttpBody};
use axum::http::Request;
use axum::middleware::Next;
use bytes::Bytes;
use http_body::Frame;
use relay_statsd::metric;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::time::Instant;

pub async fn body_timing<B>(request: Request<B>, next: Next) -> axum::response::Response
where
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: std::error::Error + Send + Sync,
{
    let (parts, body) = request.into_parts();
    let timed_body = Body::new(TimedBody::new(body, parts.uri.to_string()));
    let request = Request::from_parts(parts, timed_body);

    next.run(request).await
}

pin_project_lite::pin_project! {
    struct TimedBody<B>
    where
        B: HttpBody<Data = bytes::Bytes>,
    {
        #[pin]
        inner: Pin<Box<B>>,
        reading_starts: Option<Instant>,
        route: String
    }
}

impl<B> TimedBody<B>
where
    B: HttpBody<Data = Bytes>,
{
    fn new(inner: B, route: String) -> Self {
        Self {
            inner: Box::pin(inner),
            reading_starts: None,
            route,
        }
    }
}

impl<B> HttpBody for TimedBody<B>
where
    B: HttpBody<Data = Bytes>,
{
    type Data = <B as HttpBody>::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.as_mut().project();

        if this.reading_starts.is_none() {
            *this.reading_starts = Some(Instant::now());
        }
        let poll_result = this.inner.poll_frame(cx);
        match poll_result {
            Poll::Ready(None) => {
                if let Some(start_time) = this.reading_starts {
                    let duration = start_time.elapsed();
                    metric!(
                        timer(RelayTimers::BodyReading) = duration,
                        label = "body_reading_testing",
                        route = this.route
                    );
                    *this.reading_starts = None;
                }
                Poll::Ready(None)
            }
            other => other,
        }
    }
}
