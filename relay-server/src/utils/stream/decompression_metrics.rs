use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use axum::body::HttpBody;
use bytes::{Buf, Bytes};
use futures::Stream;
use hyper::body::{Frame, SizeHint};

use crate::statsd::RelayDistributions;

/// Counts bytes on both sides of a decoder and transfers reporting to its output.
pub(crate) struct DecompressionMetrics {
    encoded_size: Arc<AtomicU64>,
    decoded_size: Arc<AtomicU64>,
    ratio: Option<DecompressionRatio>,
}

impl DecompressionMetrics {
    /// Creates counters and a report for a single decoded body or stream.
    pub fn new(route: Cow<'static, str>, content_encoding: &'static str) -> Self {
        let encoded_size = Arc::new(AtomicU64::new(0));
        let decoded_size = Arc::new(AtomicU64::new(0));
        let ratio = Some(DecompressionRatio {
            encoded_size: Arc::clone(&encoded_size),
            decoded_size: Arc::clone(&decoded_size),
            route,
            content_encoding,
        });
        Self {
            encoded_size,
            decoded_size,
            ratio,
        }
    }

    /// Counts input bytes without owning the metric's lifetime.
    pub fn count_encoded<T>(&self, inner: T) -> CountingBody<T> {
        CountingBody {
            inner,
            count: Arc::clone(&self.encoded_size),
            metrics: None,
        }
    }

    /// Counts output bytes and transfers reporting on completion, error, or drop.
    pub fn count_decoded<T>(&mut self, inner: T) -> CountingBody<T> {
        CountingBody {
            inner,
            count: Arc::clone(&self.decoded_size),
            metrics: self.ratio.take(),
        }
    }
}

/// Records the final observed counts, including partial reads on drop.
struct DecompressionRatio {
    encoded_size: Arc<AtomicU64>,
    decoded_size: Arc<AtomicU64>,
    route: Cow<'static, str>,
    content_encoding: &'static str,
}

impl Drop for DecompressionRatio {
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

/// A body or byte stream that counts bytes and holds an optional reporting guard.
pub(crate) struct CountingBody<T> {
    inner: T,
    count: Arc<AtomicU64>,
    metrics: Option<DecompressionRatio>,
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

impl<S, E> Stream for CountingBody<S>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_next(cx);
        if let Poll::Ready(Some(Ok(bytes))) = &poll {
            this.count.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        if matches!(poll, Poll::Ready(None | Some(Err(_)))) {
            this.metrics.take();
        }
        poll
    }
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use futures::{StreamExt, TryStreamExt};
    use http_body_util::BodyExt;

    use super::*;

    #[test]
    fn counting_stream_preserves_polling_and_counts_only_data() {
        #[derive(Default)]
        struct WakeCount(AtomicU64);

        impl std::task::Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let first = Bytes::from_static(b"hello");
        let second = Bytes::from_static(b" world");
        let mut steps = [
            Poll::Pending,
            Poll::Ready(Some(Ok(first.clone()))),
            Poll::Pending,
            Poll::Ready(Some(Ok(Bytes::new()))),
            Poll::Ready(Some(Err(std::io::Error::from_raw_os_error(27)))),
            Poll::Ready(Some(Ok(second.clone()))),
            Poll::Ready(None),
        ]
        .into_iter();
        let polls = Arc::new(AtomicU64::new(0));
        let source_polls = Arc::clone(&polls);
        let source = futures::stream::poll_fn(move |cx| {
            source_polls.fetch_add(1, Ordering::Relaxed);
            let poll = steps.next().expect("the source is only polled seven times");
            if poll.is_pending() {
                cx.waker().wake_by_ref();
            }
            poll
        });
        let count = Arc::new(AtomicU64::new(0));
        let mut stream = CountingBody {
            inner: source,
            count: Arc::clone(&count),
            metrics: None,
        };
        let wakes = Arc::new(WakeCount::default());
        let waker = std::task::Waker::from(Arc::clone(&wakes));
        let mut cx = Context::from_waker(&waker);

        assert!(Pin::new(&mut stream).poll_next(&mut cx).is_pending());
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        assert_eq!(wakes.0.load(Ordering::Relaxed), 1);
        assert_eq!(count.load(Ordering::Relaxed), 0);

        let Poll::Ready(Some(Ok(actual))) = Pin::new(&mut stream).poll_next(&mut cx) else {
            panic!("expected the first chunk");
        };
        assert_eq!(actual, first);
        assert_eq!(actual.as_ptr(), first.as_ptr());
        assert_eq!(count.load(Ordering::Relaxed), 5);

        assert!(Pin::new(&mut stream).poll_next(&mut cx).is_pending());
        assert_eq!(wakes.0.load(Ordering::Relaxed), 2);
        assert_eq!(count.load(Ordering::Relaxed), 5);
        assert!(matches!(
            Pin::new(&mut stream).poll_next(&mut cx),
            Poll::Ready(Some(Ok(bytes))) if bytes.is_empty()
        ));
        assert_eq!(count.load(Ordering::Relaxed), 5);
        assert!(matches!(
            Pin::new(&mut stream).poll_next(&mut cx),
            Poll::Ready(Some(Err(error))) if error.raw_os_error() == Some(27)
        ));
        assert_eq!(count.load(Ordering::Relaxed), 5);

        let Poll::Ready(Some(Ok(actual))) = Pin::new(&mut stream).poll_next(&mut cx) else {
            panic!("expected the second chunk after the error");
        };
        assert_eq!(actual, second);
        assert_eq!(actual.as_ptr(), second.as_ptr());
        assert_eq!(count.load(Ordering::Relaxed), 11);
        assert!(matches!(
            Pin::new(&mut stream).poll_next(&mut cx),
            Poll::Ready(None)
        ));
        assert_eq!(count.load(Ordering::Relaxed), 11);
        assert_eq!(polls.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn decompression_metric_follows_stream_lifetime() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        for (case, as_body) in ["eof", "error", "partial", "unread"]
            .into_iter()
            .flat_map(|case| [(case, false), (case, true)])
        {
            let mut retained = None;
            let captures = relay_statsd::with_capturing_test_client(|| {
                runtime.block_on(async {
                    let last = if case == "error" {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::FileTooLarge,
                            "source failed",
                        ))
                    } else {
                        Ok(Bytes::from_static(b" world"))
                    };
                    let source = futures::stream::iter([Ok(Bytes::from_static(b"hello")), last]);
                    let mut metrics = DecompressionMetrics::new("/test".into(), "other");
                    let mut stream = if as_body {
                        let body = metrics.count_encoded(Body::from_stream(source));
                        metrics.count_decoded(body).into_data_stream().boxed()
                    } else {
                        let stream = metrics.count_encoded(source);
                        // Counting wrappers must preserve the original stream error kind.
                        let stream = metrics.count_decoded(stream).inspect(|result| {
                            if let Err(error) = result {
                                assert_eq!(error.kind(), std::io::ErrorKind::FileTooLarge);
                            }
                        });
                        stream.map_err(axum::Error::new).boxed()
                    };
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
