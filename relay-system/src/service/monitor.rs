use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use crate::service::registry::ServiceId;

pin_project_lite::pin_project! {
    pub struct ServiceMonitor<F> {
        id: ServiceId,
        #[pin]
        inner: F,

        metrics: Arc<RawMetrics>,

    }
}

impl<F> ServiceMonitor<F> {
    pub fn wrap(id: ServiceId, inner: F) -> Self {
        Self {
            id,
            inner,
            metrics: Arc::new(RawMetrics {
                poll_count: AtomicU64::new(0),
                total_duration_ns: AtomicU64::new(0),
            }),
        }
    }

    pub fn metrics(&self) -> &Arc<RawMetrics> {
        &self.metrics
    }
}

impl<F> Future for ServiceMonitor<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let poll_start = Instant::now();

        let this = self.project();
        this.metrics.poll_count.fetch_add(1, Ordering::Relaxed);

        let ret = this.inner.poll(cx);

        let poll_end = Instant::now();
        let poll_duration = poll_end.saturating_duration_since(poll_start);
        let poll_duration_ns = poll_duration.as_nanos().try_into().unwrap_or(u64::MAX);
        this.metrics
            .total_duration_ns
            .fetch_add(poll_duration_ns, Ordering::Relaxed);

        ret
    }
}

#[derive(Debug)]
pub struct RawMetrics {
    pub poll_count: AtomicU64,
    pub total_duration_ns: AtomicU64,
}
