use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::service::registry::ServiceId;

/// Minimum interval when utilization is recalculated.
const UTILIZATION_UPDATE_THRESHOLD: Duration = Duration::from_secs(5);

pin_project_lite::pin_project! {
    pub struct ServiceMonitor<F> {
        id: ServiceId,
        #[pin]
        inner: F,

        metrics: Arc<RawMetrics>,

        last_utilization_update: Instant,
        last_utilization_duration_ns: u64,
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
                utilization: AtomicU8::new(0),
            }),
            last_utilization_update: Instant::now(),
            last_utilization_duration_ns: 0,
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
        let poll_duration = poll_end - poll_start;
        let poll_duration_ns = poll_duration.as_nanos().try_into().unwrap_or(u64::MAX);

        let previous_total_duration = this
            .metrics
            .total_duration_ns
            .fetch_add(poll_duration_ns, Ordering::Relaxed);
        let total_duration_ns = previous_total_duration + poll_duration_ns;

        let utilization_duration = poll_end - *this.last_utilization_update;
        if utilization_duration >= UTILIZATION_UPDATE_THRESHOLD {
            // Time spent the service was busy since the last utilization calculation.
            let busy = total_duration_ns - *this.last_utilization_duration_ns;

            // The maximum possible time spent busy is the total time between the last measurement
            // and the current measurement. We can extract a percentage from this.
            let percentage = (busy * 100).div_ceil(utilization_duration.as_nanos().max(1) as u64);
            this.metrics
                .utilization
                .store(percentage.min(100) as u8, Ordering::Relaxed);

            *this.last_utilization_duration_ns = total_duration_ns;
            *this.last_utilization_update = poll_end;
        }

        ret
    }
}

#[derive(Debug)]
pub struct RawMetrics {
    pub poll_count: AtomicU64,
    pub total_duration_ns: AtomicU64,
    pub utilization: AtomicU8,
}
