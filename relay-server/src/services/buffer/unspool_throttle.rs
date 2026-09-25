use std::num::NonZeroU32;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

/// Paces how quickly envelopes are read back from disk.
///
/// The throttle is shared between all partitions of the envelope buffer. This is needed because
/// with round-robin, a project's stacks exist on multiple partitions.
#[derive(Debug)]
pub struct UnspoolThrottle {
    envelopes_per_second: f64,
    state: Mutex<ThrottleState>,
}

#[derive(Debug)]
struct ThrottleState {
    remaining: f64,
    last_refill: Instant,
}

impl UnspoolThrottle {
    /// Creates a new [`UnspoolThrottle`] with the given rate.
    pub fn new(envelopes_per_second: NonZeroU32) -> Self {
        let envelopes_per_second = f64::from(envelopes_per_second.get());
        Self {
            envelopes_per_second,
            state: Mutex::new(ThrottleState {
                remaining: envelopes_per_second,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Accounts `envelopes` against the throttle and waits until the rate is back within
    /// the configured limit.
    ///
    /// The envelopes are deducted immediately and any debt is paid off by waiting.
    pub async fn acquire(&self, envelopes: usize) {
        let wait = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);

            let now = Instant::now();
            let elapsed = now.duration_since(state.last_refill).as_secs_f64();
            state.last_refill = now;

            state.remaining = (state.remaining + elapsed * self.envelopes_per_second)
                .min(self.envelopes_per_second);
            state.remaining -= envelopes as f64;

            (state.remaining < 0.0)
                .then(|| Duration::from_secs_f64(-state.remaining / self.envelopes_per_second))
        };

        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn throttle(envelopes_per_second: u32) -> UnspoolThrottle {
        UnspoolThrottle::new(NonZeroU32::new(envelopes_per_second).unwrap())
    }

    #[tokio::test(start_paused = true)]
    async fn test_within_rate_does_not_wait() {
        let throttle = throttle(100);

        let start = Instant::now();
        throttle.acquire(100).await;

        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_debt_is_paid_off_by_waiting() {
        let throttle = throttle(100);

        let start = Instant::now();
        throttle.acquire(100).await;
        throttle.acquire(50).await;

        assert_eq!(start.elapsed(), Duration::from_millis(500));
    }

    #[tokio::test(start_paused = true)]
    async fn test_large_batch_is_delayed_not_starved() {
        let throttle = throttle(10);

        let start = Instant::now();
        throttle.acquire(30).await;

        assert_eq!(start.elapsed(), Duration::from_secs(2));
    }

    #[tokio::test(start_paused = true)]
    async fn test_remaining_envelopes_refill_over_time() {
        let throttle = throttle(100);
        throttle.acquire(100).await;
        tokio::time::advance(Duration::from_secs(1)).await;

        let start = Instant::now();
        throttle.acquire(100).await;

        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_refill_is_capped_at_envelopes_per_second() {
        let throttle = throttle(100);
        tokio::time::advance(Duration::from_secs(60)).await;

        let start = Instant::now();
        throttle.acquire(200).await;

        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }
}
