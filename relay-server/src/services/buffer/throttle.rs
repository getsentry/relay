use std::num::NonZeroU32;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

/// Paces how quickly something happens.
#[derive(Debug)]
pub struct Throttle {
    state: Mutex<ThrottleState>,
}

#[derive(Debug)]
struct ThrottleState {
    rate: Option<f64>,
    budget: f64,
    last_refill: Instant,
}

impl Throttle {
    /// Creates a new [`Throttle`] with the given rate.
    ///
    /// `rate` is per second. A rate of `None` disables the throttle.
    pub fn new(rate: Option<NonZeroU32>) -> Self {
        let rate = rate.map(|rate| f64::from(rate.get()));
        Self {
            state: Mutex::new(ThrottleState {
                rate,
                budget: rate.unwrap_or_default(),
                last_refill: Instant::now(),
            }),
        }
    }

    /// Updates the rate of the throttle.
    ///
    /// A rate of `None` disables the throttle.
    pub fn set_rate(&self, rate: Option<NonZeroU32>) {
        let rate = rate.map(|rate| f64::from(rate.get()));

        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.rate != rate {
            state.rate = rate;
            state.budget = rate.unwrap_or_default();
            state.last_refill = Instant::now();
        }
    }

    /// Accounts units against the throttle and waits until the rate is back within
    /// the configured limit.
    ///
    /// The units are deducted immediately and any debt is paid off by waiting.
    pub async fn acquire(&self, units: usize) {
        let wait = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);

            match state.rate {
                None => None,
                Some(rate) => {
                    let now = Instant::now();
                    let elapsed = now.duration_since(state.last_refill).as_secs_f64();
                    state.last_refill = now;

                    state.budget += elapsed * rate;
                    state.budget = state.budget.min(rate);
                    state.budget -= units as f64;

                    (state.budget < 0.0).then(|| Duration::from_secs_f64(-state.budget / rate))
                }
            }
        };

        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn throttle(rate: u32) -> Throttle {
        Throttle::new(NonZeroU32::new(rate))
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
    async fn test_budget_units_refill_over_time() {
        let throttle = throttle(100);
        throttle.acquire(100).await;
        tokio::time::advance(Duration::from_secs(1)).await;

        let start = Instant::now();
        throttle.acquire(100).await;

        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_refill_is_capped_at_rate() {
        let throttle = throttle(100);
        tokio::time::advance(Duration::from_secs(60)).await;

        let start = Instant::now();
        throttle.acquire(200).await;

        assert_eq!(start.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn test_disabled_does_not_wait() {
        let throttle = Throttle::new(None);

        let start = Instant::now();
        throttle.acquire(1_000_000).await;

        assert_eq!(start.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn test_set_rate_enables_throttling() {
        let throttle = Throttle::new(None);
        throttle.set_rate(NonZeroU32::new(100));

        let start = Instant::now();
        throttle.acquire(100).await;
        throttle.acquire(50).await;

        assert_eq!(start.elapsed(), Duration::from_millis(500));
    }

    #[tokio::test(start_paused = true)]
    async fn test_set_rate_disables_throttling() {
        let throttle = throttle(10);
        throttle.acquire(30).await;
        throttle.set_rate(None);

        let start = Instant::now();
        throttle.acquire(1_000_000).await;

        assert_eq!(start.elapsed(), Duration::ZERO);
    }
}
