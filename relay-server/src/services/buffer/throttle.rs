use std::num::NonZeroU32;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

/// Paces how quickly something happens.
#[derive(Debug)]
pub struct Throttle {
    rate: f64, // x per second
    state: Mutex<ThrottleState>,
}

#[derive(Debug)]
struct ThrottleState {
    budget: f64,
    last_refill: Instant,
}

impl Throttle {
    /// Creates a new [`Throttle`] with the given rate.
    pub fn new(rate: NonZeroU32) -> Self {
        let rate = f64::from(rate.get());
        Self {
            rate,
            state: Mutex::new(ThrottleState {
                budget: rate,
                last_refill: Instant::now(),
            }),
        }
    }

    /// Accounts units against the throttle and waits until the rate is back within
    /// the configured limit.
    ///
    /// The units are deducted immediately and any debt is paid off by waiting.
    pub async fn acquire(&self, units: usize) {
        let wait = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);

            let now = Instant::now();
            let elapsed = now.duration_since(state.last_refill).as_secs_f64();
            state.last_refill = now;

            state.budget += elapsed * self.rate;
            state.budget = state.budget.min(self.rate);
            state.budget -= units as f64;

            (state.budget < 0.0).then(|| Duration::from_secs_f64(-state.budget / self.rate))
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
        Throttle::new(NonZeroU32::new(rate).unwrap())
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
}
