use std::cmp;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hashbrown::HashMap;
use parking_lot::RwLock;

use crate::Key;
use crate::debounced::Debounced;

/// A really basic rate limiter to protect downstream systems from partition imbalance on ingestion
/// topics.
///
/// The rate limiter is not used to drop messages, but rather to have them be randomly partitioned
/// (instead of by trace ID) to reduce the partition imbalance again. If the topic is semantically
/// partitioned this almost certainly will have product impact.
///
/// A partition key's message rate is measured in a single window. If the span rate ever exceeds this rate
/// (`limit_per_window / window_size`), the partition key is limited for up to a duration of
/// `window_size`.
pub struct KafkaRateLimits {
    limit_per_window: u64,
    map: RwLock<HashMap<Key, AtomicU64>>,
    wipe_debouncer: Debounced,
    window_size: Duration,
}

impl KafkaRateLimits {
    pub fn new(limit_per_window: u64, window_size: Duration) -> Self {
        KafkaRateLimits {
            map: Default::default(),
            wipe_debouncer: Debounced::new(window_size.as_secs()),
            limit_per_window,
            window_size,
        }
    }

    #[must_use]
    pub fn try_increment(&self, now: Instant, key: Key, amount: u64) -> u64 {
        self.wipe_debouncer.debounce(now, || {
            self.map.write().clear();
        });

        let increment = |value: &AtomicU64| {
            let actual_count = value.fetch_add(amount, Ordering::Relaxed);
            let headroom = self.limit_per_window.saturating_sub(actual_count);
            cmp::min(amount, headroom)
        };

        // Fast path, check if we already have a stored bucket.
        if let Some(value) = self.map.read().get(&key) {
            return increment(value);
        }

        // Slow path, get an exclusive lock and create the bucket if it wasn't already created in
        // the meantime.
        let mut map_w = self.map.write();
        increment(map_w.entry(key).or_default())
    }
}

impl fmt::Debug for KafkaRateLimits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaRateLimits")
            .field("limit_per_window", &self.limit_per_window)
            .field("window_size", &self.window_size)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limits() {
        let limiter = KafkaRateLimits::new(10, Duration::from_secs(10));
        let now = Instant::now();
        let key = 42;

        for i in 1..=5 {
            assert_eq!(
                limiter.try_increment(now + Duration::from_secs(i), key, 2),
                2
            );
        }

        for i in 6..=10 {
            assert_eq!(
                limiter.try_increment(now + Duration::from_secs(i), key, 2),
                0
            );
        }

        assert_eq!(
            limiter.try_increment(now + Duration::from_secs(11), key, 2),
            2
        );
    }
}
