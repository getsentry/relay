use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct Debounced {
    /// Time of last activation in seconds.
    last_activation: AtomicU64,
    /// Debounce interval in seconds.
    interval: u64,
    /// Relative instant used for measurements.
    instant: Instant,
}

impl Debounced {
    pub fn new(interval: u64) -> Self {
        Self {
            last_activation: AtomicU64::new(0),
            interval,
            instant: Instant::now(),
        }
    }

    pub fn debounce(&self, now: Instant, f: impl FnOnce()) -> bool {
        // Add interval to make sure it always triggers immediately.
        let now = now.duration_since(self.instant).as_secs() + self.interval;

        let prev = self.last_activation.load(Ordering::Relaxed);
        if now.saturating_sub(prev) < self.interval {
            return false;
        }

        if self
            .last_activation
            .compare_exchange(prev, now, Ordering::SeqCst, Ordering::Acquire)
            .is_ok()
        {
            f();
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_debounce() {
        let now = Instant::now();
        let d = Debounced::new(1);

        assert!(d.debounce(now, || {}));
        for _ in 0..10 {
            assert!(!d.debounce(now, || {}));
        }

        assert!(d.debounce(now + Duration::from_secs(2), || {}));
        for _ in 0..10 {
            assert!(!d.debounce(now + Duration::from_secs(2), || {}));
        }
    }
}
