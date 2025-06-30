/// Result of a sampling operation, whether to keep or discard something.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum PickResult {
    /// The item should be kept.
    #[default]
    Keep,
    /// The item should be dropped.
    Discard,
}

/// Returns [`PickResult::Keep`] if `id` is rolled out with a rollout rate `rate`.
///
/// Deterministically makes a rollout decision for an id, usually organization id,
/// and rate.
pub fn is_rolled_out(id: u64, rate: f32) -> PickResult {
    match ((id % 100000) as f32 / 100000.0f32) < rate {
        true => PickResult::Keep,
        false => PickResult::Discard,
    }
}

impl PickResult {
    /// Returns `true` if the sampling result is [`PickResult::Keep`].
    pub fn is_keep(self) -> bool {
        matches!(self, PickResult::Keep)
    }

    /// Returns `true` if the sampling result is [`PickResult::Discard`].
    #[cfg(feature = "processing")]
    pub fn is_discard(self) -> bool {
        !self.is_keep()
    }
}

/// Returns [`PickResult::Keep`] if the current item should be sampled.
///
/// The passed `rate` is expected to be `0 <= rate <= 1`.
pub fn sample(rate: f32) -> PickResult {
    match (rate >= 1.0) || (rate > 0.0 && rand::random::<f32>() < rate) {
        true => PickResult::Keep,
        false => PickResult::Discard,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_rollout() {
        assert_eq!(is_rolled_out(1, 0.0), PickResult::Discard);
        assert_eq!(is_rolled_out(1, 0.0001), PickResult::Keep); // Id 1 should always be rolled out
        assert_eq!(is_rolled_out(1, 0.1), PickResult::Keep);
        assert_eq!(is_rolled_out(1, 0.9), PickResult::Keep);
        assert_eq!(is_rolled_out(1, 1.0), PickResult::Keep);

        assert_eq!(is_rolled_out(0, 0.0), PickResult::Discard);
        assert_eq!(is_rolled_out(0, 1.0), PickResult::Keep);
        assert_eq!(is_rolled_out(100000, 0.0), PickResult::Discard);
        assert_eq!(is_rolled_out(100000, 1.0), PickResult::Keep);
        assert_eq!(is_rolled_out(100001, 0.0), PickResult::Discard);
        assert_eq!(is_rolled_out(100001, 1.0), PickResult::Keep);

        assert_eq!(is_rolled_out(42, -100.0), PickResult::Discard);
        assert_eq!(is_rolled_out(42, 100.0), PickResult::Keep);
    }

    #[test]
    fn test_sample() {
        assert_eq!(sample(1.0), PickResult::Keep);
        assert_eq!(sample(0.0), PickResult::Discard);

        let mut r: i64 = 0;
        for _ in 0..10000 {
            if sample(0.5).is_keep() {
                r += 1;
            } else {
                r -= 1;
            }
        }
        assert!(r.abs() < 500);
    }
}
