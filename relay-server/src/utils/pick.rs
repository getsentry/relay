/// Returns `true` if `id` is rolled out with a rollout rate `rate`.
///
/// Deterministically makes a rollout decision for an id, usually organization id,
/// and rate.
#[cfg(any(test, feature = "processing"))]
pub fn is_rolled_out(id: u64, rate: f32) -> bool {
    ((id % 100000) as f32 / 100000.0f32) < rate
}

/// Returns `true` if the current item should be sampled.
///
/// The passed `rate` is expected to be `0 <= rate <= 1`.
pub fn sample(rate: f32) -> bool {
    (rate >= 1.0) || (rate > 0.0 && rand::random::<f32>() < rate)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_rollout() {
        assert!(!is_rolled_out(1, 0.0));
        assert!(is_rolled_out(1, 0.0001)); // Id 1 should always be rolled out
        assert!(is_rolled_out(1, 0.1));
        assert!(is_rolled_out(1, 0.9));
        assert!(is_rolled_out(1, 1.0));

        assert!(!is_rolled_out(0, 0.0));
        assert!(is_rolled_out(0, 1.0));
        assert!(!is_rolled_out(100000, 0.0));
        assert!(is_rolled_out(100000, 1.0));
        assert!(!is_rolled_out(100001, 0.0));
        assert!(is_rolled_out(100001, 1.0));

        assert!(!is_rolled_out(42, -100.0));
        assert!(is_rolled_out(42, 100.0));
    }

    #[test]
    fn test_sample() {
        assert!(sample(1.0));
        assert!(!sample(0.0));

        let mut r: i64 = 0;
        for _ in 0..10000 {
            if sample(0.5) {
                r += 1;
            } else {
                r -= 1;
            }
        }
        assert!(r.abs() < 500);
    }
}
