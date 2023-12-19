use relay_statsd::{metric, CounterMetric};

/// Helper to aggregate cache hits by scope before reporting them to statsd.
#[derive(Debug)]
pub struct HitCounter(hashbrown::HashMap<String, (i64, i64)>); // uses hashbrown for `entry_ref`

impl HitCounter {
    /// Creates a new [`HitCounter`] with an initial capacity for scopes.
    pub fn with_capacity(capacity: usize) -> Self {
        Self(hashbrown::HashMap::with_capacity(capacity))
    }

    /// Tracks a hit for a scope.
    pub fn hit(&mut self, scope: &str) {
        self.0.entry_ref(scope).or_default().0 += 1;
    }

    /// Tracks a miss for a scope.
    pub fn miss(&mut self, scope: &str) {
        self.0.entry_ref(scope).or_default().1 += 1;
    }

    /// Consumes all counters and reports to statsd.
    pub fn report<H, M>(self, hit: H, miss: M)
    where
        H: CounterMetric,
        M: CounterMetric,
    {
        for (scope, (hits, misses)) in self.0 {
            if hits > 0 {
                metric!(counter(hit) += hits, scope = &scope);
            }
            if misses > 0 {
                metric!(counter(miss) += misses, scope = &scope);
            }
        }
    }
}
