use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use uuid::Uuid;

/// A really basic rate limiter to protect downstream systems from partition imbalance on
/// snuba-spans/ingest-spans topics. The rate limiter is not used to drop spans, but rather to have
/// them be randomly partitioned (instead of by trace ID) to reduce the partition imbalance again.
/// This will effectively disable the span buffer for that trace, impact segment enrichment and
/// therefore features like performance issues.
///
/// Trace IDs's span rate is measured in a single window. If the span rate ever exceeds this rate
/// (`items_per_window / window_size`), the trace ID is limited for up to a duration of
/// `window_size`.
#[derive(Clone)]
pub struct SpanSystemLimits {
    map_and_last_wipe: Arc<Mutex<(HashMap<Uuid, u64>, Option<Instant>)>>,
    window_size: Duration,
}

impl SpanSystemLimits {
    pub fn new(window_size: Duration) -> Self {
        SpanSystemLimits {
            map_and_last_wipe: Default::default(),
            window_size,
        }
    }
    #[must_use]
    pub fn try_increment(&self, key: Uuid, amount: u64, limit_per_window: u64) -> u64 {
        let mut guard = self.map_and_last_wipe.lock();
        let (ref mut map, ref mut last_wipe) = *guard;

        let now = Instant::now();

        if last_wipe.map_or(false, |last_wipe| {
            now.duration_since(last_wipe) > self.window_size
        }) {
            map.clear();
            *last_wipe = Some(now);
        }

        let value = map.entry(key).or_insert(0);
        let granted = limit_per_window.saturating_sub(*value + amount);
        *value += granted;
        granted
    }
}
