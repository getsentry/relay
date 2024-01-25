use std::fmt;

use relay_common::time::UnixTimestamp;
use serde::{Deserialize, Serialize};

/// A sliding window.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlidingWindow {
    /// The number of seconds to apply the limit to.
    pub window_seconds: u64,
    /// A number between 1 and `window_seconds`. Since `window_seconds` is a
    /// sliding window, configure what the granularity of that window is.
    ///
    /// If this is equal to `window_seconds`, the quota resets to 0 every
    /// `window_seconds`.  If this is a very small number, the window slides
    /// "more smoothly" at the expense of having much more redis keys.
    ///
    /// The number of redis keys required to enforce a quota is `window_seconds /
    /// granularity_seconds`.
    pub granularity_seconds: u64,
}

impl SlidingWindow {
    /// Iterate over the quota's window, yielding values representing each
    /// (absolute) granule.
    ///
    /// This function is used to calculate keys for storing the number of
    /// requests made in each granule.
    ///
    /// The iteration is done in reverse-order (newest timestamp to oldest),
    /// starting with the key to which a currently-processed request should be
    /// added. That request's timestamp is `request_timestamp`.
    ///
    /// * `request_timestamp / self.granularity_seconds - 1`
    /// * `request_timestamp / self.granularity_seconds - 2`
    /// * `request_timestamp / self.granularity_seconds - 3`
    /// * ...
    pub fn iter(&self, timestamp: UnixTimestamp) -> impl Iterator<Item = Slot> {
        let value = timestamp.as_secs() / self.granularity_seconds;
        (0..self.window_seconds / self.granularity_seconds)
            .map(move |i| value.saturating_sub(i + 1))
            .map(Slot)
    }

    /// The active bucket is the oldest active granule.
    pub fn active_slot(&self, timestamp: UnixTimestamp) -> Slot {
        self.iter(timestamp).last().unwrap_or(Slot(0))
    }
}

/// A single slot from a [`SlidingWindow`].
#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Slot(u64);

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_sliding_window() {
        let window = SlidingWindow {
            window_seconds: 3600,
            granularity_seconds: 720,
        };

        let timestamp = UnixTimestamp::from_secs(1701775000);
        let r = window.iter(timestamp).collect::<Vec<_>>();
        assert_eq!(
            r.len() as u64,
            window.window_seconds / window.granularity_seconds
        );
        assert_eq!(
            r,
            vec![
                Slot(2363575),
                Slot(2363574),
                Slot(2363573),
                Slot(2363572),
                Slot(2363571)
            ]
        );
        assert_eq!(window.active_slot(timestamp), *r.last().unwrap());

        let r2 = window
            .iter(timestamp + Duration::from_secs(10))
            .collect::<Vec<_>>();
        assert_eq!(r2, r);

        let r3 = window
            .iter(timestamp + Duration::from_secs(window.granularity_seconds))
            .collect::<Vec<_>>();
        assert_ne!(r3, r);
    }
}
