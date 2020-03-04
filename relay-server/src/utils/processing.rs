//! Utilities to deal with date-time types. (DateTime, Instant, SystemTime, etc)

use std::time::{Duration, Instant, SystemTime};

/// Converts an Instant into a SystemTime.
pub fn instant_to_system_time(instant: Instant) -> SystemTime {
    SystemTime::now() - instant.elapsed()
}

/// A unix timestap (time elapsed since 1970).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct UnixTimestamp(Duration);

impl UnixTimestamp {
    /// Creates a unix timestamp from the given number of seconds.
    pub fn from_secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs))
    }

    /// Creates a unix timestamp from the given system time.
    pub fn from_system(time: SystemTime) -> Self {
        let duration = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        Self(duration)
    }

    /// Converts the given `Instant` into a UNIX timestamp.
    ///
    /// This is done by comparing the `Instant` with the current system time. Note that the system
    /// time is subject to skew, so subsequent calls to `from_instant` may return different values.
    #[inline]
    pub fn from_instant(instant: Instant) -> Self {
        Self::from_system(instant_to_system_time(instant))
    }

    /// Returns the current timestamp.
    #[inline]
    pub fn now() -> Self {
        Self::from_system(SystemTime::now())
    }

    /// Returns the number of seconds since the UNIX epoch start.
    pub fn as_secs(self) -> u64 {
        self.0.as_secs()
    }
}

impl std::ops::Sub for UnixTimestamp {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        self.0 - rhs.0
    }
}
