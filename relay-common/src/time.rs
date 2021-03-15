//! Utilities to deal with date-time types. (DateTime, Instant, SystemTime, etc)

use std::fmt;
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};

/// Converts an `Instant` into a `SystemTime`.
pub fn instant_to_system_time(instant: Instant) -> SystemTime {
    SystemTime::now() - instant.elapsed()
}

/// Converts an `Instant` into a `DateTime`.
pub fn instant_to_date_time(instant: Instant) -> chrono::DateTime<chrono::Utc> {
    instant_to_system_time(instant).into()
}

/// A unix timestap (full seconds elapsed since 1970).
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct UnixTimestamp(u64);

impl UnixTimestamp {
    /// Creates a unix timestamp from the given number of seconds.
    pub fn from_secs(secs: u64) -> Self {
        Self(secs)
    }

    /// Creates a unix timestamp from the given system time.
    pub fn from_system(time: SystemTime) -> Self {
        let duration = time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

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
        self.0
    }
}

impl fmt::Debug for UnixTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UnixTimestamp({})", self.as_secs())
    }
}

impl fmt::Display for UnixTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_secs().fmt(f)
    }
}

impl std::ops::Sub for UnixTimestamp {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        Duration::from_secs(self.0 - rhs.0)
    }
}

/// An error returned from parsing [`UnixTimestamp`].
pub struct ParseUnixTimestampError(());

impl std::str::FromStr for UnixTimestamp {
    type Err = ParseUnixTimestampError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ts = s.parse().or(Err(ParseUnixTimestampError(())))?;
        Ok(Self(ts))
    }
}

impl Serialize for UnixTimestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u64(self.as_secs())
    }
}

impl<'de> Deserialize<'de> for UnixTimestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Self::from_secs(secs))
    }
}
