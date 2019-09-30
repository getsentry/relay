//! Utilities to deal with date-time types. (DateTime, Instant, SystemTime, etc)

use std::time::{Instant, SystemTime};

/// Converts an Instant into a SystemTime.
pub fn instant_to_system_time(instant: Instant) -> SystemTime {
    SystemTime::now() - instant.elapsed()
}

/// Creates a unix timestamp ( i.e. seconds from epoch) from an Instant
pub fn instant_to_unix_timestamp(instant: Instant) -> u64 {
    let time = instant_to_system_time(instant);
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
