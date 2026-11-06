use android_trace_log::AndroidTraceLog;
use data_encoding::BASE64_NOPAD;

use crate::ProfileError;

pub mod chunk;
pub mod legacy;

// These limits were determined by observing actual counts in production
// and adding plenty of headroom:
const MAX_THREADS: usize = 10_000;
const MAX_METHODS: usize = 100_000;
const MAX_EVENTS: usize = 1_000_000;

/// Decodes an Android trace log with entity limits, recording its size by profile type.
fn parse_trace_log(sampled_profile: &str) -> Result<AndroidTraceLog, ProfileError> {
    let profile_bytes = BASE64_NOPAD
        .decode(sampled_profile.as_bytes())
        .map_err(|_| ProfileError::InvalidBase64Value)?;
    let limits = android_trace_log::Limits {
        threads: MAX_THREADS,
        methods: MAX_METHODS,
        events: MAX_EVENTS,
    };
    let profile = android_trace_log::parse_with_limits(&profile_bytes, limits)
        .map_err(|_| ProfileError::InvalidSampledProfile)?;

    Ok(profile)
}
