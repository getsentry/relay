use android_trace_log::AndroidTraceLog;
use data_encoding::BASE64_NOPAD;

use crate::ProfileError;
use crate::statsd::ProfilingDistributions;

pub mod chunk;
pub mod legacy;

const MAX_THREADS: usize = 10_000; // TBD
const MAX_METHODS: usize = 100_000; // TBD
const MAX_EVENTS: usize = 1_000_000; // TBD

/// Decodes an Android trace log with entity limits, recording its size by profile type.
fn parse_trace_log(
    sampled_profile: &str,
    profile_type: &'static str,
) -> Result<AndroidTraceLog, ProfileError> {
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

    relay_statsd::metric!(
        distribution(ProfilingDistributions::AndroidTraceLogEvents) = profile.events.len() as u64,
        type = profile_type,
    );
    relay_statsd::metric!(
        distribution(ProfilingDistributions::AndroidTraceLogMethods) = profile.methods.len() as u64,
        type = profile_type,
    );
    relay_statsd::metric!(
        distribution(ProfilingDistributions::AndroidTraceLogThreads) = profile.threads.len() as u64,
        type = profile_type,
    );

    Ok(profile)
}
