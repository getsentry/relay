use android_trace_log::AndroidTraceLog;
use data_encoding::BASE64_NOPAD;

use crate::ProfileError;
use crate::statsd::ProfilingDistributions;

pub mod chunk;
pub mod legacy;

/// Decodes and parses an Android trace log, recording its size by profile type.
fn parse_trace_log(
    sampled_profile: &str,
    profile_type: &'static str,
) -> Result<AndroidTraceLog, ProfileError> {
    let profile_bytes = BASE64_NOPAD
        .decode(sampled_profile.as_bytes())
        .map_err(|_| ProfileError::InvalidBase64Value)?;
    let profile = android_trace_log::parse(&profile_bytes)
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
