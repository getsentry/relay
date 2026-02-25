use crate::ProfileError;

pub fn discard_reason(err: &ProfileError) -> &'static str {
    match err {
        ProfileError::CannotSerializePayload => "profiling_failed_serialization",
        ProfileError::ExceedSizeLimit => "profiling_exceed_size_limit",
        ProfileError::InvalidBase64Value => "profiling_invalid_base64_value",
        ProfileError::InvalidJson(_) => "profiling_invalid_json",
        ProfileError::InvalidSampledProfile => "profiling_invalid_sampled_profile",
        ProfileError::InvalidTransactionMetadata => "profiling_invalid_transaction_metadata",
        ProfileError::InvalidProfileType => "profiling_invalid_profile_type",
        ProfileError::MalformedSamples => "profiling_malformed_samples",
        ProfileError::MalformedStacks => "profiling_malformed_stacks",
        ProfileError::MissingProfileMetadata => "profiling_invalid_profile_metadata",
        ProfileError::NoTransactionAssociated => "profiling_no_transaction_associated",
        ProfileError::NotEnoughSamples => "profiling_not_enough_samples",
        ProfileError::PlatformNotSupported => "profiling_platform_not_supported",
        ProfileError::TooManyProfiles => "profiling_too_many_profiles",
        ProfileError::DurationIsTooLong => "profiling_duration_is_too_long",
        ProfileError::DurationIsZero => "profiling_duration_is_zero",
        ProfileError::Filtered(_) => "profiling_filtered",
        ProfileError::InvalidBuildID(_) => "invalid_build_id",
        ProfileError::InvalidProtobuf => "profiling_invalid_protobuf",
        ProfileError::NoProfileSamplesInTrace => "profiling_no_profile_samples_in_trace",
        ProfileError::MissingClockSnapshot => "profiling_missing_clock_snapshot",
    }
}
