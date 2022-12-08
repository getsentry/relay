use crate::ProfileError;

pub fn discard_reason(err: ProfileError) -> &'static str {
    match err {
        ProfileError::CannotSerializePayload => "profiling_failed_serialization",
        ProfileError::InvalidTransactionMetadata => "profiling_invalid_transaction_metadata",
        ProfileError::MalformedSamples => "profiling_malformed_samples",
        ProfileError::MalformedStacks => "profiling_malformed_stacks",
        ProfileError::MissingProfileMetadata => "profiling_invalid_profile_metadata",
        ProfileError::NoTransactionAssociated => "profiling_no_transaction_associated",
        ProfileError::NotEnoughSamples => "profiling_not_enough_samples",
        ProfileError::PlatformNotSupported => "profiling_platform_not_supported",
        _ => "profiling_unknown",
    }
}
