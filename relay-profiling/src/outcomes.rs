use crate::ProfileError;

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[allow(dead_code)]
pub enum DiscardReason {
    FailedSerialization,
    InvalidJSON,
    InvalidProfileMetadata,
    InvalidTransactionMetadata,
    MalformedSamples,
    MalformedStacks,
    NoTransactionAssociated,
    NotEnoughSamples,
    Unknown,
}

impl DiscardReason {
    pub fn name(self) -> &'static str {
        match self {
            DiscardReason::FailedSerialization => "profiling_failed_serialization",
            DiscardReason::InvalidJSON => "profiling_invalid_json",
            DiscardReason::InvalidProfileMetadata => "profiling_invalid_profile_metadata",
            DiscardReason::InvalidTransactionMetadata => "profiling_invalid_transaction_metadata",
            DiscardReason::MalformedSamples => "profiling_malformed_samples",
            DiscardReason::MalformedStacks => "profiling_malformed_stacks",
            DiscardReason::NoTransactionAssociated => "profiling_no_transaction_associated",
            DiscardReason::NotEnoughSamples => "profiling_not_enough_samples",
            DiscardReason::Unknown => "profiling_unknown",
        }
    }
}

pub fn discard_reason(err: ProfileError) -> DiscardReason {
    match err {
        ProfileError::CannotSerializePayload => DiscardReason::FailedSerialization,
        ProfileError::NotEnoughSamples => DiscardReason::NotEnoughSamples,
        ProfileError::NoTransactionAssociated => DiscardReason::NoTransactionAssociated,
        ProfileError::InvalidTransactionMetadata => DiscardReason::InvalidTransactionMetadata,
        ProfileError::MissingProfileMetadata => DiscardReason::InvalidProfileMetadata,
        ProfileError::MalformedStacks => DiscardReason::MalformedStacks,
        ProfileError::MalformedSamples => DiscardReason::MalformedSamples,
        _ => DiscardReason::Unknown,
    }
}
