use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error(transparent)]
    InvalidJson(#[from] serde_json::Error),
    #[error("invalid base64 value")]
    InvalidBase64Value,
    #[error("invalid sampled profile")]
    InvalidSampledProfile,
    #[error("cannot serialize payload")]
    CannotSerializePayload,
    #[error("not enough samples")]
    NotEnoughSamples,
    #[error("platform not supported")]
    PlatformNotSupported,
    #[error("no transaction associated")]
    NoTransactionAssociated,
    #[error("invalid transaction metadata")]
    InvalidTransactionMetadata,
    #[error("missing profile metadata")]
    MissingProfileMetadata,
    #[error("malformed stacks")]
    MalformedStacks,
    #[error("malformed samples")]
    MalformedSamples,
    #[error("exceed size limit")]
    ExceedSizeLimit,
    #[error("too many profiles")]
    TooManyProfiles,
    #[error("duration is too long")]
    DurationIsTooLong,
}
