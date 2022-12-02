use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("invalid json in profile")]
    InvalidJson(#[source] serde_json::Error),
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
}
