use thiserror::Error;

#[derive(Debug, Error)]
#[allow(missing_docs)]
pub enum ProfileError {
    #[error(transparent)]
    InvalidJson(#[from] serde_path_to_error::Error<serde_json::Error>),
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

impl ProfileError {
    /// Returns the path to the field in the JSON payload at which the error occurred.
    ///
    /// This is only available for [`InvalidJson`](Self::InvalidJson). Returns `""` otherwise.
    pub fn path(self) -> String {
        match self {
            Self::InvalidJson(err) => err.path().to_string(),
            _ => "".into(),
        }
    }
}
