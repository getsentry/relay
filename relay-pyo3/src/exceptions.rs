use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use relay_auth::{KeyParseError, UnpackError};
use relay_event_normalization::GeoIpError;
use relay_event_schema::processor::ProcessingAction;
use sentry_release_parser::InvalidRelease;
use std::collections::HashMap;
use strum::IntoEnumIterator;

// Represents all possible error codes.
#[repr(u32)]
#[allow(missing_docs)]
#[pyclass(rename_all = "PascalCase")]
#[derive(Debug, Eq, PartialEq, Clone, Copy, strum::Display, strum::EnumIter)]
#[strum(serialize_all = "PascalCase")]
pub enum RelayErrorCode {
    Unknown = 2,

    InvalidJsonError = 101, // serde_json::Error

    // relay_auth::KeyParseError
    KeyParseErrorBadEncoding = 1000,
    KeyParseErrorBadKey = 1001,

    // relay_auth::UnpackError
    UnpackErrorBadSignature = 1003,
    UnpackErrorBadPayload = 1004,
    UnpackErrorSignatureExpired = 1005,
    UnpackErrorBadEncoding = 1006,

    // relay_protocol::annotated::ProcessingAction
    ProcessingErrorInvalidTransaction = 2001,
    ProcessingErrorInvalidGeoIp = 2002,

    // sentry_release_parser::InvalidRelease
    InvalidReleaseErrorTooLong = 3001,
    InvalidReleaseErrorRestrictedName = 3002,
    InvalidReleaseErrorBadCharacters = 3003,
}

impl From<&'_ anyhow::Error> for RelayErrorCode {
    /// This maps all errors that can possibly happen.
    fn from(error: &anyhow::Error) -> Self {
        for cause in error.chain() {
            if cause.downcast_ref::<serde_json::Error>().is_some() {
                return Self::InvalidJsonError;
            }
            if cause.downcast_ref::<GeoIpError>().is_some() {
                return Self::ProcessingErrorInvalidGeoIp;
            }
            if let Some(err) = cause.downcast_ref::<KeyParseError>() {
                return match err {
                    KeyParseError::BadEncoding => Self::KeyParseErrorBadEncoding,
                    KeyParseError::BadKey => Self::KeyParseErrorBadKey,
                };
            }
            if let Some(err) = cause.downcast_ref::<UnpackError>() {
                return match err {
                    UnpackError::BadSignature => Self::UnpackErrorBadSignature,
                    UnpackError::BadPayload(..) => Self::UnpackErrorBadPayload,
                    UnpackError::SignatureExpired => Self::UnpackErrorSignatureExpired,
                    UnpackError::BadEncoding => Self::UnpackErrorBadEncoding,
                };
            }
            if let Some(err) = cause.downcast_ref::<ProcessingAction>() {
                return match err {
                    ProcessingAction::InvalidTransaction(_) => {
                        Self::ProcessingErrorInvalidTransaction
                    }
                    _ => Self::Unknown,
                };
            }
            if let Some(err) = cause.downcast_ref::<InvalidRelease>() {
                return match err {
                    InvalidRelease::TooLong => Self::InvalidReleaseErrorTooLong,
                    InvalidRelease::RestrictedName => Self::InvalidReleaseErrorRestrictedName,
                    InvalidRelease::BadCharacters => Self::InvalidReleaseErrorBadCharacters,
                };
            }
        }
        Self::Unknown
    }
}

impl RelayErrorCode {
    fn exceptions_by_code() -> HashMap<u32, String> {
        Self::iter().map(|v| (v as u32, v.to_string())).collect()
    }
}

#[pyclass(extends = PyException, subclass)]
#[derive(Debug, Clone)]
pub struct RelayError {
    #[pyo3(get, set)]
    code: Option<RelayErrorCode>,
}

impl From<&'_ anyhow::Error> for RelayError {
    fn from(error: &'_ anyhow::Error) -> Self {
        Self {
            code: Some(RelayErrorCode::from(error)),
        }
    }
}

impl From<RelayError> for PyErr {
    fn from(_value: RelayError) -> Self {
        Self::new::<RelayError, ()>(())
    }
}

macro_rules! make_error {
    ($error_name:ident) => {
        #[pyclass(extends = PyException)]
        #[derive(Debug, Clone)]
        pub(crate) struct $error_name {
            #[pyo3(get, set)]
            code: Option<RelayErrorCode>,
        }

        #[pymethods]
        impl $error_name {
            #[new]
            pub(crate) fn new() -> Self {
                Self {
                    code: Some(RelayErrorCode::$error_name),
                }
            }
        }

        impl From<$error_name> for PyErr {
            fn from(_value: $error_name) -> Self {
                Self::new::<$error_name, ()>(())
            }
        }
    };
}

make_error!(Unknown);
make_error!(InvalidJsonError);
make_error!(KeyParseErrorBadEncoding);
make_error!(KeyParseErrorBadKey);
make_error!(UnpackErrorBadSignature);
make_error!(UnpackErrorBadPayload);
make_error!(UnpackErrorSignatureExpired);
make_error!(UnpackErrorBadEncoding);
make_error!(ProcessingErrorInvalidTransaction);
make_error!(ProcessingErrorInvalidGeoIp);
make_error!(InvalidReleaseErrorTooLong);
make_error!(InvalidReleaseErrorRestrictedName);
make_error!(InvalidReleaseErrorBadCharacters);

#[pymodule]
pub fn exceptions(m: &Bound<PyModule>) -> PyResult<()> {
    m.add_class::<RelayError>()?;
    m.add_class::<Unknown>()?;
    m.add_class::<InvalidJsonError>()?;
    m.add_class::<KeyParseErrorBadEncoding>()?;
    m.add_class::<KeyParseErrorBadKey>()?;
    m.add_class::<UnpackErrorBadSignature>()?;
    m.add_class::<UnpackErrorBadPayload>()?;
    m.add_class::<UnpackErrorSignatureExpired>()?;
    m.add_class::<UnpackErrorBadEncoding>()?;
    m.add_class::<ProcessingErrorInvalidTransaction>()?;
    m.add_class::<ProcessingErrorInvalidGeoIp>()?;
    m.add_class::<InvalidReleaseErrorTooLong>()?;
    m.add_class::<InvalidReleaseErrorRestrictedName>()?;
    m.add_class::<InvalidReleaseErrorBadCharacters>()?;
    m.add_class::<RelayErrorCode>()?;
    m.add("exceptions_by_code", RelayErrorCode::exceptions_by_code())?;
    Ok(())
}
