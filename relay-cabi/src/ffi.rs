use sentry_release_parser::InvalidRelease;

use relay_auth::{KeyParseError, UnpackError};
use relay_ffi::Panic;
use relay_general::store::GeoIpError;
use relay_general::types::ProcessingAction;

use crate::core::RelayStr;

/// Represents all possible error codes.
#[repr(u32)]
#[allow(missing_docs)]
pub enum RelayErrorCode {
    NoError = 0,
    Panic = 1,
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

    // relay_general::types::annotated::ProcessingAction
    ProcessingErrorInvalidTransaction = 2001,
    ProcessingErrorInvalidGeoIp = 2002,

    // sentry_release_parser::InvalidRelease
    InvalidReleaseErrorTooLong = 3001,
    InvalidReleaseErrorRestrictedName = 3002,
    InvalidReleaseErrorBadCharacters = 3003,
}

impl RelayErrorCode {
    /// This maps all errors that can possibly happen.
    pub fn from_error(error: &failure::Error) -> RelayErrorCode {
        for cause in error.iter_chain() {
            if let Some(..) = cause.downcast_ref::<Panic>() {
                return RelayErrorCode::Panic;
            }
            if cause.downcast_ref::<serde_json::Error>().is_some() {
                return RelayErrorCode::InvalidJsonError;
            }
            if cause.downcast_ref::<GeoIpError>().is_some() {
                return RelayErrorCode::ProcessingErrorInvalidGeoIp;
            }
            if let Some(err) = cause.downcast_ref::<KeyParseError>() {
                return match err {
                    KeyParseError::BadEncoding => RelayErrorCode::KeyParseErrorBadEncoding,
                    KeyParseError::BadKey => RelayErrorCode::KeyParseErrorBadKey,
                };
            }
            if let Some(err) = cause.downcast_ref::<UnpackError>() {
                return match err {
                    UnpackError::BadSignature => RelayErrorCode::UnpackErrorBadSignature,
                    UnpackError::BadPayload(..) => RelayErrorCode::UnpackErrorBadPayload,
                    UnpackError::SignatureExpired => RelayErrorCode::UnpackErrorSignatureExpired,
                    UnpackError::BadEncoding => RelayErrorCode::UnpackErrorBadEncoding,
                };
            }
            if let Some(err) = cause.downcast_ref::<ProcessingAction>() {
                return match err {
                    ProcessingAction::InvalidTransaction(_) => {
                        RelayErrorCode::ProcessingErrorInvalidTransaction
                    }
                    _ => RelayErrorCode::Unknown,
                };
            }
            if let Some(err) = cause.downcast_ref::<InvalidRelease>() {
                return match err {
                    InvalidRelease::TooLong => RelayErrorCode::InvalidReleaseErrorTooLong,
                    InvalidRelease::RestrictedName => {
                        RelayErrorCode::InvalidReleaseErrorRestrictedName
                    }
                    InvalidRelease::BadCharacters => {
                        RelayErrorCode::InvalidReleaseErrorBadCharacters
                    }
                };
            }
        }
        RelayErrorCode::Unknown
    }
}

/// Initializes the library
#[no_mangle]
pub extern "C" fn relay_init() {
    relay_ffi::set_panic_hook();
}

/// Returns the last error code.
///
/// If there is no error, 0 is returned.
#[no_mangle]
pub extern "C" fn relay_err_get_last_code() -> RelayErrorCode {
    relay_ffi::with_last_error(|err| RelayErrorCode::from_error(err))
        .unwrap_or(RelayErrorCode::NoError)
}

/// Returns the last error message.
///
/// If there is no error an empty string is returned.  This allocates new memory
/// that needs to be freed with `relay_str_free`.
#[no_mangle]
pub extern "C" fn relay_err_get_last_message() -> RelayStr {
    use std::fmt::Write;
    relay_ffi::with_last_error(|err| {
        let mut msg = err.to_string();
        for cause in err.iter_chain().skip(1) {
            write!(&mut msg, "\n  caused by: {}", cause).ok();
        }
        RelayStr::from_string(msg)
    })
    .unwrap_or_default()
}

/// Returns the panic information as string.
#[no_mangle]
pub extern "C" fn relay_err_get_backtrace() -> RelayStr {
    let backtrace = relay_ffi::with_last_error(|error| error.backtrace().to_string())
        .filter(|bt| !bt.is_empty());

    match backtrace {
        Some(backtrace) => RelayStr::from_string(format!("stacktrace: {}", backtrace)),
        None => RelayStr::default(),
    }
}

/// Clears the last error.
#[no_mangle]
pub extern "C" fn relay_err_clear() {
    relay_ffi::take_last_error();
}
