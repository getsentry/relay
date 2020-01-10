use std::ffi::CStr;
use std::mem;
use std::os::raw::c_char;
use std::ptr;
use std::slice;
use std::str;

use failure::Error;
use relay_common::{KeyParseError, UnpackError, Uuid};
use relay_general::types::ProcessingAction;

use crate::utils::{set_panic_hook, Panic, LAST_ERROR};

/// Represents a uuid.
#[repr(C)]
pub struct RelayUuid {
    pub data: [u8; 16],
}

/// Represents a string.
#[repr(C)]
pub struct RelayStr {
    pub data: *mut c_char,
    pub len: usize,
    pub owned: bool,
}

/// Represents a buffer.
#[repr(C)]
pub struct RelayBuf {
    pub data: *mut u8,
    pub len: usize,
    pub owned: bool,
}

/// Represents all possible error codes
#[repr(u32)]
pub enum RelayErrorCode {
    NoError = 0,
    Panic = 1,
    Unknown = 2,

    // relay_common::auth::KeyParseError
    KeyParseErrorBadEncoding = 1000,
    KeyParseErrorBadKey = 1001,

    // relay_common::auth::UnpackError
    UnpackErrorBadSignature = 1003,
    UnpackErrorBadPayload = 1004,
    UnpackErrorSignatureExpired = 1005,

    // relay_general::types::annotated::ProcessingAction
    ProcessingActionInvalidTransaction = 2000,
}

impl RelayErrorCode {
    /// This maps all errors that can possibly happen.
    pub fn from_error(error: &Error) -> RelayErrorCode {
        for cause in error.iter_chain() {
            if let Some(..) = cause.downcast_ref::<Panic>() {
                return RelayErrorCode::Panic;
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
                };
            }
            if let Some(err) = cause.downcast_ref::<ProcessingAction>() {
                return match err {
                    ProcessingAction::InvalidTransaction(_) => {
                        RelayErrorCode::ProcessingActionInvalidTransaction
                    }
                    _ => RelayErrorCode::Unknown,
                };
            }
        }
        RelayErrorCode::Unknown
    }
}

// RelayStr is immutable, thus it can be Send + Sync

unsafe impl Sync for RelayStr {}
unsafe impl Send for RelayStr {}

impl Default for RelayStr {
    fn default() -> RelayStr {
        RelayStr {
            data: ptr::null_mut(),
            len: 0,
            owned: false,
        }
    }
}

impl From<String> for RelayStr {
    fn from(string: String) -> RelayStr {
        RelayStr::from_string(string)
    }
}

impl<'a> From<&'a str> for RelayStr {
    fn from(string: &str) -> RelayStr {
        RelayStr::new(string)
    }
}

impl RelayStr {
    pub fn new(s: &str) -> RelayStr {
        RelayStr {
            data: s.as_ptr() as *mut c_char,
            len: s.len(),
            owned: false,
        }
    }

    pub fn from_string(mut s: String) -> RelayStr {
        s.shrink_to_fit();
        let rv = RelayStr {
            data: s.as_ptr() as *mut c_char,
            len: s.len(),
            owned: true,
        };
        mem::forget(s);
        rv
    }

    pub unsafe fn free(&mut self) {
        if self.owned {
            String::from_raw_parts(self.data as *mut _, self.len, self.len);
            self.data = ptr::null_mut();
            self.len = 0;
            self.owned = false;
        }
    }

    pub fn as_str(&self) -> &str {
        unsafe { str::from_utf8_unchecked(slice::from_raw_parts(self.data as *const _, self.len)) }
    }
}

impl RelayUuid {
    pub fn new(uuid: Uuid) -> RelayUuid {
        unsafe { mem::transmute(*uuid.as_bytes()) }
    }

    pub fn as_uuid(&self) -> &Uuid {
        unsafe { &*(self as *const RelayUuid as *const Uuid) }
    }
}

impl Default for RelayBuf {
    fn default() -> RelayBuf {
        RelayBuf {
            data: ptr::null_mut(),
            len: 0,
            owned: false,
        }
    }
}

impl From<Uuid> for RelayUuid {
    fn from(uuid: Uuid) -> RelayUuid {
        RelayUuid::new(uuid)
    }
}

impl RelayBuf {
    pub fn new(b: &[u8]) -> RelayBuf {
        RelayBuf {
            data: b.as_ptr() as *mut u8,
            len: b.len(),
            owned: false,
        }
    }

    pub fn from_vec(mut b: Vec<u8>) -> RelayBuf {
        b.shrink_to_fit();
        let rv = RelayBuf {
            data: b.as_ptr() as *mut u8,
            len: b.len(),
            owned: true,
        };
        mem::forget(b);
        rv
    }

    pub unsafe fn free(&mut self) {
        if self.owned {
            Vec::from_raw_parts(self.data as *mut u8, self.len, self.len);
            self.data = ptr::null_mut();
            self.len = 0;
            self.owned = false;
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.data as *const u8, self.len) }
    }
}

/// Initializes the library
#[no_mangle]
pub unsafe extern "C" fn relay_init() {
    set_panic_hook();
}

/// Returns the last error code.
///
/// If there is no error, 0 is returned.
#[no_mangle]
pub unsafe extern "C" fn relay_err_get_last_code() -> RelayErrorCode {
    LAST_ERROR.with(|e| {
        if let Some(ref err) = *e.borrow() {
            RelayErrorCode::from_error(err)
        } else {
            RelayErrorCode::NoError
        }
    })
}

/// Returns the last error message.
///
/// If there is no error an empty string is returned.  This allocates new memory
/// that needs to be freed with `relay_str_free`.
#[no_mangle]
pub unsafe extern "C" fn relay_err_get_last_message() -> RelayStr {
    use std::fmt::Write;
    LAST_ERROR.with(|e| {
        if let Some(ref err) = *e.borrow() {
            let mut msg = err.to_string();
            for cause in err.iter_chain().skip(1) {
                write!(&mut msg, "\n  caused by: {}", cause).ok();
            }
            RelayStr::from_string(msg)
        } else {
            RelayStr::default()
        }
    })
}

/// Returns the panic information as string.
#[no_mangle]
pub unsafe extern "C" fn relay_err_get_backtrace() -> RelayStr {
    LAST_ERROR.with(|e| {
        if let Some(ref error) = *e.borrow() {
            let backtrace = error.backtrace().to_string();
            if !backtrace.is_empty() {
                use std::fmt::Write;
                let mut out = String::new();
                write!(&mut out, "stacktrace: {}", backtrace).ok();
                RelayStr::from_string(out)
            } else {
                RelayStr::default()
            }
        } else {
            RelayStr::default()
        }
    })
}

/// Clears the last error.
#[no_mangle]
pub unsafe extern "C" fn relay_err_clear() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

ffi_fn! {
    /// Creates a Relay str from a c string.
    ///
    /// This sets the string to owned.  In case it's not owned you either have
    /// to make sure you are not freeing the memory or you need to set the
    /// owned flag to false.
    unsafe fn relay_str_from_cstr(s: *const c_char) -> Result<RelayStr> {
        let s = CStr::from_ptr(s).to_str()?;
        Ok(RelayStr {
            data: s.as_ptr() as *mut _,
            len: s.len(),
            owned: true,
        })
    }
}

/// Frees a Relay str.
///
/// If the string is marked as not owned then this function does not
/// do anything.
#[no_mangle]
pub unsafe extern "C" fn relay_str_free(s: *mut RelayStr) {
    if !s.is_null() {
        (*s).free()
    }
}

/// Frees a Relay buf.
///
/// If the buffer is marked as not owned then this function does not
/// do anything.
#[no_mangle]
pub unsafe extern "C" fn relay_buf_free(b: *mut RelayBuf) {
    if !b.is_null() {
        (*b).free()
    }
}

/// Returns true if the uuid is nil
#[no_mangle]
pub unsafe extern "C" fn relay_uuid_is_nil(uuid: *const RelayUuid) -> bool {
    if let Ok(uuid) = Uuid::from_slice(&(*uuid).data[..]) {
        uuid == Uuid::nil()
    } else {
        false
    }
}

/// Formats the UUID into a string.
///
/// The string is newly allocated and needs to be released with
/// `relay_cstr_free`.
#[no_mangle]
pub unsafe extern "C" fn relay_uuid_to_str(uuid: *const RelayUuid) -> RelayStr {
    let uuid = Uuid::from_slice(&(*uuid).data[..]).unwrap_or_else(|_| Uuid::nil());
    RelayStr::from_string(uuid.to_hyphenated_ref().to_string())
}
