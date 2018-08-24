use std::ffi::CStr;
use std::mem;
use std::os::raw::c_char;
use std::ptr;
use std::slice;
use std::str;

use failure::Error;
use semaphore_common::{KeyParseError, UnpackError};
use uuid::Uuid;

use utils::{set_panic_hook, Panic, LAST_ERROR};

/// Represents a uuid.
#[repr(C)]
pub struct SemaphoreUuid {
    pub data: [u8; 16],
}

/// Represents a string.
#[repr(C)]
pub struct SemaphoreStr {
    pub data: *mut c_char,
    pub len: usize,
    pub owned: bool,
}

/// Represents a buffer.
#[repr(C)]
pub struct SemaphoreBuf {
    pub data: *mut u8,
    pub len: usize,
    pub owned: bool,
}

/// Represents all possible error codes
#[repr(u32)]
pub enum SemaphoreErrorCode {
    NoError = 0,
    Panic = 1,
    Unknown = 2,

    // semaphore_common::auth::KeyParseError
    KeyParseErrorBadEncoding = 1000,
    KeyParseErrorBadKey = 1001,

    // semaphore_common::auth::UnpackError
    UnpackErrorBadSignature = 1003,
    UnpackErrorBadPayload = 1004,
    UnpackErrorSignatureExpired = 1005,
}

impl SemaphoreErrorCode {
    /// This maps all errors that can possibly happen.
    pub fn from_error(error: &Error) -> SemaphoreErrorCode {
        for cause in error.iter_chain() {
            if let Some(..) = cause.downcast_ref::<Panic>() {
                return SemaphoreErrorCode::Panic;
            }
            if let Some(err) = cause.downcast_ref::<KeyParseError>() {
                return match err {
                    KeyParseError::BadEncoding => SemaphoreErrorCode::KeyParseErrorBadEncoding,
                    KeyParseError::BadKey => SemaphoreErrorCode::KeyParseErrorBadKey,
                };
            }
            if let Some(err) = cause.downcast_ref::<UnpackError>() {
                return match err {
                    UnpackError::BadSignature => SemaphoreErrorCode::UnpackErrorBadSignature,
                    UnpackError::BadPayload(..) => SemaphoreErrorCode::UnpackErrorBadPayload,
                    UnpackError::SignatureExpired => {
                        SemaphoreErrorCode::UnpackErrorSignatureExpired
                    }
                };
            }
        }
        SemaphoreErrorCode::Unknown
    }
}

impl Default for SemaphoreStr {
    fn default() -> SemaphoreStr {
        SemaphoreStr {
            data: ptr::null_mut(),
            len: 0,
            owned: false,
        }
    }
}

impl SemaphoreStr {
    pub fn new(s: &str) -> SemaphoreStr {
        SemaphoreStr {
            data: s.as_ptr() as *mut c_char,
            len: s.len(),
            owned: false,
        }
    }

    pub fn from_string(mut s: String) -> SemaphoreStr {
        s.shrink_to_fit();
        let rv = SemaphoreStr {
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

impl SemaphoreUuid {
    pub fn new(uuid: Uuid) -> SemaphoreUuid {
        unsafe { mem::transmute(*uuid.as_bytes()) }
    }

    pub fn as_uuid(&self) -> &Uuid {
        unsafe { mem::transmute(self) }
    }
}

impl Default for SemaphoreBuf {
    fn default() -> SemaphoreBuf {
        SemaphoreBuf {
            data: ptr::null_mut(),
            len: 0,
            owned: false,
        }
    }
}

impl SemaphoreBuf {
    pub fn new(b: &[u8]) -> SemaphoreBuf {
        SemaphoreBuf {
            data: b.as_ptr() as *mut u8,
            len: b.len(),
            owned: false,
        }
    }

    pub fn from_vec(mut b: Vec<u8>) -> SemaphoreBuf {
        b.shrink_to_fit();
        let rv = SemaphoreBuf {
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
pub unsafe extern "C" fn semaphore_init() {
    set_panic_hook();
}

/// Returns the last error code.
///
/// If there is no error, 0 is returned.
#[no_mangle]
pub unsafe extern "C" fn semaphore_err_get_last_code() -> SemaphoreErrorCode {
    LAST_ERROR.with(|e| {
        if let Some(ref err) = *e.borrow() {
            SemaphoreErrorCode::from_error(err)
        } else {
            SemaphoreErrorCode::NoError
        }
    })
}

/// Returns the last error message.
///
/// If there is no error an empty string is returned.  This allocates new memory
/// that needs to be freed with `semaphore_str_free`.
#[no_mangle]
pub unsafe extern "C" fn semaphore_err_get_last_message() -> SemaphoreStr {
    use std::fmt::Write;
    LAST_ERROR.with(|e| {
        if let Some(ref err) = *e.borrow() {
            let mut msg = err.to_string();
            for cause in err.iter_chain().skip(1) {
                write!(&mut msg, "\n  caused by: {}", cause).ok();
            }
            SemaphoreStr::from_string(msg)
        } else {
            Default::default()
        }
    })
}

/// Returns the panic information as string.
#[no_mangle]
pub unsafe extern "C" fn semaphore_err_get_backtrace() -> SemaphoreStr {
    LAST_ERROR.with(|e| {
        if let Some(ref error) = *e.borrow() {
            let backtrace = error.backtrace().to_string();
            if !backtrace.is_empty() {
                use std::fmt::Write;
                let mut out = String::new();
                write!(&mut out, "stacktrace: {}", backtrace).ok();
                SemaphoreStr::from_string(out)
            } else {
                Default::default()
            }
        } else {
            Default::default()
        }
    })
}

/// Clears the last error.
#[no_mangle]
pub unsafe extern "C" fn semaphore_err_clear() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

ffi_fn! {
    /// Creates a semaphore str from a c string.
    ///
    /// This sets the string to owned.  In case it's not owned you either have
    /// to make sure you are not freeing the memory or you need to set the
    /// owned flag to false.
    unsafe fn semaphore_str_from_cstr(s: *const c_char) -> Result<SemaphoreStr> {
        let s = CStr::from_ptr(s).to_str()?;
        Ok(SemaphoreStr {
            data: s.as_ptr() as *mut _,
            len: s.len(),
            owned: true,
        })
    }
}

/// Frees a semaphore str.
///
/// If the string is marked as not owned then this function does not
/// do anything.
#[no_mangle]
pub unsafe extern "C" fn semaphore_str_free(s: *mut SemaphoreStr) {
    if !s.is_null() {
        (*s).free()
    }
}

/// Frees a semaphore buf.
///
/// If the buffer is marked as not owned then this function does not
/// do anything.
#[no_mangle]
pub unsafe extern "C" fn semaphore_buf_free(b: *mut SemaphoreBuf) {
    if !b.is_null() {
        (*b).free()
    }
}

/// Returns true if the uuid is nil
#[no_mangle]
pub unsafe extern "C" fn semaphore_uuid_is_nil(uuid: *const SemaphoreUuid) -> bool {
    if let Ok(uuid) = Uuid::from_bytes(&(*uuid).data[..]) {
        uuid == Uuid::nil()
    } else {
        false
    }
}

/// Formats the UUID into a string.
///
/// The string is newly allocated and needs to be released with
/// `semaphore_cstr_free`.
#[no_mangle]
pub unsafe extern "C" fn semaphore_uuid_to_str(uuid: *const SemaphoreUuid) -> SemaphoreStr {
    let uuid = Uuid::from_bytes(&(*uuid).data[..]).unwrap_or(Uuid::nil());
    SemaphoreStr::from_string(uuid.hyphenated().to_string())
}
