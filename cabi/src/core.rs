use std::mem;
use std::ptr;
use std::str;
use std::slice;
use std::ffi::CStr;
use std::os::raw::c_char;

use utils::{set_panic_hook, Panic, LAST_ERROR};

use uuid::Uuid;
use failure::Error;
use smith_aorta::KeyParseError;

/// Represents a uuid.
#[repr(C)]
pub struct SmithUuid {
    pub data: [u8; 16]
}

/// Represents a string.
#[repr(C)]
pub struct SmithStr {
    pub data: *mut c_char,
    pub len: usize,
    pub owned: bool,
}

/// Represents a buffer.
#[repr(C)]
pub struct SmithBuf {
    pub data: *mut u8,
    pub len: usize,
    pub owned: bool,
}

/// Represents all possible error codes
#[repr(u32)]
pub enum SmithErrorCode {
    NoError = 0,
    Panic = 1,
    Unknown = 2,
    // smith_aorta::auth::KeyParseError
    KeyParseErrorBadEncoding = 1000,
    KeyParseErrorBadKey = 1001,
}

impl SmithErrorCode {
    /// This maps all errors that can possibly happen.
    pub fn from_error(error: &Error) -> SmithErrorCode {
        for cause in error.causes() {
            if let Some(..) = cause.downcast_ref::<Panic>() {
                return SmithErrorCode::Panic;
            }
            if let Some(err) = cause.downcast_ref::<KeyParseError>() {
                return match err {
                    &KeyParseError::BadEncoding => SmithErrorCode::KeyParseErrorBadEncoding,
                    &KeyParseError::BadKey => SmithErrorCode::KeyParseErrorBadKey,
                };
            }
        }
        SmithErrorCode::Unknown
    }
}

impl Default for SmithStr {
    fn default() -> SmithStr {
        SmithStr {
            data: ptr::null_mut(),
            len: 0,
            owned: false,
        }
    }
}

impl SmithStr {
    pub fn new(s: &str) -> SmithStr {
        SmithStr {
            data: s.as_ptr() as *mut c_char,
            len: s.len(),
            owned: false,
        }
    }

    pub fn from_string(mut s: String) -> SmithStr {
        s.shrink_to_fit();
        let rv = SmithStr {
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
        unsafe {
            str::from_utf8_unchecked(slice::from_raw_parts(
                self.data as *const _, self.len))
        }
    }
}

impl SmithUuid {
    pub fn new(uuid: Uuid) -> SmithUuid {
        unsafe { mem::transmute(*uuid.as_bytes()) }
    }

    pub fn as_uuid(&self) -> &Uuid {
        unsafe {
            mem::transmute(self)
        }
    }
}

impl Default for SmithBuf {
    fn default() -> SmithBuf {
        SmithBuf {
            data: ptr::null_mut(),
            len: 0,
            owned: false,
        }
    }
}

impl SmithBuf {
    pub fn new(b: &[u8]) -> SmithBuf {
        SmithBuf {
            data: b.as_ptr() as *mut u8,
            len: b.len(),
            owned: false,
        }
    }

    pub fn from_vec(mut b: Vec<u8>) -> SmithBuf {
        b.shrink_to_fit();
        let rv = SmithBuf {
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
        unsafe {
            slice::from_raw_parts(self.data as *const u8, self.len)
        }
    }
}

/// Initializes the library
#[no_mangle]
pub unsafe extern "C" fn smith_init() {
    set_panic_hook();
}

/// Returns the last error code.
///
/// If there is no error, 0 is returned.
#[no_mangle]
pub unsafe extern "C" fn smith_err_get_last_code() -> SmithErrorCode {
    LAST_ERROR.with(|e| {
        if let Some(ref err) = *e.borrow() {
            SmithErrorCode::from_error(err)
        } else {
            SmithErrorCode::NoError
        }
    })
}

/// Returns the last error message.
///
/// If there is no error an empty string is returned.  This allocates new memory
/// that needs to be freed with `smith_str_free`.
#[no_mangle]
pub unsafe extern "C" fn smith_err_get_last_message() -> SmithStr {
    use std::fmt::Write;
    LAST_ERROR.with(|e| {
        if let Some(ref err) = *e.borrow() {
            let mut msg = err.to_string();
            for cause in err.causes().skip(1) {
                write!(&mut msg, "\n  caused by: {}", cause).ok();
            }
            SmithStr::from_string(msg)
        } else {
            Default::default()
        }
    })
}

/// Returns the panic information as string.
#[no_mangle]
pub unsafe extern "C" fn smith_err_get_backtrace() -> SmithStr {
    LAST_ERROR.with(|e| {
        if let Some(ref error) = *e.borrow() {
            let backtrace = error.backtrace().to_string();
            if !backtrace.is_empty() {
                use std::fmt::Write;
                let mut out = String::new();
                write!(&mut out, "stacktrace: {}", backtrace).ok();
                SmithStr::from_string(out)
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
pub unsafe extern "C" fn smith_err_clear() {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = None;
    });
}

ffi_fn! {
    /// Creates a smith str from a c string.
    ///
    /// This sets the string to owned.  In case it's not owned you either have
    /// to make sure you are not freeing the memory or you need to set the
    /// owned flag to false.
    unsafe fn smith_str_from_cstr(s: *const c_char) -> Result<SmithStr> {
        let s = CStr::from_ptr(s).to_str()?;
        Ok(SmithStr {
            data: s.as_ptr() as *mut _,
            len: s.len(),
            owned: true,
        })
    }
}

/// Frees a smith str.
///
/// If the string is marked as not owned then this function does not
/// do anything.
#[no_mangle]
pub unsafe extern "C" fn smith_str_free(s: *mut SmithStr) {
    if !s.is_null() {
        (*s).free()
    }
}

/// Frees a smith buf.
///
/// If the buffer is marked as not owned then this function does not
/// do anything.
#[no_mangle]
pub unsafe extern "C" fn smith_buf_free(b: *mut SmithBuf) {
    if !b.is_null() {
        (*b).free()
    }
}

/// Returns true if the uuid is nil
#[no_mangle]
pub unsafe extern "C" fn smith_uuid_is_nil(uuid: *const SmithUuid) -> bool {
    if let Ok(uuid) = Uuid::from_bytes(&(*uuid).data[..]) {
        uuid == Uuid::nil()
    } else {
        false
    }
}

/// Formats the UUID into a string.
///
/// The string is newly allocated and needs to be released with
/// `smith_cstr_free`.
#[no_mangle]
pub unsafe extern "C" fn smith_uuid_to_str(uuid: *const SmithUuid) -> SmithStr {
    let uuid =  Uuid::from_bytes(&(*uuid).data[..]).unwrap_or(Uuid::nil());
    SmithStr::from_string(uuid.hyphenated().to_string())
}
