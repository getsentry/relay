use std::ffi::CStr;
use std::mem;
use std::os::raw::c_char;
use std::ptr;
use std::slice;
use std::str;

use relay_common::Uuid;

/// A length-prefixed UTF-8 string.
///
/// As opposed to C strings, this string is not null-terminated. If the string is owned, indicated
/// by the `owned` flag, the owner must call the `free` function on this string. The convention is:
///
///  - When obtained as instance through return values, always free the string.
///  - When obtained as pointer through field access, never free the string.
#[repr(C)]
pub struct RelayStr {
    /// Pointer to the UTF-8 encoded string data.
    pub data: *mut c_char,
    /// The length of the string pointed to by `data`.
    pub len: usize,
    /// Indicates that the string is owned and must be freed.
    pub owned: bool,
}

impl RelayStr {
    /// Creates a new `RelayStr` by borrowing the given `&str`.
    pub(crate) fn new(s: &str) -> RelayStr {
        RelayStr {
            data: s.as_ptr() as *mut c_char,
            len: s.len(),
            owned: false,
        }
    }

    /// Creates a new `RelayStr` by assuming ownership over the given `String`.
    ///
    /// When dropping this `RelayStr` instance, the buffer is freed.
    pub(crate) fn from_string(mut s: String) -> RelayStr {
        s.shrink_to_fit();
        let rv = RelayStr {
            data: s.as_ptr() as *mut c_char,
            len: s.len(),
            owned: true,
        };
        mem::forget(s);
        rv
    }

    /// Frees the string buffer if it is owned.
    pub(crate) unsafe fn free(&mut self) {
        if self.owned {
            String::from_raw_parts(self.data as *mut _, self.len, self.len);
            self.data = ptr::null_mut();
            self.len = 0;
            self.owned = false;
        }
    }

    /// Returns a borrowed string.
    pub(crate) unsafe fn as_str(&self) -> &str {
        str::from_utf8_unchecked(slice::from_raw_parts(self.data as *const _, self.len))
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

/// Creates a Relay string from a c string.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_str_from_cstr(s: *const c_char) -> RelayStr {
    let s = CStr::from_ptr(s).to_str()?;
    RelayStr {
        data: s.as_ptr() as *mut _,
        len: s.len(),
        owned: false,
    }
}

/// Frees a Relay str.
///
/// If the string is marked as not owned then this function does not
/// do anything.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_str_free(s: *mut RelayStr) {
    if !s.is_null() {
        (*s).free()
    }
}

/// A 16-byte UUID.
#[repr(C)]
pub struct RelayUuid {
    /// UUID bytes in network byte order (big endian).
    pub data: [u8; 16],
}

impl RelayUuid {
    pub(crate) fn new(uuid: Uuid) -> RelayUuid {
        let data = *uuid.as_bytes();
        Self { data }
    }
}

impl From<Uuid> for RelayUuid {
    fn from(uuid: Uuid) -> RelayUuid {
        RelayUuid::new(uuid)
    }
}

/// Returns true if the uuid is nil.
#[no_mangle]
#[relay_ffi::catch_unwind]
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
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_uuid_to_str(uuid: *const RelayUuid) -> RelayStr {
    let uuid = Uuid::from_slice(&(*uuid).data[..]).unwrap_or_else(|_| Uuid::nil());
    RelayStr::from_string(uuid.to_hyphenated_ref().to_string())
}

/// A binary buffer of known length.
///
/// If the buffer is owned, indicated by the `owned` flag, the owner must call the `free` function
/// on this buffer. The convention is:
///
///  - When obtained as instance through return values, always free the buffer.
///  - When obtained as pointer through field access, never free the buffer.
#[repr(C)]
pub struct RelayBuf {
    /// Pointer to the raw data.
    pub data: *mut u8,
    /// The length of the buffer pointed to by `data`.
    pub len: usize,
    /// Indicates that the buffer is owned and must be freed.
    pub owned: bool,
}

impl RelayBuf {
    pub(crate) unsafe fn free(&mut self) {
        if self.owned {
            Vec::from_raw_parts(self.data as *mut u8, self.len, self.len);
            self.data = ptr::null_mut();
            self.len = 0;
            self.owned = false;
        }
    }

    pub(crate) unsafe fn as_bytes(&self) -> &[u8] {
        slice::from_raw_parts(self.data as *const u8, self.len)
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

/// Frees a Relay buf.
///
/// If the buffer is marked as not owned then this function does not
/// do anything.
#[no_mangle]
#[relay_ffi::catch_unwind]
pub unsafe extern "C" fn relay_buf_free(b: *mut RelayBuf) {
    if !b.is_null() {
        (*b).free()
    }
}
