//! Utilities for error handling in FFI bindings.
//!
//! This crate facilitates an [`errno`]-like error handling pattern: On success, the result of a
//! function call is returned. On error, a thread-local marker is set that allows to retrieve the
//! error, message, and a backtrace if available.
//!
//! # Catch Errors and Panics
//!
//! The [`catch_unwind`] attribute annotates functions that can internally throw errors. It allows
//! the use of the questionmark operator `?` in a function that does not return `Result`. The error
//! is then available using [`with_last_error`]:
//!
//! ```
//! use relay_ffi::catch_unwind;
//!
//! #[catch_unwind]
//! unsafe fn parse_number() -> i32 {
//!     // use the questionmark operator for errors:
//!     let number: i32 = "42".parse()?;
//!
//!     // return the value directly, not `Ok`:
//!     number * 2
//! }
//! ```
//!
//! # Safety
//!
//! Since function calls always need to return a value, this crate has to return
//! `std::mem::zeroed()` as a placeholder in case of an error. This is unsafe for reference types
//! and function pointers. Because of this, functions must be marked `unsafe`.
//!
//! In most cases, FFI functions should return either `repr(C)` structs or pointers, in which case
//! this is safe in principle. The author of the API is responsible for defining the contract,
//! however, and document the behavior of custom structures in case of an error.
//!
//! # Examples
//!
//! Annotate FFI functions with [`catch_unwind`] to capture errors. The error can be inspected via
//! [`with_last_error`]:
//!
//! ```
//! use relay_ffi::{catch_unwind, with_last_error};
//!
//! #[catch_unwind]
//! unsafe fn parse_number() -> i32 {
//!     "42".parse()?
//! }
//!
//! let parsed = unsafe { parse_number() };
//! match with_last_error(|e| e.to_string()) {
//!     Some(error) => println!("errored with: {}", error),
//!     None => println!("result: {}", parsed),
//! }
//! ```
//!
//! To capture panics, register the panic hook early during library initialization:
//!
//! ```
//! use relay_ffi::{catch_unwind, with_last_error};
//!
//! relay_ffi::set_panic_hook();
//!
//! #[catch_unwind]
//! unsafe fn fail() {
//!     panic!("expected panic");
//! }
//!
//! unsafe { fail() };
//!
//! if let Some(description) = with_last_error(|e| e.to_string()) {
//!     println!("{}", description);
//! }
//! ```
//!
//! # Creating C-APIs
//!
//! This is an example for exposing an API to C:
//!
//! ```
//! use std::ffi::CString;
//! use std::os::raw::c_char;
//!
//! #[no_mangle]
//! pub unsafe extern "C" fn init_ffi() {
//!     relay_ffi::set_panic_hook();
//! }
//!
//! #[no_mangle]
//! pub unsafe extern "C" fn last_strerror() -> *mut c_char {
//!     let ptr_opt = relay_ffi::with_last_error(|err| {
//!         CString::new(err.to_string())
//!             .unwrap_or_default()
//!             .into_raw()
//!     });
//!
//!     ptr_opt.unwrap_or(std::ptr::null_mut())
//! }
//! ```
//!
//! [`errno`]: https://man7.org/linux/man-pages/man3/errno.3.html

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::panic;
use std::thread;

pub use relay_ffi_macros::catch_unwind;

thread_local! {
    static LAST_ERROR: RefCell<Option<failure::Error>> = RefCell::new(None);
}

fn set_last_error(err: failure::Error) {
    LAST_ERROR.with(|e| {
        *e.borrow_mut() = Some(err);
    });
}

#[doc(hidden)]
pub mod __internal {
    use super::*;

    /// Catches down panics and errors from the given closure.
    ///
    /// Returns the result of the passed function on success. On error or panic, returns
    /// zero-initialized memory and sets the thread-local error.
    ///
    /// # Safety
    ///
    /// Returns `std::mem::zeroed` on error, which is unsafe for reference types and function
    /// pointers.
    #[inline]
    pub unsafe fn catch_errors<F, T>(f: F) -> T
    where
        F: FnOnce() -> Result<T, failure::Error> + panic::UnwindSafe,
    {
        match panic::catch_unwind(f) {
            Ok(Ok(result)) => result,
            Ok(Err(err)) => {
                set_last_error(err);
                std::mem::zeroed()
            }
            Err(_) => std::mem::zeroed(),
        }
    }
}

/// Acquires a reference to the last error and passes it to the callback, if any.
///
/// Returns `Some(R)` if there was an error, otherwise `None`. The error resets when it is taken
/// with [`take_last_error`].
///
/// # Example
///
/// ```
/// use relay_ffi::{catch_unwind, with_last_error};
///
/// #[catch_unwind]
/// unsafe fn run_ffi() -> i32 {
///     "invalid".parse()?
/// }
///
/// let parsed = unsafe { run_ffi() };
/// match with_last_error(|e| e.to_string()) {
///     Some(error) => println!("errored with: {}", error),
///     None => println!("result: {}", parsed),
/// }
/// ```
pub fn with_last_error<R, F>(f: F) -> Option<R>
where
    F: FnOnce(&failure::Error) -> R,
{
    LAST_ERROR.with(|e| e.borrow().as_ref().map(f))
}

/// Takes the last error, leaving `None` in its place.
///
/// To inspect the error without removing it, use [`with_last_error`].
///
/// # Example
///
/// ```
/// use relay_ffi::{catch_unwind, take_last_error};
///
/// #[catch_unwind]
/// unsafe fn run_ffi() -> i32 {
///     "invalid".parse()?
/// }
///
/// let parsed = unsafe { run_ffi() };
/// match take_last_error() {
///     Some(error) => println!("errored with: {}", error),
///     None => println!("result: {}", parsed),
/// }
/// ```
pub fn take_last_error() -> Option<failure::Error> {
    LAST_ERROR.with(|e| e.borrow_mut().take())
}

/// An error representing a panic carrying the message as payload.
///
/// To capture panics, register the hook using [`set_panic_hook`].
///
/// # Example
///
/// ```
/// use relay_ffi::{catch_unwind, with_last_error, Panic};
///
/// #[catch_unwind]
/// unsafe fn panics() {
///     panic!("this is fine");
/// }
///
/// relay_ffi::set_panic_hook();
///
/// unsafe { panics() };
///
/// with_last_error(|error| {
///     if let Some(panic) = error.downcast_ref::<Panic>() {
///         println!("{}", panic.description());
///     }
/// });
/// ```
#[derive(Debug)]
pub struct Panic(String);

impl Panic {
    fn new(info: &panic::PanicInfo) -> Self {
        let thread = thread::current();
        let thread = thread.name().unwrap_or("unnamed");

        let message = match info.payload().downcast_ref::<&str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &**s,
                None => "Box<Any>",
            },
        };

        let description = match info.location() {
            Some(location) => format!(
                "thread '{}' panicked with '{}' at {}:{}",
                thread,
                message,
                location.file(),
                location.line()
            ),
            None => format!("thread '{}' panicked with '{}'", thread, message),
        };

        Self(description)
    }

    /// Returns a description containing the location and message of the panic.
    #[inline]
    pub fn description(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "panic: {}", self.description())
    }
}

impl Error for Panic {}

/// Registers a hook for capturing panics with backtraces.
///
/// This function must be registered early when the FFI is initialized before any other calls are
/// made. Usually, this would be exported from an initialization function.
///
/// See the [`Panic`] documentation for more information.
///
/// # Example
///
/// ```
/// pub unsafe extern "C" fn init_ffi() {
///     relay_ffi::set_panic_hook();
/// }
/// ```
pub fn set_panic_hook() {
    panic::set_hook(Box::new(|info| set_last_error(Panic::new(info).into())));
}
