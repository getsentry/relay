//! TODO

#![warn(missing_docs)]

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

    /// # Safety
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

/// TODO: Doc
pub fn with_last_error<R, F>(f: F) -> Option<R>
where
    F: FnOnce(&dyn failure::Fail) -> R,
{
    LAST_ERROR.with(|e| e.borrow().as_ref().map(failure::Error::as_fail).map(f))
}

/// TODO: Doc
pub fn clear_error() {
    LAST_ERROR.with(|e| *e.borrow_mut() = None);
}

/// TODO: Doc
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
}

impl fmt::Display for Panic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "panic: {}", self.0)
    }
}

impl Error for Panic {}

/// TODO: Doc
pub fn set_panic_hook() {
    panic::set_hook(Box::new(|info| set_last_error(Panic::new(info).into())));
}
