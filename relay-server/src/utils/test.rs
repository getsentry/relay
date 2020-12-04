//! Helpers for testing the web server and service.
//!
//! When writing tests, keep the following points in mind:
//!
//!  - In every test, call `test::setup`. This will set up the logger so that all console output is
//!    captured by the test runner.
//!
//! Note: This is copied (with some functionality removed) from the file with the same name in
//! Symbolicator

use std::cell::RefCell;

use actix::{System, SystemRunner};
use futures::{future, IntoFuture};

pub use actix_web::test::*;

thread_local! {
    static SYSTEM: RefCell<Inner> = RefCell::new(Inner::new());
}

struct Inner {
    system: Option<System>,
    runner: Option<SystemRunner>,
}

impl Inner {
    fn new() -> Self {
        let runner = System::new("relay tests");
        let system = System::current();

        Self {
            system: Some(system),
            runner: Some(runner),
        }
    }

    fn set_current(&self) {
        System::set_current(self.system.clone().unwrap());
    }

    fn runner(&mut self) -> &mut SystemRunner {
        self.runner.as_mut().unwrap()
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.system.take();
        std::mem::forget(self.runner.take().unwrap())
    }
}

/// Setup the test environment.
///
///  - Initializes logs: The logger only captures logs from this crate and mutes all
///    other logs.
///  - Switches threadpools into test mode: In this mode, threadpools do not actually spawn threads,
///    but instead return the futures that are spawned. This allows to capture console output logged
///    from spawned tasks.
pub(crate) fn setup() {
    relay_log::init_test!();

    // Force initialization of the actix system
    SYSTEM.with(|_sys| ());
}

/// Runs the provided function, blocking the current thread until the result
/// future completes.
///
/// This function can be used to synchronously block the current thread
/// until the provided `future` has resolved either successfully or with an
/// error. The result of the future is then returned from this function
/// call.
///
/// This is provided rather than a `block_on()`-like interface to avoid accidentally calling
/// a function which spawns before creating a future, which would attempt to spawn before
/// actix is initialised.
///
/// Note that this function is intended to be used only for testing purpose.
/// This function panics on nested call.
pub fn block_fn<F, R>(f: F) -> Result<R::Item, R::Error>
where
    F: FnOnce() -> R,
    R: IntoFuture,
{
    SYSTEM.with(|cell| {
        let mut inner = cell.borrow_mut();
        inner.set_current();
        inner.runner().block_on(future::lazy(f))
    })
}
