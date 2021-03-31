//! Helpers for testing the web server and services.
//!
//! When writing tests, keep the following points in mind:
//!
//!  - In every test, call [`setup`]. This will set up the logger so that all console output is
//!    captured by the test runner. All logs emitted with [`relay_log`] will show up for test
//!    failures or when run with `--nocapture`.
//!
//!  - Wrap all code that spawns futures into [`block_fn`]. This ensures the actix system is both
//!    registered and running.
//!
//! ```
//! relay_test::setup();
//!
//! // Access to the actix System is possible outside of `block_fn`.
//! let system = actix::System::current();
//!
//! relay_test::block_fn(|| {
//!     // `actix::spawn` can be called in here.
//!     Ok::<(), ()>(())
//! });
//! ```

use std::{
    cell::RefCell,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use actix::{clock, System, SystemRunner};
use futures::{future, IntoFuture};

pub use actix_web::test::*;
use tokio_timer::Delay;

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
///  - Initializes logs: The logger only captures logs from this crate and mutes all other logs.
///  - Initializes an actix actor system and registers it as current. This allows to call
///    `System::current`.
pub fn setup() {
    relay_log::init_test!();

    // Force initialization of the actix system
    SYSTEM.with(|_sys| ());
}

/// Runs the provided function, blocking the current thread until the result future completes.
///
/// This function can be used to synchronously block the current thread until the provided `future`
/// has resolved either successfully or with an error. The result of the future is then returned
/// from this function call.
///
/// This is provided rather than a `block_on`-like interface to avoid accidentally calling a
/// function which spawns before creating a future, which would attempt to spawn before actix is
/// initialised.
///
/// Note that this function is intended to be used only for testing purpose.
///
/// # Panics
///
/// This function panics on nested call.
pub fn block_fn<F, R>(future: F) -> Result<R::Item, R::Error>
where
    F: FnOnce() -> R,
    R: IntoFuture,
{
    SYSTEM.with(|cell| {
        let mut inner = cell.borrow_mut();
        inner.set_current();
        inner.runner().block_on(future::lazy(future))
    })
}

/// Returns a future which completes after the requested delay.
/// ```
/// use std::time::Duration;
/// use futures::future::Future;
///
/// relay_test::setup();
/// relay_test::block_fn(|| {
///     relay_test::delay(Duration::from_millis(1000)).and_then(|_| {
///         println!("One second has passed.");
///         Ok(())
///     })
/// });
/// ```
pub fn delay(timeout: Duration) -> Delay {
    Delay::new(clock::now() + timeout)
}

/// Provides a clock which has to be driven manually. See [`clock::Now`].
/// ```
/// use actix::prelude::*;
/// use futures::future::Future;
/// use std::time::Duration;
///
/// #[test]
/// fn test_clock() {
///     let mut test_now = TestNow::new();
///     let test_clock = clock::Clock::new_with_now(test_now.clone());
///     let sys = System::builder().clock(test_clock);
///     sys.run(move || {
///         actix::spawn(delay(Duration::from_millis(1000)).then(|_| {
///             println!("This will never happen unless you advance the clock.");
///             System::current().stop();
///             Ok(())
///         }));
///         test_now.advance(Duration::from_millis(1100));
///     });
/// }
/// ```
#[derive(Clone)]
pub struct TestNow {
    current: Arc<RwLock<Instant>>,
}

impl TestNow {
    pub fn new() -> Self {
        TestNow {
            current: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub fn advance(&mut self, duration: Duration) {
        *self.current.write().unwrap() += duration;
    }
}

impl Default for TestNow {
    fn default() -> Self {
        Self::new()
    }
}

impl actix::clock::Now for TestNow {
    fn now(&self) -> std::time::Instant {
        *self.current.read().unwrap()
    }
}
