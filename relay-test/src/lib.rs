//! Helpers for testing the web server and services.
//!
//! When writing tests, keep the following points in mind:
//!
//!  - In every test, call [`setup`]. This will set up the logger so that all console output is
//!    captured by the test runner. All logs emitted with [`relay_log`] will show up for test
//!    failures or when run with `--nocapture`.
//!
//! # Example
//!
//! ```
//! #[test]
//! fn my_test() {
//!     relay_test::setup();
//!
//!     relay_log::debug!("hello, world!");
//! }
//! ```

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

/// Setup the test environment.
///
///  - Initializes logs: The logger only captures logs from this crate and mutes all other logs.
pub fn setup() {
    relay_log::init_test!();
}
