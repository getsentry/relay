//! Error reporting and logging facade for Relay.
//!
//! # Setup
//!
//! To enable logging, invoke the [`init`] function with [`logging`](LogConfig) and
//! [`sentry`](SentryConfig) configuration. The configuration implements `serde` traits, so it can
//! be obtained from configuration files.
//!
//! ```
//! use relay_log::{LogConfig, SentryConfig};
//!
//! let log_config = LogConfig {
//!     enable_backtraces: true,
//!     ..LogConfig::default()
//! };
//!
//! let sentry_config = SentryConfig {
//!     enabled: true,
//!     ..SentryConfig::default()
//! };
//!
//! relay_log::init(&log_config, &sentry_config);
//! ```
//!
//! # Logging
//!
//! The basic use of the log crate is through the five logging macros: [`error!`], [`warn!`],
//! [`info!`], [`debug!`] and [`trace!`] where `error!` represents the highest-priority log messages
//! and `trace!` the lowest. The log messages are filtered by configuring the log level to exclude
//! messages with a lower priority. Each of these macros accept format strings similarly to
//! [`println!`].
//!
//! ## Conventions
//!
//! Log messages should start lowercase and end without punctuation. Prefer short and precise log
//! messages over verbose text. Choose the log level according to these rules:
//!
//! - [`error!`] for bugs and invalid behavior. This will also be reported to Sentry.
//! - [`warn!`] for undesirable behavior.
//! - [`info!`] for messages relevant to the average user.
//! - [`debug!`] for messages usually relevant to debugging.
//! - [`trace!`] for full auxiliary information.
//!
//! ## Examples
//!
//! ```
//! use relay_log::{LogConfig, SentryConfig};
//!
//! relay_log::info!("startup complete");
//! ```
//!
//! # Error Reporting
//!
//! `sentry` is used for error reporting of all messages logged with an error level. To add custom
//! scope information, use [`configure_scope`] or [`with_scope`].
//!
//! ## Scopes
//!
//! ```
//! use relay_log::{LogConfig, SentryConfig};
//!
//! relay_log::with_scope(|scope| scope.set_tag("custom", "value"), || {
//!     relay_log::error!("this message has a custom tag");
//! });
//! ```
//!
//! ## Logging Error Types
//!
//! To log [`Fail`](failure::Fail) errors to both Sentry and the error stream, use the [`LogError`] wrapper. It
//! formats the error with all its causes, and ensures the format is suitable for error reporting to
//! Sentry.
//!
//! ```
//! use std::io::{Error, ErrorKind};
//! use relay_log::{LogConfig, SentryConfig, LogError};
//!
//! let custom_error = Error::new(ErrorKind::Other, "oh no!");
//! relay_log::error!("operation failed: {}", LogError(&custom_error));
//! ```
//!
//! ## Capturing without Logging
//!
//! Additionally, errors can be captured without logging.
//!
//! ```
//! use std::io::{Error, ErrorKind};
//! use relay_log::{LogConfig, SentryConfig};
//!
//! let custom_error = Error::new(ErrorKind::Other, "oh no!");
//! relay_log::capture_error(&custom_error);
//! ```
//!
//! # Testing
//!
//! For unit testing, there is a separate initialization macro [`init_test!`] that should be called
//! at the beginning of test method. It enables test mode of the logger and customizes log levels
//! for the current crate.
//!
//! ```
//! #[test]
//! fn test_something() {
//!     relay_log::init_test!();
//! }
//! ```

#![warn(missing_docs)]

mod sentry_failure;

mod setup;
pub use setup::*;

mod utils;
pub use utils::*;

// Expose the minimal log facade.
#[doc(inline)]
pub use log::{debug, error, info, log, trace, warn};

// Expose the minimal error reporting API.
#[doc(inline)]
pub use sentry::{capture_error, configure_scope, protocol, with_scope, Hub};

// Required for the temporarily vendored actix integration.
#[doc(hidden)]
pub use {sentry as _sentry, sentry_failure::exception_from_single_fail};
