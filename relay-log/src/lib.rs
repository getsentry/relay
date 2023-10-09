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
//! # let startup_time = std::time::Duration::ZERO;
//! relay_log::info!(duration = ?startup_time, "startup complete");
//! ```
//!
//! # Error Reporting
//!
//! `sentry` is used for error reporting of all messages logged with an ERROR level. To add custom
//! information, add fields with the `tags.` prefix.
//!
//! ## Tags and Fields
//!
//! ```
//! relay_log::error!(
//!     tags.custom = "value",            // will become a tag in Sentry
//!     field = "value",                  // will become a context field
//!     "this message has a custom tag",
//! );
//! ```
//!
//! ## Logging Error Types
//!
//! To log [errors](std::error::Error) to both Sentry and the error stream, use [`error!`] and
//! assign a reference to the error as `error` field. This formats the error with all its sources,
//! and ensures the format is suitable for error reporting to Sentry.
//!
//! ```
//! use std::error::Error;
//! use std::io;
//!
//! let custom_error = io::Error::new(io::ErrorKind::Other, "oh no!");
//! relay_log::error!(error = &custom_error as &dyn Error, "operation failed");
//! ```
//!
//! Alternatively, higher level self-explanatory errors can be passed without an additional message.
//! They are displayed in the same way in log output, but in Sentry they will display with the error
//! type as primary title.
//!
//! ## Capturing without Logging
//!
//! Additionally, errors can be captured without logging.
//!
//! ```
//! use std::io::{Error, ErrorKind};
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
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

#[cfg(feature = "init")]
mod setup;
#[cfg(feature = "init")]
pub use setup::*;

#[cfg(feature = "dashboard")]
pub mod dashboard;

#[cfg(feature = "test")]
mod test;
#[cfg(feature = "test")]
pub use test::*;

mod utils;
#[cfg(feature = "sentry")]
pub use sentry::integrations::tower;
// Expose the minimal log facade.
#[doc(inline)]
pub use tracing::{debug, error, info, trace, warn, Level};
// Expose the minimal error reporting API.
#[doc(inline)]
pub use sentry_core::{capture_error, configure_scope, protocol, with_scope, Hub};
pub use utils::*;
