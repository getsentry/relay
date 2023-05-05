//! Common functionality for the sentry relay.
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod macros;

mod constants;
mod glob;
mod project;
mod time;

pub use sentry_types::protocol::LATEST as PROTOCOL_VERSION;
pub use sentry_types::{Auth, Dsn, ParseAuthError, ParseDsnError, Scheme};
#[doc(inline)]
pub use uuid::Uuid;

pub use crate::constants::*;
pub use crate::glob::*;
pub use crate::macros::*;
pub use crate::project::*;
pub use crate::time::*;

/// Validates a metric name. This is the statsd name, i.e. without type or unit.
///
/// Metric names cannot be empty, must begin with a letter and can consist of ASCII alphanumerics,
/// underscores, slashes and periods.
pub fn is_valid_metric_name(name: &str) -> bool {
    let mut iter = name.as_bytes().iter();
    if let Some(first_byte) = iter.next() {
        if first_byte.is_ascii_alphabetic() {
            return iter.all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'/'));
        }
    }
    false
}
