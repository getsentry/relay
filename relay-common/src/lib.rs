//! Common functionality for the sentry relay.
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod macros;

pub mod glob;
pub mod glob2;
pub mod glob3;
pub mod time;

pub use sentry_types::{Auth, Dsn, ParseAuthError, ParseDsnError, Scheme};
#[doc(inline)]
pub use uuid::Uuid;
