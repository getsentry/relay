//! Common functionality for the sentry relay.
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

#[macro_use]
mod macros;

mod cell;
mod constants;
mod glob;
mod project;
mod retry;
mod time;
mod utils;

pub use crate::cell::*;
pub use crate::constants::*;
pub use crate::glob::*;
pub use crate::project::*;
pub use crate::retry::*;
pub use crate::time::*;
pub use crate::utils::*;

pub use sentry_types::protocol::LATEST as PROTOCOL_VERSION;
pub use sentry_types::{
    Auth, Dsn, ParseAuthError, ParseDsnError, ParseProjectIdError, Scheme, Uuid,
};
