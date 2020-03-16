//! Common functionality for the sentry relay.
#![warn(missing_docs)]

#[macro_use]
mod macros;

#[macro_use]
pub mod metrics;

mod cell;
mod glob;
mod log;
mod retry;
mod time;
mod utils;

pub use crate::cell::*;
pub use crate::glob::*;
pub use crate::log::*;
pub use crate::retry::*;
pub use crate::time::*;
pub use crate::utils::*;

pub use sentry_types::protocol::LATEST as PROTOCOL_VERSION;
pub use sentry_types::{
    Auth, Dsn, ParseAuthError, ParseDsnError, ParseProjectIdError, ProjectId, Scheme, Uuid,
};
