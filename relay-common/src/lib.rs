//! Common functionality for the sentry relay.
#![warn(missing_docs)]

#[macro_use]
mod macros;

#[macro_use]
pub mod metrics;

mod auth;
mod config;
mod glob;
mod log;
mod retry;
mod types;
mod upstream;
mod utils;

pub use crate::auth::*;
pub use crate::config::*;
pub use crate::glob::*;
pub use crate::log::*;
pub use crate::retry::*;
pub use crate::types::*;
pub use crate::upstream::*;
pub use crate::utils::*;

pub use sentry_types::protocol::LATEST as PROTOCOL_VERSION;
pub use sentry_types::{Auth, AuthParseError, Dsn, DsnParseError, Scheme, Uuid};

/// Represents a project ID.
pub type ProjectId = u64;

/// Raised if a project ID cannot be parsed from a string.
pub type ProjectIdParseError = std::num::ParseIntError;
