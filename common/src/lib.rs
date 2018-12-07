//! Common functionality for the sentry relay.
#![warn(missing_docs)]

#[macro_use]
mod macros;

#[macro_use]
pub mod metrics;

mod auth;
mod config;
mod retry;
mod types;
mod upstream;
mod utils;

pub use crate::auth::*;
pub use crate::config::*;
pub use crate::retry::*;
pub use crate::types::*;
pub use crate::upstream::*;
pub use crate::utils::*;

pub use sentry_types::{
    Auth, AuthParseError, Dsn, DsnParseError, ProjectId, ProjectIdParseError, Scheme, Uuid,
};

// TODO: all of these should soon be directly imported

// expose the general module entirely
pub use semaphore_general::store;

/// Processing stuff
pub mod processor {
    pub use semaphore_general::pii::*;
    pub use semaphore_general::processor::*;
}

/// The v8 version of the protocol.
pub mod protocol {
    pub use semaphore_general::protocol::*;
    pub use semaphore_general::types::*;
}
