//! Common functionality for the sentry relay.
#![warn(missing_docs)]

extern crate backoff;
extern crate base64;
extern crate cadence;
extern crate chrono;
extern crate ed25519_dalek;
extern crate failure;
extern crate semaphore_general;
extern crate human_size;
extern crate marshal;
extern crate parking_lot;
extern crate rand;
extern crate regex;
extern crate sentry_types;
extern crate serde;
extern crate serde_json;
extern crate serde_yaml;
extern crate sha2;
extern crate url;

#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate serde_derive;

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

pub use auth::*;
pub use config::*;
pub use retry::*;
pub use types::*;
pub use upstream::*;
pub use utils::*;

// compat behavior
pub use marshal::processor as processor_compat;
pub use marshal::protocol as v8_compat;

pub use sentry_types::{
    Auth, AuthParseError, Dsn, DsnParseError, ProjectId, ProjectIdParseError, Scheme, Uuid,
};

// expose the general module entirely
pub use semaphore_general::{processor, store};

/// The v8 version of the protocol.
pub mod protocol {
    pub use semaphore_general::protocol::*;
    pub use semaphore_general::types::*;
}
