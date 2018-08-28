//! Common functionality for the sentry relay.
#![warn(missing_docs)]

extern crate backoff;
extern crate base64;
extern crate cadence;
extern crate chrono;
extern crate ed25519_dalek;
extern crate failure;
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
extern crate uuid;

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

pub use marshal::processor;
pub use marshal::protocol as v8;
pub use sentry_types::{
    Auth, AuthParseError, Dsn, DsnParseError, ProjectId, ProjectIdParseError, Scheme,
};
