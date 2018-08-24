//! Config system for the relay.
#![warn(missing_docs)]

#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;

extern crate chrono;
extern crate failure;
extern crate sentry;
extern crate serde;
extern crate serde_json;
extern crate serde_yaml;
extern crate url;

#[macro_use]
extern crate semaphore_common;

mod types;
mod upstream;

pub use types::*;
pub use upstream::*;
