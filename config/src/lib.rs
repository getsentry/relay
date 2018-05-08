//! Config system for the relay.
#![warn(missing_docs)]
extern crate chrono;
extern crate failure;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate log;
extern crate sentry;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde_yaml;
extern crate url;
extern crate url_serde;

extern crate smith_aorta;
extern crate smith_common;

mod types;

pub use types::*;
