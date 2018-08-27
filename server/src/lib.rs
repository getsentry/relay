//! Implements basics for the protocol.
#![warn(missing_docs)]

extern crate actix;
extern crate actix_web;
extern crate base64;
extern crate bytes;
extern crate chrono;
extern crate failure;
extern crate flate2;
extern crate futures;
extern crate listenfd;
extern crate num_cpus;
#[cfg(feature = "with_ssl")]
extern crate openssl;
extern crate sentry;
extern crate sentry_actix;
extern crate serde;
extern crate serde_json;
extern crate url;
extern crate uuid;

#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate log;
#[macro_use]
extern crate semaphore_common;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate lazy_static;

#[cfg(not(windows))]
extern crate libc;

mod actors;
mod constants;
mod endpoints;
mod errors;
mod extractors;
mod middlewares;
mod service;
mod utils;

pub use errors::*;
pub use service::*;
