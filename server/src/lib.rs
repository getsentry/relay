//! Implements basics for the protocol.
#![warn(missing_docs)]

extern crate actix;
extern crate actix_web;
extern crate base64;
extern crate bytes;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate flate2;
extern crate futures;
extern crate http;
extern crate itertools;
#[macro_use]
extern crate log;
extern crate mime;
#[cfg(feature = "with_ssl")]
extern crate openssl;
extern crate semaphore_aorta;
#[macro_use]
extern crate semaphore_common;
extern crate semaphore_config;
extern crate sentry;
extern crate sentry_actix;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate listenfd;
extern crate parking_lot;
extern crate serde_json;
extern crate url;
extern crate uuid;

#[cfg(not(windows))]
extern crate libc;

mod actors;
mod body;
mod constants;
mod endpoints;
mod errors;
mod extractors;
mod middlewares;
mod service;
mod utils;

pub use errors::*;
pub use service::*;
