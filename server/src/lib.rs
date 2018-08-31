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
#[cfg(feature = "with_ssl")]
extern crate native_tls;
extern crate num_cpus;
extern crate parking_lot;
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

mod constants;
mod service;

pub use service::*;
