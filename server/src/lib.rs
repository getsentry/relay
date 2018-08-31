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

extern crate semaphore_common;

#[cfg(not(windows))]
extern crate libc;

mod service;

pub use service::*;
