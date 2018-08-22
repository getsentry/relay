//! This library manages project config updates and various async requests to the upstream relay.
#![warn(missing_docs)]
#![cfg_attr(feature = "cargo-clippy", allow(needless_pass_by_value))]

extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate log;
extern crate native_tls;
extern crate parking_lot;
extern crate semaphore_aorta;
extern crate semaphore_common;
extern crate serde;
extern crate serde_json;
extern crate tokio_core;

mod types;

pub use types::*;
