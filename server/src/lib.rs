//! Implements basics for the protocol.
#![warn(missing_docs)]
extern crate actix;
extern crate actix_web;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate futures;
extern crate http;
#[macro_use]
extern crate log;
extern crate sentry;
extern crate sentry_types;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate smith_aorta;
extern crate smith_common;
extern crate smith_config;
extern crate smith_trove;
extern crate uuid;

mod service;
mod errors;
mod utils;
mod extractors;
mod middlewares;

pub use service::*;
pub use errors::*;
