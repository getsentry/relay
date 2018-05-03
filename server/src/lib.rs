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
extern crate mime;
#[cfg(feature = "with_ssl")]
extern crate openssl;
extern crate sentry;
extern crate sentry_types;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate smith_aorta;
extern crate smith_common;
extern crate smith_config;
extern crate smith_trove;
extern crate url;
extern crate uuid;

mod errors;
mod extractors;
mod middlewares;
mod service;
mod endpoints;
mod constants;

pub use errors::*;
pub use service::*;
