//! Implements basics for the protocol.
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate hyper;
extern crate smith_aorta;
extern crate smith_config;

mod service;
mod errors;

pub use service::*;
pub use errors::*;
