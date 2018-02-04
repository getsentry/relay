//! Implements basics for the protocol.
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate smith_aorta;
extern crate smith_config;
extern crate smith_trove;

mod service;
mod errors;

pub use service::*;
pub use errors::*;
