//! Implements basics for the protocol.
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate hyper;

mod service;
mod errors;

pub use service::*;
pub use errors::*;
