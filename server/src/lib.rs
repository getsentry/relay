//! Implements basics for the protocol.
extern crate hyper;
extern crate failure;
#[macro_use] extern crate failure_derive;

mod service;
mod errors;

pub use service::*;
pub use errors::*;
