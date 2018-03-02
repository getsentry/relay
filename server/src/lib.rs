//! Implements basics for the protocol.
extern crate futures;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate smith_aorta;
extern crate smith_config;
extern crate smith_trove;
extern crate ctrlc;
extern crate parking_lot;

mod service;
mod errors;

pub use service::*;
pub use errors::*;
