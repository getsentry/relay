//! Implements the agent <-> backend protocol.
extern crate base64;
extern crate chrono;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate futures;
extern crate hyper;
extern crate parking_lot;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
#[macro_use]
extern crate smith_common;
extern crate sodiumoxide;
extern crate tokio_core;
extern crate url;
extern crate uuid;

mod auth;
mod config;
mod upstream;
mod projectstate;

pub use auth::*;
pub use config::*;
pub use upstream::*;
pub use projectstate::*;
