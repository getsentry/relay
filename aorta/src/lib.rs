//! Implements the agent <-> backend protocol.
extern crate base64;
extern crate chrono;
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate sodiumoxide;
extern crate uuid;

mod auth;

pub use auth::*;
