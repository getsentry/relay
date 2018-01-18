//! Implements the agent <-> backend protocol.
extern crate uuid;
extern crate base64;
extern crate sodiumoxide;
extern crate serde;
extern crate serde_json;
extern crate failure;
#[macro_use] extern crate failure_derive;

mod auth;


pub use auth::*;
