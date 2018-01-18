//! Implements the agent <-> backend protocol.
extern crate base64;
extern crate sodiumoxide;
extern crate failure;
#[macro_use] extern crate failure_derive;

mod auth;


pub use auth::*;
