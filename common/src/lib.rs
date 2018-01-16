//! Common functionality for the sentry agent.
extern crate failure;
#[macro_use] extern crate failure_derive;

mod auth;

pub use auth::*;
