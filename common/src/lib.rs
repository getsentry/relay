//! Common functionality for the sentry agent.
extern crate failure;
extern crate url;
#[macro_use] extern crate failure_derive;

mod auth;
mod dsn;

pub use auth::*;
pub use dsn::*;
