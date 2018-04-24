//! Common functionality for the sentry relay.
#![warn(missing_docs)]
extern crate failure;
#[macro_use]
extern crate failure_derive;
extern crate hyper;
extern crate sentry_types;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate url;

#[macro_use]
mod macros;
mod utils;

pub use sentry_types::{Auth, AuthParseError, Dsn, DsnParseError, ProjectId, ProjectIdParseError,
                       Scheme};

pub use utils::*;
