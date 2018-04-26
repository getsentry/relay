//! Common functionality for the sentry relay.
#![warn(missing_docs)]
extern crate failure;
extern crate hyper;
extern crate sentry_types;
extern crate serde;
extern crate serde_json;
extern crate url;

#[macro_use]
mod macros;
mod utils;

pub use sentry_types::{Auth, AuthParseError, Dsn, DsnParseError, ProjectId, ProjectIdParseError,
                       Scheme};

pub use utils::*;
