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
extern crate url;
extern crate smith_common;

mod auth;
mod upstream;
mod projectstate;

pub use auth::*;
pub use upstream::*;
pub use projectstate::*;
