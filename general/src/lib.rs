// Clippy throws errors in situations where a closure is clearly the better way. An example is
// `Annotated::as_str`, which can't be used directly because it's part of two impl blocks.
#![allow(clippy::redundant_closure)]
#![deny(unused_must_use)]

// we use macro_use here because we really consider this to be an internal
// macro which currently cannot be imported.
#[macro_use]
extern crate relay_general_derive;

#[macro_use]
mod macros;

#[cfg(test)]
#[macro_use]
mod testutils;

pub mod datascrubbing;
pub mod filter;
pub mod pii;
pub mod processor;
pub mod protocol;
pub mod store;
pub mod types;

#[cfg(feature = "uaparser")]
mod user_agent;
