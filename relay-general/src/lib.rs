// Clippy throws errors in situations where a closure is clearly the better way. An example is
// `Annotated::as_str`, which can't be used directly because it's part of two impl blocks.
#![allow(clippy::redundant_closure)]
#![deny(unused_must_use)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

// we use macro_use here because we really consider this to be an internal
// macro which currently cannot be imported.
#[macro_use]
extern crate relay_general_derive;

mod macros;

#[cfg(test)]
mod testutils;

pub mod pii;
pub mod processor;
pub mod protocol;
pub mod store;
pub mod types;

pub mod user_agent;
