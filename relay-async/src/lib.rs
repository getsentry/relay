//! Relay's async combinators and utilities.
//!
//! This crate is an extension of crates like [`tokio-util`](https://docs.rs/tokio-util/) and
//! [`futures`](https://docs.rs/futures/). It contains a collection of async combinators
//! and utilities needed in Relay but not provided by other crates.
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod scheduled;

pub use self::scheduled::*;
