//! Configuration for the Relay CLI and server.
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod byte_size;
mod config;
mod upstream;

pub use crate::byte_size::*;
pub use crate::config::*;
pub use crate::upstream::*;
