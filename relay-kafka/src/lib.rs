//! Kafka-related functionality.
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod config;
#[cfg(feature = "producer")]
mod producer;

pub use config::*;
#[cfg(feature = "producer")]
pub use producer::*;
