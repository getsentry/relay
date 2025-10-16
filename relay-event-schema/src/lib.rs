//! Event schema (Error, Transaction, Security) and types for event processing.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

extern crate self as relay_event_schema;

pub mod processor;
pub mod protocol;
