//! Scrubbing of personally identifiable information (PII) from events

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod attachments;
mod builtin;
mod compiledconfig;
mod config;
mod convert;
mod generate_selectors;
mod legacy;
mod minidumps;
mod processor;
mod redactions;
mod regexes;
mod selector;
mod utils;

pub use self::attachments::*;
pub use self::compiledconfig::*;
pub use self::config::*;
pub use self::generate_selectors::selector_suggestions_from_value;
pub use self::legacy::*;
pub use self::minidumps::*;
pub use self::processor::*;
pub use self::redactions::*;
pub use self::selector::*;
