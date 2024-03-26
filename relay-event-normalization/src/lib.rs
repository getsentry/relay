//! Event normalization and processing.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod clock_drift;
mod event;
mod event_error;
mod geo;
mod legacy;
mod logentry;
mod mechanism;
mod normalize;
mod regexes;
mod remove_other;
mod schema;
mod stacktrace;
mod statsd;
mod timestamp;
mod transactions;
mod trimming;
mod validation;

pub use validation::{
    validate_event_timestamps, validate_span, validate_transaction, EventValidationConfig,
    TransactionValidationConfig,
};
pub mod replay;
pub use event::{
    normalize_event, normalize_measurements, normalize_performance_score, NormalizationConfig,
};
pub use normalize::breakdowns::*;
pub use normalize::*;
pub use remove_other::RemoveOtherProcessor;
pub use schema::SchemaProcessor;
pub use timestamp::TimestampProcessor;
pub use transactions::*;
pub use trimming::TrimmingProcessor;
pub use user_agent::*;

pub use self::clock_drift::*;
pub use self::geo::*;

pub use sentry_release_parser::{validate_environment, validate_release};
