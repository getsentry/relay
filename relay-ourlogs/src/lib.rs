//! Structs and functions needed to ingest Sentry logs.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod ourlog;
mod size;

pub use self::ourlog::otel_to_sentry_log;
pub use self::ourlog::ourlog_merge_otel;
pub use self::size::calculate_size;
pub use opentelemetry_proto::tonic::logs::v1::LogRecord as OtelLog;
