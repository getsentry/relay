//! Structs and functions needed to ingest OpenTelemetry logs.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

pub use opentelemetry_proto::tonic::logs::v1::LogRecord as OtelLog;

pub use crate::ourlog::otel_to_sentry_log;
pub use crate::ourlog::ourlog_merge_otel;

mod ourlog;
