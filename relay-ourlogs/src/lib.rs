//! Structs and functions needed to ingest Sentry logs.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

mod otel_to_sentry;
mod size;
mod vercel_to_sentry;

pub use self::otel_to_sentry::otel_to_sentry_log;
pub use self::size::calculate_size;
pub use self::vercel_to_sentry::{VercelLog, vercel_log_to_sentry_log};

pub use opentelemetry_proto::tonic::logs::v1 as otel_logs;
