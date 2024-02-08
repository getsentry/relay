//! Structs and functions needed to ingest OpenTelemetry spans.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

pub use crate::span::otel_to_sentry_span;

pub use opentelemetry_proto::tonic::trace::v1 as otel_trace;

mod otel_to_sentry_tags;
mod span;
mod status_codes;
