//! Structs and functions needed to ingest OpenTelemetry spans.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

pub use crate::otel_to_sentry::otel_to_sentry_span;
pub use crate::sentry_to_otel::name_for_span;
pub use crate::v2_to_v1::span_v2_to_span_v1;

pub use opentelemetry_proto::tonic::trace::v1 as otel_trace;

mod otel_to_sentry;
mod otel_to_sentry_v2;
mod sentry_to_otel;
mod status_codes;
mod v2_to_v1;
