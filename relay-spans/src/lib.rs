//! Structs and functions needed to ingest OpenTelemetry spans.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

pub use crate::name::name_for_span;
pub use crate::op::derive_op_for_v2_span;
pub use crate::otel_to_sentry_v2::otel_to_sentry_span as otel_to_sentry_span_v2;
pub use crate::v1_to_v2::span_v1_to_span_v2;

pub use opentelemetry_proto::tonic::trace::v1 as otel_trace;

mod name;
mod op;
mod otel_to_sentry_v2;
mod v1_to_v2;
