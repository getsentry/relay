//! The Home of Relay's integrations with other protocols and vendors.
//!
//! Integrations are protocols and vendor APIs Relay implements. Data received by integrations must
//! be converted into a Sentry native format within the same Relay instance.
//!
//! Relay needs to temporarily store integration payloads in the protocol in an [`Envelope`](`crate::Envelope`)
//! until it can be converted by a [`Processor`](`crate::processing::Processor`).
//! Integration *must not* be transported via the [`Envelope`](crate::Envelope) network protocol.
//! See also: [`ItemType::is_internal`](`crate::envelope::ItemType::is_internal`) and
//! [`ItemType::Integration`](`crate::envelope::ItemType::Integration`).
//!
//! This module contains an exhaustive list of all integrations Relay supports.

macro_rules! define_integrations {
    (@as_content_type, $($ct:literal => $value:pat,)*) => {
        impl Integration {
            pub fn as_content_type(&self) -> &'static str {
                match self {
                    $($value => $ct),*
                }
            }
        }
    };
    (@from_content_type, $($ct:literal => $value:expr,)*) => {
        impl Integration {
            pub fn from_content_type(s: &str) -> Option<Integration> {
                match s {
                    $(s if s.eq_ignore_ascii_case($ct) => Some($value),)*
                    _ => None
                }
            }
        }
    };
    ($($tt:tt)*) => {
        define_integrations!(@as_content_type, $($tt)*);
        define_integrations!(@from_content_type, $($tt)*);
    };
}

define_integrations!(
    "application/vnd.sentry.integration.otel.logs+json" => Integration::Logs(LogsIntegration::OtelV1 { format: OtelFormat::Json }),
    "application/vnd.sentry.integration.otel.logs+protobuf" => Integration::Logs(LogsIntegration::OtelV1 { format: OtelFormat::Protobuf }),
    "application/vnd.sentry.integration.vercel.logs+json" => Integration::Logs(LogsIntegration::VercelV1 { format: VercelFormat::Json }),
);

/// An exhaustive list of all integrations supported by Relay.
///
/// While the list is currently exhaustive more integrations may be added at any time.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Integration {
    /// All logging integrations.
    Logs(LogsIntegration),
}

/// All logging integrations supported by Relay.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LogsIntegration {
    /// The OTeL logging integration.
    ///
    /// Supports OTeL's [`LogsData`](opentelemetry_proto::tonic::logs::v1::LogsData).
    OtelV1 { format: OtelFormat },
    /// The Vercel logging integration.
    ///
    /// Supports Vercel's log format as JSON arrays or NDJSON.
    VercelV1 { format: VercelFormat },
}

/// An OTeL wire format.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum OtelFormat {
    /// OTeL data in a Protocol Buffers container.
    Protobuf,
    /// OTeL data in a JSON container.
    Json,
}

/// A Vercel wire format.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum VercelFormat {
    /// Vercel data in a JSON container.
    Json,
}
