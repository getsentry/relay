//! Constants shared with the C-ABI and Sentry.

// FIXME: Workaround for https://github.com/GREsau/schemars/pull/65
#![allow(clippy::field_reassign_with_default)]

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "jsonschema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The type of an event.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum EventType {
    /// Events that carry an exception payload.
    Error,
    /// A CSP violation payload.
    Csp,
    /// An HPKP violation payload.
    Hpkp,
    /// An ExpectCT violation payload.
    ExpectCT,
    /// An ExpectStaple violation payload.
    ExpectStaple,
    /// Performance monitoring transactions carrying spans.
    Transaction,
    /// All events that do not qualify as any other type.
    #[serde(other)]
    Default,
}

/// An error used when parsing `EventType`.
#[derive(Clone, Copy, Debug)]
pub struct ParseEventTypeError;

impl fmt::Display for ParseEventTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid event type")
    }
}

impl std::error::Error for ParseEventTypeError {}

impl Default for EventType {
    fn default() -> Self {
        EventType::Default
    }
}

impl FromStr for EventType {
    type Err = ParseEventTypeError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "default" => EventType::Default,
            "error" => EventType::Error,
            "csp" => EventType::Csp,
            "hpkp" => EventType::Hpkp,
            "expectct" => EventType::ExpectCT,
            "expectstaple" => EventType::ExpectStaple,
            "transaction" => EventType::Transaction,
            _ => return Err(ParseEventTypeError),
        })
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            EventType::Default => write!(f, "default"),
            EventType::Error => write!(f, "error"),
            EventType::Csp => write!(f, "csp"),
            EventType::Hpkp => write!(f, "hpkp"),
            EventType::ExpectCT => write!(f, "expectct"),
            EventType::ExpectStaple => write!(f, "expectstaple"),
            EventType::Transaction => write!(f, "transaction"),
        }
    }
}

/// Classifies the type of data that is being ingested.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[repr(i8)]
pub enum DataCategory {
    /// Reserved and unused.
    Default,
    /// Error events and Events with an `event_type` not explicitly listed below.
    Error,
    /// Transaction events.
    Transaction,
    /// Events with an event type of `csp`, `hpkp`, `expectct` and `expectstaple`.
    Security,
    /// An attachment. Quantity is the size of the attachment in bytes.
    Attachment,
    /// Session updates. Quantity is the number of updates in the batch.
    Session,
    /// Any other data category not known by this Relay.
    #[serde(other)]
    Unknown = -1,
}

impl DataCategory {
    /// Returns the data category corresponding to the given name.
    pub fn from_name(string: &str) -> Self {
        match string {
            "default" => Self::Default,
            "error" => Self::Error,
            "transaction" => Self::Transaction,
            "security" => Self::Security,
            "attachment" => Self::Attachment,
            "session" => Self::Session,
            _ => Self::Unknown,
        }
    }

    /// Returns the canonical name of this data category.
    pub fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Error => "error",
            Self::Transaction => "transaction",
            Self::Security => "security",
            Self::Attachment => "attachment",
            Self::Session => "session",
            Self::Unknown => "unknown",
        }
    }

    /// Returns true if the DataCategory refers to an error (i.e an error event).
    pub fn is_error(self) -> bool {
        matches!(self, Self::Error | Self::Default | Self::Security)
    }
}

impl fmt::Display for DataCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl FromStr for DataCategory {
    type Err = ();

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(Self::from_name(string))
    }
}

impl From<EventType> for DataCategory {
    fn from(ty: EventType) -> Self {
        match ty {
            EventType::Default | EventType::Error => Self::Error,
            EventType::Transaction => Self::Transaction,
            EventType::Csp | EventType::Hpkp | EventType::ExpectCT | EventType::ExpectStaple => {
                Self::Security
            }
        }
    }
}

/// Trace status.
///
/// Values from <https://github.com/open-telemetry/opentelemetry-specification/blob/8fb6c14e4709e75a9aaa64b0dbbdf02a6067682a/specification/api-tracing.md#status>
/// Mapping to HTTP from <https://github.com/open-telemetry/opentelemetry-specification/blob/8fb6c14e4709e75a9aaa64b0dbbdf02a6067682a/specification/data-http.md#status>
//
// Note: This type is represented as a u8 in Snuba/Clickhouse, with Unknown being the default
// value. We use repr(u8) to statically validate that the trace status has 255 variants at most.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
#[repr(u8)] // size limit in clickhouse
pub enum SpanStatus {
    /// The operation completed successfully.
    ///
    /// HTTP status 100..299 + successful redirects from the 3xx range.
    Ok = 0,

    /// The operation was cancelled (typically by the user).
    Cancelled = 1,

    /// Unknown. Any non-standard HTTP status code.
    ///
    /// "We do not know whether the transaction failed or succeeded"
    Unknown = 2,

    /// Client specified an invalid argument. 4xx.
    ///
    /// Note that this differs from FailedPrecondition. InvalidArgument indicates arguments that
    /// are problematic regardless of the state of the system.
    InvalidArgument = 3,

    /// Deadline expired before operation could complete.
    ///
    /// For operations that change the state of the system, this error may be returned even if the
    /// operation has been completed successfully.
    ///
    /// HTTP redirect loops and 504 Gateway Timeout
    DeadlineExceeded = 4,

    /// 404 Not Found. Some requested entity (file or directory) was not found.
    NotFound = 5,

    /// Already exists (409)
    ///
    /// Some entity that we attempted to create already exists.
    AlreadyExists = 6,

    /// 403 Forbidden
    ///
    /// The caller does not have permission to execute the specified operation.
    PermissionDenied = 7,

    /// 429 Too Many Requests
    ///
    /// Some resource has been exhausted, perhaps a per-user quota or perhaps the entire file
    /// system is out of space.
    ResourceExhausted = 8,

    /// Operation was rejected because the system is not in a state required for the operation's
    /// execution
    FailedPrecondition = 9,

    /// The operation was aborted, typically due to a concurrency issue.
    Aborted = 10,

    /// Operation was attempted past the valid range.
    OutOfRange = 11,

    /// 501 Not Implemented
    ///
    /// Operation is not implemented or not enabled.
    Unimplemented = 12,

    /// Other/generic 5xx.
    InternalError = 13,

    /// 503 Service Unavailable
    Unavailable = 14,

    /// Unrecoverable data loss or corruption
    DataLoss = 15,

    /// 401 Unauthorized (actually does mean unauthenticated according to RFC 7235)
    ///
    /// Prefer PermissionDenied if a user is logged in.
    Unauthenticated = 16,
}

/// Error parsing a `SpanStatus`.
#[derive(Clone, Copy, Debug)]
pub struct ParseSpanStatusError;

impl fmt::Display for ParseSpanStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid span status")
    }
}

impl std::error::Error for ParseSpanStatusError {}

impl FromStr for SpanStatus {
    type Err = ParseSpanStatusError;

    fn from_str(string: &str) -> Result<SpanStatus, Self::Err> {
        Ok(match string {
            "ok" => SpanStatus::Ok,
            "success" => SpanStatus::Ok, // Backwards compat with initial schema
            "deadline_exceeded" => SpanStatus::DeadlineExceeded,
            "unauthenticated" => SpanStatus::Unauthenticated,
            "permission_denied" => SpanStatus::PermissionDenied,
            "not_found" => SpanStatus::NotFound,
            "resource_exhausted" => SpanStatus::ResourceExhausted,
            "invalid_argument" => SpanStatus::InvalidArgument,
            "unimplemented" => SpanStatus::Unimplemented,
            "unavailable" => SpanStatus::Unavailable,
            "internal_error" => SpanStatus::InternalError,
            "failure" => SpanStatus::InternalError, // Backwards compat with initial schema
            "unknown" | "unknown_error" => SpanStatus::Unknown,
            "cancelled" => SpanStatus::Cancelled,
            "already_exists" => SpanStatus::AlreadyExists,
            "failed_precondition" => SpanStatus::FailedPrecondition,
            "aborted" => SpanStatus::Aborted,
            "out_of_range" => SpanStatus::OutOfRange,
            "data_loss" => SpanStatus::DataLoss,
            _ => return Err(ParseSpanStatusError),
        })
    }
}

impl fmt::Display for SpanStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            SpanStatus::Ok => write!(f, "ok"),
            SpanStatus::DeadlineExceeded => write!(f, "deadline_exceeded"),
            SpanStatus::Unauthenticated => write!(f, "unauthenticated"),
            SpanStatus::PermissionDenied => write!(f, "permission_denied"),
            SpanStatus::NotFound => write!(f, "not_found"),
            SpanStatus::ResourceExhausted => write!(f, "resource_exhausted"),
            SpanStatus::InvalidArgument => write!(f, "invalid_argument"),
            SpanStatus::Unimplemented => write!(f, "unimplemented"),
            SpanStatus::Unavailable => write!(f, "unavailable"),
            SpanStatus::InternalError => write!(f, "internal_error"),
            SpanStatus::Unknown => write!(f, "unknown"),
            SpanStatus::Cancelled => write!(f, "cancelled"),
            SpanStatus::AlreadyExists => write!(f, "already_exists"),
            SpanStatus::FailedPrecondition => write!(f, "failed_precondition"),
            SpanStatus::Aborted => write!(f, "aborted"),
            SpanStatus::OutOfRange => write!(f, "out_of_range"),
            SpanStatus::DataLoss => write!(f, "data_loss"),
        }
    }
}
