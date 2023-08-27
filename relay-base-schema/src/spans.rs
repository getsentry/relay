//! Defines data types relating to performance spans.

use std::fmt;
use std::str::FromStr;

use relay_protocol::{Annotated, Empty, Error, FromValue, IntoValue, SkipSerialization, Value};
#[cfg(feature = "jsonschema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

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

impl SpanStatus {
    /// Returns the string representation of the status.
    pub fn as_str(&self) -> &'static str {
        match *self {
            SpanStatus::Ok => "ok",
            SpanStatus::DeadlineExceeded => "deadline_exceeded",
            SpanStatus::Unauthenticated => "unauthenticated",
            SpanStatus::PermissionDenied => "permission_denied",
            SpanStatus::NotFound => "not_found",
            SpanStatus::ResourceExhausted => "resource_exhausted",
            SpanStatus::InvalidArgument => "invalid_argument",
            SpanStatus::Unimplemented => "unimplemented",
            SpanStatus::Unavailable => "unavailable",
            SpanStatus::InternalError => "internal_error",
            SpanStatus::Unknown => "unknown",
            SpanStatus::Cancelled => "cancelled",
            SpanStatus::AlreadyExists => "already_exists",
            SpanStatus::FailedPrecondition => "failed_precondition",
            SpanStatus::Aborted => "aborted",
            SpanStatus::OutOfRange => "out_of_range",
            SpanStatus::DataLoss => "data_loss",
        }
    }
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
        f.write_str(self.as_str())
    }
}

impl Empty for SpanStatus {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for SpanStatus {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::String(value)), mut meta) => match value.parse() {
                Ok(status) => Annotated(Some(status), meta),
                Err(_) => {
                    meta.add_error(Error::expected("a trace status"));
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(Some(Value::I64(value)), mut meta) => Annotated(
                Some(match value {
                    0 => SpanStatus::Ok,
                    1 => SpanStatus::Cancelled,
                    2 => SpanStatus::Unknown,
                    3 => SpanStatus::InvalidArgument,
                    4 => SpanStatus::DeadlineExceeded,
                    5 => SpanStatus::NotFound,
                    6 => SpanStatus::AlreadyExists,
                    7 => SpanStatus::PermissionDenied,
                    8 => SpanStatus::ResourceExhausted,
                    9 => SpanStatus::FailedPrecondition,
                    10 => SpanStatus::Aborted,
                    11 => SpanStatus::OutOfRange,
                    12 => SpanStatus::Unimplemented,
                    13 => SpanStatus::InternalError,
                    14 => SpanStatus::Unavailable,
                    15 => SpanStatus::DataLoss,
                    16 => SpanStatus::Unauthenticated,
                    _ => {
                        meta.add_error(Error::expected("a trace status"));
                        meta.set_original_value(Some(value));
                        return Annotated(None, meta);
                    }
                }),
                meta,
            ),
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("a string"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

impl IntoValue for SpanStatus {
    fn into_value(self) -> Value {
        Value::String(self.to_string())
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        Serialize::serialize(self.as_str(), s)
    }
}

/// A span attribute to compute during normalization.
///
/// Relay computes certain known span data fields from spans and their children during
/// normalization. These fields are written back into span data and can be used in ingestion and
/// storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SpanAttribute {
    /// The time spent in this span without time spent in children.
    ///
    /// Sometimes also referred to as own-time, this is the execution time spent in this span
    /// without the time spent in all transitive children.
    ExclusiveTime,

    /// A field unknown to this Relay for forward compatibility.
    #[serde(other)]
    Unknown,
}
