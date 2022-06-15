//! Constants shared with the C-ABI and Sentry.

use std::convert::TryInto;
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "jsonschema")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The type of an event.
///
/// The event type determines how Sentry handles the event and has an impact on processing, rate
/// limiting, and quotas. There are three fundamental classes of event types:
///
///  - **Error monitoring events** (`default`, `error`): Processed and grouped into unique issues
///    based on their exception stack traces and error messages.
///  - **Security events** (`csp`, `hpkp`, `expectct`, `expectstaple`): Derived from Browser
///    security violation reports and grouped into unique issues based on the endpoint and
///    violation. SDKs do not send such events.
///  - **Transaction events** (`transaction`): Contain operation spans and collected into traces for
///    performance monitoring.
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
    ExpectCt,
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
            "expectct" => EventType::ExpectCt,
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
            EventType::ExpectCt => write!(f, "expectct"),
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
    Default = 0,
    /// Error events and Events with an `event_type` not explicitly listed below.
    Error = 1,
    /// Transaction events.
    Transaction = 2,
    /// Events with an event type of `csp`, `hpkp`, `expectct` and `expectstaple`.
    Security = 3,
    /// An attachment. Quantity is the size of the attachment in bytes.
    Attachment = 4,
    /// Session updates. Quantity is the number of updates in the batch.
    Session = 5,
    /// A profile
    Profile = 6,
    /// A transaction that was processed but not stored.
    ProcessedTransaction = 7,
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
            "profile" => Self::Profile,
            "processed_transaction" => Self::ProcessedTransaction,
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
            Self::Profile => "profile",
            Self::ProcessedTransaction => "processed_transaction",
            Self::Unknown => "unknown",
        }
    }

    /// Returns true if the DataCategory refers to an error (i.e an error event).
    pub fn is_error(self) -> bool {
        matches!(self, Self::Error | Self::Default | Self::Security)
    }

    /// Returns the numeric value for this outcome.
    pub fn value(self) -> Option<u8> {
        // negative values (Internal and Unknown) cannot be sent as
        // outcomes (internally so!)
        (self as i8).try_into().ok()
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
            EventType::Csp | EventType::Hpkp | EventType::ExpectCt | EventType::ExpectStaple => {
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

/// Time duration units used in [`MetricUnit::Duration`].
///
/// Defaults to `millisecond`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum DurationUnit {
    /// Nanosecond (`"nanosecond"`), 10^-9 seconds.
    NanoSecond,
    /// Microsecond (`"microsecond"`), 10^-6 seconds.
    MicroSecond,
    /// Millisecond (`"millisecond"`), 10^-3 seconds.
    MilliSecond,
    /// Full second (`"second"`).
    Second,
    /// Minute (`"minute"`), 60 seconds.
    Minute,
    /// Hour (`"hour"`), 3600 seconds.
    Hour,
    /// Day (`"day"`), 86,400 seconds.
    Day,
    /// Week (`"week"`), 604,800 seconds.
    Week,
}

impl Default for DurationUnit {
    fn default() -> Self {
        Self::MilliSecond
    }
}

impl fmt::Display for DurationUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NanoSecond => f.write_str("nanosecond"),
            Self::MicroSecond => f.write_str("microsecond"),
            Self::MilliSecond => f.write_str("millisecond"),
            Self::Second => f.write_str("second"),
            Self::Minute => f.write_str("minute"),
            Self::Hour => f.write_str("hour"),
            Self::Day => f.write_str("day"),
            Self::Week => f.write_str("week"),
        }
    }
}

/// An error parsing a [`MetricUnit`] or one of its variants.
#[derive(Clone, Copy, Debug)]
pub struct ParseMetricUnitError(());

/// Size of information derived from bytes, used in [`MetricUnit::Information`].
///
/// Defaults to `byte`. See also [Units of
/// information](https://en.wikipedia.org/wiki/Units_of_information).
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum InformationUnit {
    /// Bit (`"bit"`), corresponding to 1/8 of a byte.
    ///
    /// Note that there are computer systems with a different number of bits per byte.
    Bit,
    /// Byte (`"byte"`).
    Byte,
    /// Kilobyte (`"kilobyte"`), 10^3 bytes.
    KiloByte,
    /// Kibibyte (`"kibibyte"`), 2^10 bytes.
    KibiByte,
    /// Megabyte (`"megabyte"`), 10^6 bytes.
    MegaByte,
    /// Mebibyte (`"mebibyte"`), 2^20 bytes.
    MebiByte,
    /// Gigabyte (`"gigabyte"`), 10^9 bytes.
    GigaByte,
    /// Gibibyte (`"gibibyte"`), 2^30 bytes.
    GibiByte,
    /// Terabyte (`"terabyte"`), 10^12 bytes.
    TeraByte,
    /// Tebibyte (`"tebibyte"`), 2^40 bytes.
    TebiByte,
    /// Petabyte (`"petabyte"`), 10^15 bytes.
    PetaByte,
    /// Pebibyte (`"pebibyte"`), 2^50 bytes.
    PebiByte,
    /// Exabyte (`"exabyte"`), 10^18 bytes.
    ExaByte,
    /// Exbibyte (`"exbibyte"`), 2^60 bytes.
    ExbiByte,
}

impl Default for InformationUnit {
    fn default() -> Self {
        Self::Byte
    }
}

impl fmt::Display for InformationUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bit => f.write_str("bit"),
            Self::Byte => f.write_str("byte"),
            Self::KiloByte => f.write_str("kilobyte"),
            Self::KibiByte => f.write_str("kibibyte"),
            Self::MegaByte => f.write_str("megabyte"),
            Self::MebiByte => f.write_str("mebibyte"),
            Self::GigaByte => f.write_str("gigabyte"),
            Self::GibiByte => f.write_str("gibibyte"),
            Self::TeraByte => f.write_str("terabyte"),
            Self::TebiByte => f.write_str("tebibyte"),
            Self::PetaByte => f.write_str("petabyte"),
            Self::PebiByte => f.write_str("pebibyte"),
            Self::ExaByte => f.write_str("exabyte"),
            Self::ExbiByte => f.write_str("exbibyte"),
        }
    }
}

/// Units of fraction used in [`MetricUnit::Fraction`].
///
/// Defaults to `ratio`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum FractionUnit {
    /// Floating point fraction of `1`.
    Ratio,
    /// Ratio expressed as a fraction of `100`. `100%` equals a ratio of `1.0`.
    Percent,
}

impl Default for FractionUnit {
    fn default() -> Self {
        Self::Ratio
    }
}

impl fmt::Display for FractionUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ratio => f.write_str("ratio"),
            Self::Percent => f.write_str("percent"),
        }
    }
}

/// Custom user-defined units without builtin conversion.
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct CustomUnit([u8; 15]);

impl CustomUnit {
    /// Parses a `CustomUnit` from a string.
    pub fn parse(s: &str) -> Result<Self, ParseMetricUnitError> {
        if s.len() > 15 || !s.is_ascii() {
            return Err(ParseMetricUnitError(()));
        }

        let mut unit = Self(Default::default());
        unit.0.copy_from_slice(s.as_bytes());
        unit.0.make_ascii_lowercase();
        Ok(unit)
    }

    /// Returns the string representation of this unit.
    #[inline]
    pub fn as_str(&self) -> &str {
        // Safety: The string is already validated to be of length 32 and valid ASCII when
        // constructing `ProjectKey`.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl fmt::Debug for CustomUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl fmt::Display for CustomUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl std::str::FromStr for CustomUnit {
    type Err = ParseMetricUnitError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl std::ops::Deref for CustomUnit {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

/// The unit of measurement of a metric value.
///
/// Units augment metric values by giving them a magnitude and semantics. There are certain types of
/// units that are subdivided in their precision, such as the [`DurationUnit`] for time
/// measurements.
///
/// Units and their precisions are uniquely represented by a string identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum MetricUnit {
    /// A time duration, defaulting to `"millisecond"`.
    Duration(DurationUnit),
    /// Size of information derived from bytes, defaulting to `"byte"`.
    Information(InformationUnit),
    /// Fractions such as percentages, defaulting to `"ratio"`.
    Fraction(FractionUnit),
    /// user-defined units without builtin conversion or default.
    Custom(CustomUnit),
    /// Untyped value without a unit (`""`).
    None,
}

impl MetricUnit {
    /// Returns `true` if the metric_unit is [`None`].
    pub fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

impl Default for MetricUnit {
    fn default() -> Self {
        MetricUnit::None
    }
}

impl fmt::Display for MetricUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricUnit::Duration(u) => u.fmt(f),
            MetricUnit::Information(u) => u.fmt(f),
            MetricUnit::Fraction(u) => u.fmt(f),
            MetricUnit::Custom(u) => u.fmt(f),
            MetricUnit::None => f.write_str("none"),
        }
    }
}

impl std::str::FromStr for MetricUnit {
    type Err = ParseMetricUnitError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "nanosecond" | "ns" => Self::Duration(DurationUnit::NanoSecond),
            "microsecond" => Self::Duration(DurationUnit::MicroSecond),
            "millisecond" | "ms" => Self::Duration(DurationUnit::MilliSecond),
            "second" | "s" => Self::Duration(DurationUnit::Second),
            "minute" => Self::Duration(DurationUnit::Minute),
            "hour" => Self::Duration(DurationUnit::Hour),
            "day" => Self::Duration(DurationUnit::Day),
            "week" => Self::Duration(DurationUnit::Week),

            "bit" => Self::Information(InformationUnit::Bit),
            "byte" => Self::Information(InformationUnit::Byte),
            "kilobyte" => Self::Information(InformationUnit::KiloByte),
            "kibibyte" => Self::Information(InformationUnit::KibiByte),
            "megabyte" => Self::Information(InformationUnit::MegaByte),
            "mebibyte" => Self::Information(InformationUnit::MebiByte),
            "gigabyte" => Self::Information(InformationUnit::GigaByte),
            "gibibyte" => Self::Information(InformationUnit::GibiByte),
            "terabyte" => Self::Information(InformationUnit::TeraByte),
            "tebibyte" => Self::Information(InformationUnit::TebiByte),
            "petabyte" => Self::Information(InformationUnit::PetaByte),
            "pebibyte" => Self::Information(InformationUnit::PebiByte),
            "exabyte" => Self::Information(InformationUnit::ExaByte),
            "exbibyte" => Self::Information(InformationUnit::ExbiByte),

            "ratio" => Self::Fraction(FractionUnit::Ratio),
            "percent" => Self::Fraction(FractionUnit::Percent),

            "" | "none" => Self::None,
            _ => Self::Custom(CustomUnit::parse(s)?),
        })
    }
}

impl_str_serde!(MetricUnit, "a metric unit string");

#[cfg(feature = "jsonschema")]
impl schemars::JsonSchema for MetricUnit {
    fn schema_name() -> String {
        std::any::type_name::<Self>().to_owned()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        String::json_schema(gen)
    }
}
