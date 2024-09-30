//! Defines types related to Sentry events.
//!
//! As opposed to the event protocol defined in `relay-event-schema`, these types are meant to be
//! used outside of the event protocol, for instance to reference Events from other places.

use std::fmt;
use std::str::FromStr;

use relay_protocol::{Annotated, Empty, ErrorKind, FromValue, IntoValue, SkipSerialization, Value};
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
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize, Default,
)]
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
    /// Network Error Logging report.
    Nel,
    /// Performance monitoring transactions carrying spans.
    Transaction,
    /// User feedback payload.
    ///
    /// TODO(Jferg): Change this to UserFeedback once old UserReport logic is deprecated.
    UserReportV2,
    /// All events that do not qualify as any other type.
    #[serde(other)]
    #[default]
    Default,
}

impl EventType {
    /// Returns the string representation of this event type.
    pub fn as_str(&self) -> &'static str {
        match self {
            EventType::Default => "default",
            EventType::Error => "error",
            EventType::Csp => "csp",
            EventType::Hpkp => "hpkp",
            EventType::ExpectCt => "expectct",
            EventType::ExpectStaple => "expectstaple",
            EventType::Nel => "nel",
            EventType::Transaction => "transaction",
            EventType::UserReportV2 => "feedback",
        }
    }
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
            "nel" => EventType::Nel,
            "transaction" => EventType::Transaction,
            "feedback" => EventType::UserReportV2,
            _ => return Err(ParseEventTypeError),
        })
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Empty for EventType {
    #[inline]
    fn is_empty(&self) -> bool {
        false
    }
}

impl FromValue for EventType {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), mut meta) => match value.parse() {
                Ok(eventtype) => Annotated(Some(eventtype), meta),
                Err(_) => {
                    meta.add_error(ErrorKind::InvalidData);
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for EventType {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
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
