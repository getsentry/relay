//! Defines types related to Sentry events.
//!
//! As opposed to the event protocol defined in `relay-event-schema`, these types are meant to be
//! used outside of the event protocol, for instance to reference Events from other places.

use std::fmt;
use std::str::FromStr;

use relay_protocol::{Annotated, Empty, ErrorKind, FromValue, IntoValue, SkipSerialization, Value};
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
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize, Default,
)]
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
    /// User feedback payload.
    UserFeedback,
    /// All events that do not qualify as any other type.
    #[serde(other)]
    #[default]
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
            "user_report_v2" => EventType::UserFeedback,
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
            EventType::UserFeedback => write!(f, "user_report_v2"),
        }
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
        Value::String(format!("{self}"))
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        Serialize::serialize(&self.to_string(), s)
    }
}
