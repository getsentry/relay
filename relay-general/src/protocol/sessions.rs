use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use chrono::{DateTime, Utc};
use failure::Fail;
use serde::{Deserialize, Serialize};

/// The type of session event we're dealing with.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEventOperation {
    Start,
    #[serde(rename = "fg")]
    Foreground,
    #[serde(rename = "bg")]
    Background,
    Crash,
    Close,
}

/// An error used when parsing `SessionEventOperation`.
#[derive(Debug, Fail)]
#[fail(display = "invalid event type")]
pub struct ParseSessionEventTypeError;

impl FromStr for SessionEventOperation {
    type Err = ParseSessionEventTypeError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "start" => SessionEventOperation::Start,
            "fg" => SessionEventOperation::Foreground,
            "bg" => SessionEventOperation::Background,
            "crash" => SessionEventOperation::Crash,
            "close" => SessionEventOperation::Close,
            _ => return Err(ParseSessionEventTypeError),
        })
    }
}

impl fmt::Display for SessionEventOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            SessionEventOperation::Start => write!(f, "start"),
            SessionEventOperation::Foreground => write!(f, "fg"),
            SessionEventOperation::Background => write!(f, "bg"),
            SessionEventOperation::Crash => write!(f, "crash"),
            SessionEventOperation::Close => write!(f, "close"),
        }
    }
}

/// Event specific attributes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEventAttributes {
    pub os: Option<String>,
    pub api_level: Option<String>,
    pub release: Option<String>,
    pub environment: Option<String>,
    pub device_family: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEvent {
    /// Unique identifier of this event.
    pub id: Uuid,
    /// The timestamp of the session event.
    #[serde(rename = "ts")]
    pub timestamp: DateTime<Utc>,
    /// The session operation.
    pub op: SessionEventOperation,
    /// The session event attributes.
    #[serde(rename = "attrs")]
    pub attributes: SessionEventAttributes,
}
