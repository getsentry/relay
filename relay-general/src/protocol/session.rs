use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use chrono::{DateTime, Utc};
use failure::Fail;
use serde::{Deserialize, Serialize};

/// The type of session event we're dealing with.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Ok,
    Crashed,
    Abnormal,
    Exited,
}

/// An error used when parsing `SessionStatus`.
#[derive(Debug, Fail)]
#[fail(display = "invalid event type")]
pub struct ParseSessionStatusError;

impl FromStr for SessionStatus {
    type Err = ParseSessionStatusError;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        Ok(match string {
            "ok" => SessionStatus::Ok,
            "crashed" => SessionStatus::Crashed,
            "abnormal" => SessionStatus::Abnormal,
            "exited" => SessionStatus::Exited,
            _ => return Err(ParseSessionStatusError),
        })
    }
}

impl fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            SessionStatus::Ok => write!(f, "ok"),
            SessionStatus::Crashed => write!(f, "crashed"),
            SessionStatus::Abnormal => write!(f, "abnormal"),
            SessionStatus::Exited => write!(f, "exited"),
        }
    }
}

/// Event specific attributes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEventAttributes {
    pub os: Option<String>,
    pub os_version: Option<String>,
    pub device_family: Option<String>,
    pub release: Option<String>,
    pub environment: Option<String>,
}

fn one() -> f32 {
    1.0
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionChangeEvent {
    /// Unique identifier of this event.
    pub id: Uuid,
    /// The session identifier
    #[serde(rename = "sid")]
    pub session_id: Uuid,
    /// The distinct ID
    #[serde(rename = "did")]
    pub distinct_id: Option<Uuid>,
    /// An optional logical clock.
    pub seq: Option<u64>,
    /// The timestamp of when the session change event was created.
    #[serde(rename = "timestamp")]
    pub timestamp: DateTime<Utc>,
    /// The timestamp of when the session itself started.
    #[serde(rename = "timestamp")]
    pub started: DateTime<Utc>,
    /// The sample rate.
    #[serde(default = "one")]
    pub sample_rate: f32,
    /// An optional duration of the session so far.
    pub duration: Option<f64>,
    /// The status of the session.
    pub status: SessionStatus,
    /// The session event attributes.
    #[serde(rename = "attrs")]
    pub attributes: SessionEventAttributes,
}
