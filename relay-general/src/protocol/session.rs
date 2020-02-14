use std::fmt;
use std::str::FromStr;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use failure::Fail;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The type of session event we're dealing with.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    /// The session is healthy.
    ///
    /// This does not necessarily indicate that the session is still active.
    Ok,
    /// The session terminated normally.
    Exited,
    /// The session resulted in an application crash.
    Crashed,
    /// The session an unexpected abrupt termination (not crashing).
    Abnormal,
}

impl Default for SessionStatus {
    fn default() -> Self {
        Self::Ok
    }
}

/// An error used when parsing `SessionStatus`.
#[derive(Debug, Fail)]
#[fail(display = "invalid session status")]
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

fn is_empty_string(opt: &Option<String>) -> bool {
    opt.as_ref().map_or(true, |s| s.is_empty())
}

/// Additional attributes for Sessions.
#[serde(default)]
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionAttributes {
    /// The operating system name, corresponding to the os context.
    pub os: Option<String>,
    /// The full operating system version string, corresponding to the os context.
    pub os_version: Option<String>,
    /// The device famility identifier, corresponding to the device context.
    pub device_family: Option<String>,
    /// The release version string.
    pub release: Option<String>,
    /// The environment identifier.
    pub environment: Option<String>,
}

impl SessionAttributes {
    fn is_empty(&self) -> bool {
        is_empty_string(&self.os)
            && is_empty_string(&self.os_version)
            && is_empty_string(&self.device_family)
            && is_empty_string(&self.release)
            && is_empty_string(&self.environment)
    }
}

fn default_sequence() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn default_sample_rate() -> f32 {
    1.0
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_default_sample_rate(rate: &f32) -> bool {
    (*rate - default_sample_rate()) < std::f32::EPSILON
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionUpdate {
    /// The session identifier.
    #[serde(rename = "sid")]
    pub session_id: Uuid,
    /// The distinct identifier.
    #[serde(rename = "did", default)]
    pub distinct_id: Uuid,
    /// An optional logical clock.
    #[serde(rename = "seq", default = "default_sequence")]
    pub sequence: u64,
    /// The timestamp of when the session change event was created.
    pub timestamp: DateTime<Utc>,
    /// The timestamp of when the session itself started.
    pub started: DateTime<Utc>,
    /// The sample rate.
    #[serde(
        default = "default_sample_rate",
        skip_serializing_if = "is_default_sample_rate"
    )]
    pub sample_rate: f32,
    /// An optional duration of the session so far.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
    /// The status of the session.
    #[serde(default)]
    pub status: SessionStatus,
    /// The session event attributes.
    #[serde(
        rename = "attrs",
        default,
        skip_serializing_if = "SessionAttributes::is_empty"
    )]
    pub attributes: SessionAttributes,
}

impl SessionUpdate {
    /// Parses a session update from JSON.
    pub fn parse(payload: &[u8]) -> Result<Self, serde_json::Error> {
        let mut session = serde_json::from_slice::<Self>(payload)?;

        if session.distinct_id.is_nil() {
            session.distinct_id = session.session_id;
        }

        Ok(session)
    }

    /// Serializes a session update back into JSON.
    pub fn serialize(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_default_values() {
        let json = r#"{
  "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
  "timestamp": "2020-02-07T15:17:00Z",
  "started": "2020-02-07T14:16:00Z"
}"#;

        let output = r#"{
  "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
  "did": "8333339f-5675-4f89-a9a0-1c935255ab58",
  "seq": 4711,
  "timestamp": "2020-02-07T15:17:00Z",
  "started": "2020-02-07T14:16:00Z",
  "status": "ok"
}"#;

        let update = SessionUpdate {
            session_id: "8333339f-5675-4f89-a9a0-1c935255ab58".parse().unwrap(),
            distinct_id: "8333339f-5675-4f89-a9a0-1c935255ab58".parse().unwrap(),
            sequence: 4711, // this would be a timestamp instead
            timestamp: "2020-02-07T15:17:00Z".parse().unwrap(),
            started: "2020-02-07T14:16:00Z".parse().unwrap(),
            sample_rate: 1.0,
            duration: None,
            status: SessionStatus::Ok,
            attributes: SessionAttributes::default(),
        };

        let mut parsed = SessionUpdate::parse(json.as_bytes()).unwrap();

        // Sequence is defaulted to the current timestamp. Override for snapshot.
        assert_eq!(parsed.sequence, default_sequence());
        parsed.sequence = 4711;

        assert_eq_dbg!(update, parsed);
        assert_eq_str!(output, serde_json::to_string_pretty(&update).unwrap());
    }

    #[test]
    fn test_session_roundtrip() {
        let json = r#"{
  "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
  "did": "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf",
  "seq": 42,
  "timestamp": "2020-02-07T15:17:00Z",
  "started": "2020-02-07T14:16:00Z",
  "sample_rate": 2.0,
  "duration": 1947.49,
  "status": "exited",
  "attrs": {
    "os": "iOS",
    "os_version": "13.3.1",
    "device_family": "iPhone12,3",
    "release": "sentry-test@1.0.0",
    "environment": "production"
  }
}"#;

        let update = SessionUpdate {
            session_id: "8333339f-5675-4f89-a9a0-1c935255ab58".parse().unwrap(),
            distinct_id: "b3ef3211-58a4-4b36-a9a1-5a55df0d9aaf".parse().unwrap(),
            sequence: 42,
            timestamp: "2020-02-07T15:17:00Z".parse().unwrap(),
            started: "2020-02-07T14:16:00Z".parse().unwrap(),
            sample_rate: 2.0,
            duration: Some(1947.49),
            status: SessionStatus::Exited,
            attributes: SessionAttributes {
                os: Some("iOS".to_owned()),
                os_version: Some("13.3.1".to_owned()),
                device_family: Some("iPhone12,3".to_owned()),
                release: Some("sentry-test@1.0.0".to_owned()),
                environment: Some("production".to_owned()),
            },
        };

        assert_eq_dbg!(update, SessionUpdate::parse(json.as_bytes()).unwrap());
        assert_eq_str!(json, serde_json::to_string_pretty(&update).unwrap());
    }
}
