use std::fmt;
use std::net::IpAddr;
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
    /// The session had an unexpected abrupt termination (not crashing).
    Abnormal,
    /// The session is degraded (errors happened but it did not crash)
    Degraded,
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
            "degraded" => SessionStatus::Degraded,
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
            SessionStatus::Degraded => write!(f, "degraded"),
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
    /// The release version string.
    pub release: Option<String>,
    /// The environment identifier.
    pub environment: Option<String>,
    /// The ip address of the user.
    pub ip_address: Option<IpAddr>,
    /// The user agent of the user.
    pub user_agent: Option<String>,
}

impl SessionAttributes {
    fn is_empty(&self) -> bool {
        is_empty_string(&self.release)
            && is_empty_string(&self.environment)
            && self.ip_address.is_none()
            && is_empty_string(&self.user_agent)
    }
}

fn default_sequence() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(val: &bool) -> bool {
    !val
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionUpdate {
    /// The session identifier.
    #[serde(rename = "sid", default = "Uuid::new_v4")]
    pub session_id: Uuid,
    /// The distinct identifier.
    #[serde(rename = "did", default)]
    pub distinct_id: Option<String>,
    /// An optional logical clock.
    #[serde(rename = "seq", default = "default_sequence")]
    pub sequence: u64,
    /// A flag that indicates that this is the initial transmission of the session.
    #[serde(default, skip_serializing_if = "is_false")]
    pub init: bool,
    /// The timestamp of when the session change event was created.
    #[serde(default = "Utc::now")]
    pub timestamp: DateTime<Utc>,
    /// The timestamp of when the session itself started.
    pub started: DateTime<Utc>,
    /// An optional duration of the session so far.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
    /// The status of the session.
    #[serde(default)]
    pub status: SessionStatus,
    /// The number of errors that ocurred.
    #[serde(default)]
    pub errors: u64,
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
        serde_json::from_slice(payload)
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
  "did": null,
  "seq": 4711,
  "timestamp": "2020-02-07T15:17:00Z",
  "started": "2020-02-07T14:16:00Z",
  "status": "ok",
  "errors": 0
}"#;

        let update = SessionUpdate {
            session_id: "8333339f-5675-4f89-a9a0-1c935255ab58".parse().unwrap(),
            distinct_id: None,
            sequence: 4711, // this would be a timestamp instead
            timestamp: "2020-02-07T15:17:00Z".parse().unwrap(),
            started: "2020-02-07T14:16:00Z".parse().unwrap(),
            duration: None,
            init: false,
            status: SessionStatus::Ok,
            errors: 0,
            attributes: SessionAttributes::default(),
        };

        let mut parsed = SessionUpdate::parse(json.as_bytes()).unwrap();

        // Sequence is defaulted to the current timestamp. Override for snapshot.
        assert!((parsed.sequence - default_sequence()) <= 1);
        parsed.sequence = 4711;

        assert_eq_dbg!(update, parsed);
        assert_eq_str!(output, serde_json::to_string_pretty(&update).unwrap());
    }

    #[test]
    fn test_session_default_timestamp_and_sid() {
        let json = r#"{
  "started": "2020-02-07T14:16:00Z"
}"#;

        let parsed = SessionUpdate::parse(json.as_bytes()).unwrap();
        assert!(!parsed.session_id.is_nil());
    }

    #[test]
    fn test_session_roundtrip() {
        let json = r#"{
  "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
  "did": "foobarbaz",
  "seq": 42,
  "init": true,
  "timestamp": "2020-02-07T15:17:00Z",
  "started": "2020-02-07T14:16:00Z",
  "duration": 1947.49,
  "status": "exited",
  "errors": 0,
  "attrs": {
    "release": "sentry-test@1.0.0",
    "environment": "production",
    "ip_address": "::1",
    "user_agent": "Firefox/72.0"
  }
}"#;

        let update = SessionUpdate {
            session_id: "8333339f-5675-4f89-a9a0-1c935255ab58".parse().unwrap(),
            distinct_id: Some("foobarbaz".into()),
            sequence: 42,
            timestamp: "2020-02-07T15:17:00Z".parse().unwrap(),
            started: "2020-02-07T14:16:00Z".parse().unwrap(),
            duration: Some(1947.49),
            status: SessionStatus::Exited,
            errors: 0,
            init: true,
            attributes: SessionAttributes {
                release: Some("sentry-test@1.0.0".to_owned()),
                environment: Some("production".to_owned()),
                ip_address: Some("::1".parse().unwrap()),
                user_agent: Some("Firefox/72.0".to_owned()),
            },
        };

        assert_eq_dbg!(update, SessionUpdate::parse(json.as_bytes()).unwrap());
        assert_eq_str!(json, serde_json::to_string_pretty(&update).unwrap());
    }
}
