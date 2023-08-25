use std::fmt::{self, Display};
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use relay_common::uuid::Uuid;
use serde::{Deserialize, Serialize};

use crate::protocol::utils::null_to_default;
use crate::protocol::IpAddr;

/// The type of session event we're dealing with.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
    /// The session exited cleanly but experienced some errors during its run.
    Errored,
    /// Unknown status, for forward compatibility.
    Unknown(String),
}

impl SessionStatus {
    /// Returns `true` if the status indicates an ended session.
    pub fn is_terminal(&self) -> bool {
        !matches!(self, SessionStatus::Ok)
    }

    /// Returns `true` if the status indicates a session with any kind of error or crash.
    pub fn is_error(&self) -> bool {
        !matches!(self, SessionStatus::Ok | SessionStatus::Exited)
    }

    /// Returns `true` if the status indicates a fatal session.
    pub fn is_fatal(&self) -> bool {
        matches!(self, SessionStatus::Crashed | SessionStatus::Abnormal)
    }
    fn as_str(&self) -> &str {
        match self {
            SessionStatus::Ok => "ok",
            SessionStatus::Crashed => "crashed",
            SessionStatus::Abnormal => "abnormal",
            SessionStatus::Exited => "exited",
            SessionStatus::Errored => "errored",
            SessionStatus::Unknown(s) => s.as_str(),
        }
    }
}

impl Display for SessionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

relay_common::impl_str_serde!(SessionStatus, "A session status");

impl std::str::FromStr for SessionStatus {
    type Err = ParseSessionStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "ok" => SessionStatus::Ok,
            "crashed" => SessionStatus::Crashed,
            "abnormal" => SessionStatus::Abnormal,
            "exited" => SessionStatus::Exited,
            "errored" => SessionStatus::Errored,
            other => SessionStatus::Unknown(other.to_owned()),
        })
    }
}

impl Default for SessionStatus {
    fn default() -> Self {
        Self::Ok
    }
}

/// An error used when parsing `SessionStatus`.
#[derive(Debug)]
pub struct ParseSessionStatusError;

impl fmt::Display for ParseSessionStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid session status")
    }
}

impl std::error::Error for ParseSessionStatusError {}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AbnormalMechanism {
    AnrForeground,
    AnrBackground,
    #[serde(other)]
    #[default]
    None,
}

#[derive(Debug)]
pub struct ParseAbnormalMechanismError;

impl fmt::Display for ParseAbnormalMechanismError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid abnormal mechanism")
    }
}

relay_common::derive_fromstr_and_display!(AbnormalMechanism, ParseAbnormalMechanismError, {
    AbnormalMechanism::AnrForeground => "anr_foreground",
    AbnormalMechanism::AnrBackground => "anr_background",
    AbnormalMechanism::None => "none",
});

impl AbnormalMechanism {
    fn is_none(&self) -> bool {
        *self == Self::None
    }
}

/// Additional attributes for Sessions.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionAttributes {
    /// The release version string.
    pub release: String,

    /// The environment identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,

    /// The ip address of the user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<IpAddr>,

    /// The user agent of the user.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
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

/// Contains information about errored sessions. See [`SessionLike`].
pub enum SessionErrored {
    /// Contains the UUID for a single errored session.
    Individual(Uuid),
    /// Contains the number of all errored sessions in an aggregate.
    /// errored, crashed, abnormal all count towards errored sessions.
    Aggregated(u32),
}

/// Common interface for [`SessionUpdate`] and [`SessionAggregateItem`].
pub trait SessionLike {
    fn started(&self) -> DateTime<Utc>;
    fn distinct_id(&self) -> Option<&String>;
    fn total_count(&self) -> u32;
    fn abnormal_count(&self) -> u32;
    fn crashed_count(&self) -> u32;
    fn all_errors(&self) -> Option<SessionErrored>;
    fn abnormal_mechanism(&self) -> AbnormalMechanism;
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
    /// An optional duration of the session in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,
    /// The status of the session.
    #[serde(default)]
    pub status: SessionStatus,
    /// The number of errors that ocurred.
    #[serde(default)]
    pub errors: u64,
    /// The session event attributes.
    #[serde(rename = "attrs")]
    pub attributes: SessionAttributes,
    /// The abnormal mechanism.
    #[serde(
        default,
        deserialize_with = "null_to_default",
        skip_serializing_if = "AbnormalMechanism::is_none"
    )]
    pub abnormal_mechanism: AbnormalMechanism,
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

impl SessionLike for SessionUpdate {
    fn started(&self) -> DateTime<Utc> {
        self.started
    }

    fn distinct_id(&self) -> Option<&String> {
        self.distinct_id.as_ref()
    }

    fn total_count(&self) -> u32 {
        u32::from(self.init)
    }

    fn abnormal_count(&self) -> u32 {
        match self.status {
            SessionStatus::Abnormal => 1,
            _ => 0,
        }
    }

    fn crashed_count(&self) -> u32 {
        match self.status {
            SessionStatus::Crashed => 1,
            _ => 0,
        }
    }

    fn all_errors(&self) -> Option<SessionErrored> {
        if self.errors > 0 || self.status.is_error() {
            Some(SessionErrored::Individual(self.session_id))
        } else {
            None
        }
    }

    fn abnormal_mechanism(&self) -> AbnormalMechanism {
        self.abnormal_mechanism
    }
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(val: &u32) -> bool {
    *val == 0
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionAggregateItem {
    /// The timestamp of when the session itself started.
    pub started: DateTime<Utc>,
    /// The distinct identifier.
    #[serde(rename = "did", default, skip_serializing_if = "Option::is_none")]
    pub distinct_id: Option<String>,
    /// The number of exited sessions that ocurred.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub exited: u32,
    /// The number of errored sessions that ocurred, not including the abnormal and crashed ones.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub errored: u32,
    /// The number of abnormal sessions that ocurred.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub abnormal: u32,
    /// The number of crashed sessions that ocurred.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub crashed: u32,
}

impl SessionLike for SessionAggregateItem {
    fn started(&self) -> DateTime<Utc> {
        self.started
    }

    fn distinct_id(&self) -> Option<&String> {
        self.distinct_id.as_ref()
    }

    fn total_count(&self) -> u32 {
        self.exited + self.errored + self.abnormal + self.crashed
    }

    fn abnormal_count(&self) -> u32 {
        self.abnormal
    }

    fn crashed_count(&self) -> u32 {
        self.crashed
    }

    fn all_errors(&self) -> Option<SessionErrored> {
        // Errors contain crashed & abnormal as well.
        // See https://github.com/getsentry/snuba/blob/c45f2a8636f9ea3dfada4e2d0ae5efef6c6248de/snuba/migrations/snuba_migrations/sessions/0003_sessions_matview.py#L80-L81
        let all_errored = self.abnormal + self.crashed + self.errored;
        if all_errored > 0 {
            Some(SessionErrored::Aggregated(all_errored))
        } else {
            None
        }
    }
    fn abnormal_mechanism(&self) -> AbnormalMechanism {
        AbnormalMechanism::None
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionAggregates {
    /// A batch of sessions that were started.
    #[serde(default)]
    pub aggregates: Vec<SessionAggregateItem>,
    /// The shared session event attributes.
    #[serde(rename = "attrs")]
    pub attributes: SessionAttributes,
}

impl SessionAggregates {
    /// Parses a session batch from JSON.
    pub fn parse(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }

    /// Serializes a session batch back into JSON.
    pub fn serialize(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

#[cfg(test)]
mod tests {

    use std::str::FromStr;

    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_sessionstatus_unknown() {
        let unknown = SessionStatus::from_str("invalid status").unwrap();
        if let SessionStatus::Unknown(inner) = unknown {
            assert_eq!(inner, "invalid status".to_owned());
        } else {
            panic!();
        }
    }

    #[test]
    fn test_session_default_values() {
        let json = r#"{
  "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
  "timestamp": "2020-02-07T15:17:00Z",
  "started": "2020-02-07T14:16:00Z",
  "attrs": {
    "release": "sentry-test@1.0.0"
  }
}"#;

        let output = r#"{
  "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
  "did": null,
  "seq": 4711,
  "timestamp": "2020-02-07T15:17:00Z",
  "started": "2020-02-07T14:16:00Z",
  "status": "ok",
  "errors": 0,
  "attrs": {
    "release": "sentry-test@1.0.0"
  }
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
            abnormal_mechanism: AbnormalMechanism::None,
            errors: 0,
            attributes: SessionAttributes {
                release: "sentry-test@1.0.0".to_owned(),
                environment: None,
                ip_address: None,
                user_agent: None,
            },
        };

        let mut parsed = SessionUpdate::parse(json.as_bytes()).unwrap();

        // Sequence is defaulted to the current timestamp. Override for snapshot.
        assert!((default_sequence() - parsed.sequence) <= 1);
        parsed.sequence = 4711;

        assert_eq!(update, parsed);
        assert_eq!(output, serde_json::to_string_pretty(&update).unwrap());
    }

    #[test]
    fn test_session_default_timestamp_and_sid() {
        let json = r#"{
  "started": "2020-02-07T14:16:00Z",
  "attrs": {
      "release": "sentry-test@1.0.0"
  }
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
            abnormal_mechanism: AbnormalMechanism::None,
            errors: 0,
            init: true,
            attributes: SessionAttributes {
                release: "sentry-test@1.0.0".to_owned(),
                environment: Some("production".to_owned()),
                ip_address: Some(IpAddr::parse("::1").unwrap()),
                user_agent: Some("Firefox/72.0".to_owned()),
            },
        };

        assert_eq!(update, SessionUpdate::parse(json.as_bytes()).unwrap());
        assert_eq!(json, serde_json::to_string_pretty(&update).unwrap());
    }

    #[test]
    fn test_session_ip_addr_auto() {
        let json = r#"{
  "started": "2020-02-07T14:16:00Z",
  "attrs": {
    "release": "sentry-test@1.0.0",
    "ip_address": "{{auto}}"
  }
}"#;

        let update = SessionUpdate::parse(json.as_bytes()).unwrap();
        assert_eq!(update.attributes.ip_address, Some(IpAddr::auto()));
    }
    #[test]
    fn test_session_abnormal_mechanism() {
        let json = r#"{
    "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
    "started": "2020-02-07T14:16:00Z",
    "status": "abnormal",
    "abnormal_mechanism": "anr_background",
    "attrs": {
    "release": "sentry-test@1.0.0",
    "environment": "production"
    }
    }"#;

        let update = SessionUpdate::parse(json.as_bytes()).unwrap();
        assert_eq!(update.abnormal_mechanism, AbnormalMechanism::AnrBackground);
    }

    #[test]
    fn test_session_invalid_abnormal_mechanism() {
        let json = r#"{
  "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
  "started": "2020-02-07T14:16:00Z",
  "status": "abnormal",
  "abnormal_mechanism": "invalid_mechanism",
  "attrs": {
    "release": "sentry-test@1.0.0",
    "environment": "production"
  }
}"#;

        let update = SessionUpdate::parse(json.as_bytes()).unwrap();
        assert_eq!(update.abnormal_mechanism, AbnormalMechanism::None);
    }

    #[test]
    fn test_session_null_abnormal_mechanism() {
        let json = r#"{
  "sid": "8333339f-5675-4f89-a9a0-1c935255ab58",
  "started": "2020-02-07T14:16:00Z",
  "status": "abnormal",
  "abnormal_mechanism": null,
  "attrs": {
    "release": "sentry-test@1.0.0",
    "environment": "production"
  }
}"#;

        let update = SessionUpdate::parse(json.as_bytes()).unwrap();
        assert_eq!(update.abnormal_mechanism, AbnormalMechanism::None);
    }
}
