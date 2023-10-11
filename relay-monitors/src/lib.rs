//! Monitors protocol and processing for Sentry.
//!
//! [Monitors] allow you to monitor the uptime and performance of any scheduled, recurring job in
//! Sentry. Once implemented, it'll allow you to get alerts and metrics to help you solve errors,
//! detect timeouts, and prevent disruptions to your service.
//!
//! # API
//!
//! The public API documentation is available on [Sentry Docs](https://docs.sentry.io/api/crons/).
//!
//! [monitors]: https://docs.sentry.io/product/crons/

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![warn(missing_docs)]

use once_cell::sync::OnceCell;
use relay_base_schema::project::ProjectId;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Maximum length of monitor slugs.
const SLUG_LENGTH: usize = 50;

/// Maximum length of environment names.
const ENVIRONMENT_LENGTH: usize = 64;

/// Error returned from [`process_check_in`].
#[derive(Debug, thiserror::Error)]
pub enum ProcessCheckInError {
    /// Failed to deserialize the payload.
    #[error("failed to deserialize check in")]
    Json(#[from] serde_json::Error),

    /// Monitor slug was empty after slugification.
    #[error("the monitor slug is empty or invalid")]
    EmptySlug,

    /// Environment name was invalid.
    #[error("the environment is invalid")]
    InvalidEnvironment,
}

///
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckInStatus {
    /// Check-in had no issues during execution.
    Ok,
    /// Check-in failed or otherwise had some issues.
    Error,
    /// Check-in is expectred to complete.
    InProgress,
    /// Monitor did not check in on time.
    Missed,
    /// No status was passed.
    #[serde(other)]
    Unknown,
}

fn uuid_simple<S>(uuid: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    uuid.as_simple().serialize(serializer)
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Schedule {
    Crontab { value: String },
    Interval { value: u64, unit: IntervalName },
}

#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum IntervalName {
    Year,
    Month,
    Week,
    Day,
    Hour,
    Minute,
}

/// The monitor configuration payload for upserting monitors during check-in
#[derive(Debug, Deserialize, Serialize)]
pub struct MonitorConfig {
    /// The monitor schedule configuration
    schedule: Schedule,

    /// How long (in minutes) after the expected checkin time will we wait until we consider the
    /// checkin to have been missed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkin_margin: Option<u64>,

    /// How long (in minutes) is the checkin allowed to run for in in_rogress before it is
    /// considered failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_runtime: Option<u64>,

    /// tz database style timezone string
    #[serde(default, skip_serializing_if = "Option::is_none")]
    timezone: Option<String>,
}

/// The trace context sent with a check-in.
#[derive(Debug, Deserialize, Serialize)]
pub struct CheckInTrace {
    /// Trace-ID of the check-in.
    #[serde(serialize_with = "uuid_simple")]
    trace_id: Uuid,
}

/// Any contexts sent in the check-in payload.
#[derive(Debug, Deserialize, Serialize)]
pub struct CheckInContexts {
    /// Trace context sent with a check-in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    trace: Option<CheckInTrace>,
}

/// The monitor check-in payload.
#[derive(Debug, Deserialize, Serialize)]
pub struct CheckIn {
    /// Unique identifier of this check-in.
    #[serde(default, serialize_with = "uuid_simple")]
    pub check_in_id: Uuid,

    /// Identifier of the monitor for this check-in.
    #[serde(default)]
    pub monitor_slug: String,

    /// Status of this check-in. Defaults to `"unknown"`.
    pub status: CheckInStatus,

    /// The environment to associate the check-in with
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<String>,

    /// Duration of this check since it has started in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<f64>,

    /// monitor configuration to support upserts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub monitor_config: Option<MonitorConfig>,

    /// Contexts describing the associated environment of the job run.
    /// Only supports trace for now.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contexts: Option<CheckInContexts>,
}

/// The result from calling process_check_in
pub struct ProcessedCheckInResult {
    /// The routing key to be used for the check-in payload.
    ///
    /// Important to help ensure monitor check-ins are processed in order by routing check-ins from
    /// the same monitor to the same place.
    pub routing_hint: Uuid,

    /// The JSON payload of the processed check-in.
    pub payload: Vec<u8>,
}

/// Normalizes a monitor check-in payload.
pub fn process_check_in(
    payload: &[u8],
    project_id: ProjectId,
) -> Result<ProcessedCheckInResult, ProcessCheckInError> {
    let mut check_in = serde_json::from_slice::<CheckIn>(payload)?;

    // Missed status cannot be ingested, this is computed on the server.
    if check_in.status == CheckInStatus::Missed {
        check_in.status = CheckInStatus::Unknown;
    }

    trim_slug(&mut check_in.monitor_slug);

    if check_in.monitor_slug.is_empty() {
        return Err(ProcessCheckInError::EmptySlug);
    }

    if check_in
        .environment
        .as_ref()
        .is_some_and(|e| e.chars().count() > ENVIRONMENT_LENGTH)
    {
        return Err(ProcessCheckInError::InvalidEnvironment);
    }

    static NAMESPACE: OnceCell<Uuid> = OnceCell::new();
    let namespace = NAMESPACE
        .get_or_init(|| Uuid::new_v5(&Uuid::NAMESPACE_URL, b"https://sentry.io/crons/#did"));

    // Use the project_id + monitor_slug as the routing key hint. This helps ensure monitor
    // check-ins are processed in order by consistently routing check-ins from the same monitor.
    let project_id_slug_key: Vec<u8> = [
        &project_id.value().to_be_bytes()[..],
        check_in.monitor_slug.as_bytes(),
    ]
    .concat();

    let routing_hint = Uuid::new_v5(namespace, project_id_slug_key.as_slice());

    Ok(ProcessedCheckInResult {
        routing_hint,
        payload: serde_json::to_vec(&check_in)?,
    })
}

fn trim_slug(slug: &mut String) {
    if let Some((overflow, _)) = slug.char_indices().nth(SLUG_LENGTH) {
        slug.truncate(overflow);
    }
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn truncate_basic() {
        let mut test1 = "test_".repeat(50);
        trim_slug(&mut test1);
        assert_eq!("test_test_test_test_test_test_test_test_test_test_", test1,);

        let mut test2 = "🦀".repeat(SLUG_LENGTH + 10);
        trim_slug(&mut test2);
        assert_eq!("🦀".repeat(SLUG_LENGTH), test2);
    }

    #[test]
    fn serialize_json_roundtrip() {
        let json = r#"{
  "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_slug": "my-monitor",
  "status": "in_progress",
  "environment": "production",
  "duration": 21.0,
  "contexts": {
    "trace": {
      "trace_id": "8f431b7aa08441bbbd5a0100fd91f9fe"
    }
  }
}"#;

        let check_in = serde_json::from_str::<CheckIn>(json).unwrap();
        let serialized = serde_json::to_string_pretty(&check_in).unwrap();

        assert_eq!(json, serialized);
    }

    #[test]
    fn serialize_with_upsert_short() {
        let json = r#"{
  "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_slug": "my-monitor",
  "status": "in_progress",
  "monitor_config": {
    "schedule": {
      "type": "crontab",
      "value": "0 * * * *"
    }
  }
}"#;

        let check_in = serde_json::from_str::<CheckIn>(json).unwrap();
        let serialized = serde_json::to_string_pretty(&check_in).unwrap();

        assert_eq!(json, serialized);
    }

    #[test]
    fn serialize_with_upsert_interval() {
        let json = r#"{
  "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_slug": "my-monitor",
  "status": "in_progress",
  "monitor_config": {
    "schedule": {
      "type": "interval",
      "value": 5,
      "unit": "day"
    },
    "checkin_margin": 5,
    "max_runtime": 10,
    "timezone": "America/Los_Angles"
  }
}"#;

        let check_in = serde_json::from_str::<CheckIn>(json).unwrap();
        let serialized = serde_json::to_string_pretty(&check_in).unwrap();

        assert_eq!(json, serialized);
    }

    #[test]
    fn serialize_with_upsert_full() {
        let json = r#"{
  "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_slug": "my-monitor",
  "status": "in_progress",
  "monitor_config": {
    "schedule": {
      "type": "crontab",
      "value": "0 * * * *"
    },
    "checkin_margin": 5,
    "max_runtime": 10,
    "timezone": "America/Los_Angles"
  }
}"#;

        let check_in = serde_json::from_str::<CheckIn>(json).unwrap();
        let serialized = serde_json::to_string_pretty(&check_in).unwrap();

        assert_eq!(json, serialized);
    }

    #[test]
    fn process_simple() {
        let json = r#"{"check_in_id":"a460c25ff2554577b920fcfacae4e5eb","monitor_slug":"my-monitor","status":"ok"}"#;

        let result = process_check_in(json.as_bytes(), ProjectId::new(1));

        // The routing_hint should be consistent for the (project_id, monitor_slug)
        let expected_uuid = Uuid::parse_str("3612580a-5d37-594b-9a90-d8142792f9c8").unwrap();

        if let Ok(processed_result) = result {
            assert_eq!(String::from_utf8(processed_result.payload).unwrap(), json);
            assert_eq!(processed_result.routing_hint, expected_uuid);
        } else {
            panic!("Failed to process check-in")
        }
    }

    #[test]
    fn process_empty_slug() {
        let json = r#"{
          "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
          "monitor_slug": "",
          "status": "in_progress"
        }"#;

        let result = process_check_in(json.as_bytes(), ProjectId::new(1));
        assert!(matches!(result, Err(ProcessCheckInError::EmptySlug)));
    }

    #[test]
    fn process_invalid_environment() {
        let json = r#"{
          "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
          "monitor_slug": "test",
          "status": "in_progress",
          "environment": "1234567890123456789012345678901234567890123456789012345678901234567890"
        }"#;

        let result = process_check_in(json.as_bytes(), ProjectId::new(1));
        assert!(matches!(
            result,
            Err(ProcessCheckInError::InvalidEnvironment)
        ));
    }
}
