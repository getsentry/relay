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

use once_cell::sync::Lazy;
use regex::Regex;
use relay_common::Uuid;
use serde::{Deserialize, Serialize};

/// Maximum length of monitor slugs.
const SLUG_LENGTH: usize = 50;

/// Error returned from [`process_check_in`].
#[derive(Debug, thiserror::Error)]
pub enum ProcessCheckInError {
    /// Failed to deserialize the payload.
    #[error("failed to deserialize check in")]
    Json(#[from] serde_json::Error),

    /// Monitor slug was empty after slugification.
    #[error("the monitor slug is empty or invalid")]
    EmptySlug,
}

///
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckInStatus {
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

/// The monitor configuration playload for upserting monitors during check-in
#[derive(Debug, Deserialize, Serialize)]
struct MonitorConfig {
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

// XXX(epurkhiser): This is a duplicate of the ClientSdkInfo that is part of the relay-general
// crate. Until we're able to migrate the checkin payload over to it's own protocol using
// metastructure we'll need to have this duplicated here
//
/// The SDK Interface describes the Sentry SDK and its configuration used to capture and transmit an event.
#[derive(Debug, Deserialize, Serialize)]
struct MinimalClientSdkInfo {
    pub name: String,
    pub version: String,
}

/// The monitor check-in payload.
#[derive(Debug, Deserialize, Serialize)]
struct CheckIn {
    /// Unique identifier of this check-in.
    #[serde(serialize_with = "uuid_simple")]
    check_in_id: Uuid,

    /// Identifier of the monitor for this check-in.
    monitor_slug: String,

    /// Status of this check-in. Defaults to `"unknown"`.
    status: CheckInStatus,

    /// monitor configuration to support upserts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sdk: Option<MinimalClientSdkInfo>,

    /// The environment to associate the check-in with
    #[serde(default, skip_serializing_if = "Option::is_none")]
    environment: Option<String>,

    /// Duration of this check since it has started in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    duration: Option<f64>,

    /// monitor configuration to support upserts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    monitor_config: Option<MonitorConfig>,
}

/// Normalizes a monitor check-in payload.
pub fn process_check_in(payload: &[u8]) -> Result<Vec<u8>, ProcessCheckInError> {
    let mut check_in = serde_json::from_slice::<CheckIn>(payload)?;

    // Missed status cannot be ingested, this is computed on the server.
    if check_in.status == CheckInStatus::Missed {
        check_in.status = CheckInStatus::Unknown;
    }

    check_in.monitor_slug = slugify(&check_in.monitor_slug);
    if check_in.monitor_slug.is_empty() {
        return Err(ProcessCheckInError::EmptySlug);
    }

    Ok(serde_json::to_vec(&check_in)?)
}

fn slugify(input: &str) -> String {
    static SLUG_CLEANER: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^a-zA-Z0-9\s_-]").unwrap());
    static SLUGIFIER: Lazy<Regex> = Lazy::new(|| Regex::new(r"[\s\-]+").unwrap());

    let cleaned = SLUG_CLEANER.replace_all(input, "");
    let mut slug = SLUGIFIER
        .replace_all(&cleaned, "-")
        .trim_matches('-')
        .to_owned();

    slug.truncate(SLUG_LENGTH);

    // Truncate may leave a trailing '-', so we may need to truncate again.
    if slug.ends_with('-') {
        slug.truncate(slug.len() - 1);
    }

    slug.make_ascii_lowercase();
    slug
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn process_json_roundtrip() {
        let json = r#"{
  "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_slug": "my-monitor",
  "status": "in_progress",
  "sdk": {
    "name": "sentry.rust",
    "version": "1.0.0"
  },
  "environment": "production",
  "duration": 21.0
}"#;

        let check_in = serde_json::from_str::<CheckIn>(json).unwrap();
        let serialized = serde_json::to_string_pretty(&check_in).unwrap();

        assert_eq!(json, serialized);
    }

    #[test]
    fn process_with_upsert_short() {
        let json = r#"{
  "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_slug": "my-monitor",
  "status": "in_progress",
  "sdk": {
    "name": "sentry.rust",
    "version": "1.0.0"
  },
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
    fn process_with_upsert_interval() {
        let json = r#"{
  "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_slug": "my-monitor",
  "status": "in_progress",
  "sdk": {
    "name": "sentry.rust",
    "version": "1.0.0"
  },
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
    fn process_with_upsert_full() {
        let json = r#"{
  "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_slug": "my-monitor",
  "status": "in_progress",
  "sdk": {
    "name": "sentry.rust",
    "version": "1.0.0"
  },
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
    fn process_empty_slug() {
        let json = r#"{
          "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
          "monitor_slug": "🚀🚀🚀",
          "status": "in_progress"
        }"#;

        let result = process_check_in(json.as_bytes());
        assert!(matches!(result, Err(ProcessCheckInError::EmptySlug)));
    }

    #[test]
    fn slugify_empty_string() {
        assert_eq!("", slugify(""));
    }

    #[test]
    fn slugify_truncate_honors_trim() {
        let input = "-".repeat(SLUG_LENGTH + 10) + "hello";
        assert_eq!("hello", slugify(&input));
    }

    #[test]
    fn slugify_trim_at_truncate() {
        let expected = "a".repeat(SLUG_LENGTH - 1);
        let input = expected.clone() + "-stripped";
        assert_eq!(expected, slugify(&input));
    }

    #[test]
    fn slugify_unicode() {
        let input = "🚀🚀🚀\tmyComplicated_slug\u{200A}name is here...";
        let expected = "mycomplicated_slug-name-is-here";
        assert_eq!(expected, slugify(input));
    }
}
