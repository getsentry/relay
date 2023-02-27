//! Crons protocol and processing for Sentry.
//!
//! [Cron Monitors] allow you to monitor the uptime and performance of any scheduled, recurring job
//! in Sentry. Once implemented, it'll allow you to get alerts and metrics to help you solve errors,
//! detect timeouts, and prevent disruptions to your service.
//!
//! # API
//!
//! The public API documentation is available on [Sentry Docs](https://docs.sentry.io/api/crons/).
//!
//! [cron monitors]: https://docs.sentry.io/product/crons/

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![warn(missing_docs)]

use relay_common::Uuid;
use serde::{Deserialize, Serialize};

/// Error returned from [`process_checkin`].
#[derive(Debug, thiserror::Error)]
pub enum ProcessCheckinError {
    /// Failed to deserialize the payload.
    #[error("failed to deserialize checkin")]
    Json(#[from] serde_json::Error),
}

///
#[derive(Clone, Copy, Debug, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CheckinStatus {
    /// Checkin had no issues during execution.
    Ok,
    /// Checkin failed or otherwise had some issues.
    Error,
    /// Checkin is expectred to complete.
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

/// The cron monitor checkin payload.
#[derive(Debug, Deserialize, Serialize)]
struct Checkin {
    /// Unique identifier of this checkin.
    #[serde(serialize_with = "uuid_simple")]
    checkin_id: Uuid, // TODO(ja): roundtrip without slashes

    /// Identifier of the monitor for this checkin.
    #[serde(serialize_with = "uuid_simple")]
    monitor_id: Uuid,

    /// Status of this checkin. Defaults to `"unknown"`.
    status: CheckinStatus,

    /// Duration of this check since it has started in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    duration: Option<f64>,
}

/// Normalizes a cron monitor checkin payload.
///
/// Returns `None` if the payload was valid and does not have to be changed. Returns `Some` for
/// valid payloads that were normalized.
pub fn process_checkin(payload: &[u8]) -> Result<Vec<u8>, ProcessCheckinError> {
    let mut checkin = serde_json::from_slice::<Checkin>(payload)?;

    // Missed status cannot be ingested, this is computed on the server.
    if checkin.status == CheckinStatus::Missed {
        checkin.status = CheckinStatus::Unknown;
    }

    let serialized = serde_json::to_vec(&checkin)?;
    Ok(serialized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use similar_asserts::assert_eq;

    #[test]
    fn test_json_roundtrip() {
        let json = r#"{
  "checkin_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_id": "4dc8556e039245c7bd569f8cf513ea42",
  "status": "in_progress",
  "duration": 21.0
}"#;

        let checkin = serde_json::from_str::<Checkin>(json).unwrap();
        let serialized = serde_json::to_string_pretty(&checkin).unwrap();

        assert_eq!(json, serialized);
    }
}
