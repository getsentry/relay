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

use relay_common::Uuid;
use serde::{Deserialize, Serialize};

/// Error returned from [`process_check_in`].
#[derive(Debug, thiserror::Error)]
pub enum ProcessCheckInError {
    /// Failed to deserialize the payload.
    #[error("failed to deserialize check in")]
    Json(#[from] serde_json::Error),
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

/// The monitor check-in payload.
#[derive(Debug, Deserialize, Serialize)]
struct CheckIn {
    /// Unique identifier of this check-in.
    #[serde(serialize_with = "uuid_simple")]
    check_in_id: Uuid,

    /// Identifier of the monitor for this check-in.
    #[serde(serialize_with = "uuid_simple")]
    monitor_id: Uuid,

    /// Status of this check-in. Defaults to `"unknown"`.
    status: CheckInStatus,

    /// Duration of this check since it has started in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    duration: Option<f64>,
}

/// Normalizes a monitor check-in payload.
///
/// Returns `None` if the payload was valid and does not have to be changed. Returns `Some` for
/// valid payloads that were normalized.
pub fn process_check_in(payload: &[u8]) -> Result<Option<Vec<u8>>, ProcessCheckInError> {
    let mut check_in = serde_json::from_slice::<CheckIn>(payload)?;
    let mut changed = false;

    // Missed status cannot be ingested, this is computed on the server.
    if check_in.status == CheckInStatus::Missed {
        check_in.status = CheckInStatus::Unknown;
        changed = true;
    }

    if changed {
        Ok(Some(serde_json::to_vec(&check_in)?))
    } else {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use similar_asserts::assert_eq;

    #[test]
    fn test_json_roundtrip() {
        let json = r#"{
  "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
  "monitor_id": "4dc8556e039245c7bd569f8cf513ea42",
  "status": "in_progress",
  "duration": 21.0
}"#;

        let check_in = serde_json::from_str::<CheckIn>(json).unwrap();
        let serialized = serde_json::to_string_pretty(&check_in).unwrap();

        assert_eq!(json, serialized);
    }
}
