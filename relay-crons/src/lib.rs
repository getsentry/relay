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

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

/// Error returned from [`process_checkin`].
#[derive(Debug, thiserror::Error)]
pub enum ProcessCheckinError {
    /// Failed to deserialize the payload.
    #[error("failed to deserialize checkin")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Deserialize, Serialize)]
struct Checkin {
    // TODO(ja): Fields
}

/// Normalizes a cron monitor checkin payload.
///
/// Returns `None` if the payload was valid and does not have to be changed. Returns `Some` for
/// valid payloads that were normalized.
pub fn process_checkin(payload: &[u8]) -> Result<Option<Vec<u8>>, ProcessCheckinError> {
    let checkin = serde_json::from_slice(payload)?;

    todo!();

    Ok(None)
}
