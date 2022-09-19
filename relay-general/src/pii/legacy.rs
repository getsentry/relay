//! Legacy datascrubbing coniguration
//!
//! All these configuration options are ignored by the new data scrubbers which operate
//! solely from the [PiiConfig] rules for the project.

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

use crate::pii::{convert, PiiConfig};

use super::config::PiiConfigError;

/// Helper method to check whether a flag is false.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_flag_default(flag: &bool) -> bool {
    !*flag
}

/// Configuration for Sentry's datascrubbing
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DataScrubbingConfig {
    /// List with the fields to be excluded.
    pub exclude_fields: Vec<String>,
    /// Toggles all data scrubbing on or off.
    #[serde(skip_serializing_if = "is_flag_default")]
    pub scrub_data: bool,
    /// Should ip addresses be scrubbed from messages?
    #[serde(skip_serializing_if = "is_flag_default")]
    pub scrub_ip_addresses: bool,
    /// List of sensitive fields to be scrubbed from the messages.
    pub sensitive_fields: Vec<String>,
    /// Controls whether default fields will be scrubbed.
    #[serde(skip_serializing_if = "is_flag_default")]
    pub scrub_defaults: bool,

    /// PII config derived from datascrubbing settings.
    ///
    /// Cached because the conversion process is expensive.
    ///
    /// NOTE: We discarded the idea of making the conversion to PiiConfig part of deserialization,
    /// because we want the conversion to run on the processing sync arbiter, so that it does not
    /// slow down or even crash other parts of the system.
    #[serde(skip)]
    pub(super) pii_config: OnceCell<Result<Option<PiiConfig>, PiiConfigError>>,
}

impl DataScrubbingConfig {
    /// Creates a new data scrubbing configuration that does nothing on the event.
    pub fn new_disabled() -> Self {
        DataScrubbingConfig {
            exclude_fields: vec![],
            scrub_data: false,
            scrub_ip_addresses: false,
            sensitive_fields: vec![],
            scrub_defaults: false,
            pii_config: OnceCell::with_value(Ok(None)),
        }
    }

    /// Returns true if datascrubbing is disabled.
    pub fn is_disabled(&self) -> bool {
        !self.scrub_data && !self.scrub_ip_addresses
    }

    /// Get the PII config derived from datascrubbing settings.
    ///
    /// This can be computationally expensive when called for the first time. The result is cached
    /// internally and reused on the second call.
    pub fn pii_config(&self) -> Result<&'_ Option<PiiConfig>, &PiiConfigError> {
        self.pii_config
            .get_or_init(|| self.pii_config_uncached())
            .as_ref()
    }

    /// Like [`pii_config`](Self::pii_config) but without internal caching.
    #[inline]
    pub fn pii_config_uncached(&self) -> Result<Option<PiiConfig>, PiiConfigError> {
        convert::to_pii_config(self).map_err(|e| {
            relay_log::error!("Failed to convert datascrubbing config");
            e
        })
    }
}
