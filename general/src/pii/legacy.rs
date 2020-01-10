use serde::{Deserialize, Serialize};

use lazycell::AtomicLazyCell;

use crate::pii::{convert, PiiConfig};

/// Helper method to check whether a flag is false.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_flag_default(flag: &bool) -> bool {
    !*flag
}

/// Configuration for Sentry's datascrubbing
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    #[serde(skip, default = "AtomicLazyCell::new")]
    pub(super) pii_config: AtomicLazyCell<Option<PiiConfig>>,
}

impl DataScrubbingConfig {
    /// Returns true if datascrubbing is disabled.
    pub fn is_disabled(&self) -> bool {
        !self.scrub_data && !self.scrub_ip_addresses
    }

    /// Get the PII config derived from datascrubbing settings.
    pub fn pii_config(&self) -> Option<&PiiConfig> {
        if let Some(pii_config) = self.pii_config.borrow() {
            return pii_config.as_ref();
        }

        let pii_config = convert::to_pii_config(&self);
        self.pii_config.fill(pii_config).ok();

        // If filling the lazy cell fails, another thread is currently inserting. There are two
        // possible states:
        //  1. The cell is now filled. If we borrow the value now, we will get a response.
        //  2. The cell is locked but not filled. If we borrow the value now, we will get `None` and
        //     have to try again. Practically, this loop only executes once or twice.
        loop {
            match self.pii_config.borrow() {
                Some(pii_config) => break pii_config.as_ref(),
                None => std::thread::sleep(std::time::Duration::from_micros(1)),
            }
        }
    }
}

impl Default for DataScrubbingConfig {
    fn default() -> DataScrubbingConfig {
        DataScrubbingConfig {
            exclude_fields: Vec::new(),
            scrub_data: false,
            scrub_ip_addresses: false,
            sensitive_fields: Vec::new(),
            scrub_defaults: false,
            pii_config: AtomicLazyCell::new(),
        }
    }
}
