use serde::{Deserialize, Serialize};

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
}

impl DataScrubbingConfig {
    /// Returns true if datascrubbing is disabled.
    pub fn is_disabled(&self) -> bool {
        !self.scrub_data && !self.scrub_ip_addresses
    }
}

impl Default for DataScrubbingConfig {
    fn default() -> DataScrubbingConfig {
        DataScrubbingConfig {
            exclude_fields: Default::default(),
            scrub_data: true,
            scrub_ip_addresses: true,
            sensitive_fields: Default::default(),
            scrub_defaults: true,
        }
    }
}
