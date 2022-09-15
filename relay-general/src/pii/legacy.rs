//! Legacy datascrubbing coniguration
//!
//! All these configuration options are ignored by the new data scrubbers which operate
//! solely from the [PiiConfig] rules for the project.

use std::convert::{TryFrom, TryInto};

use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

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
pub struct DataScrubbingConfigRepr {
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

#[derive(Clone, Debug, Default)]
pub struct DataScrubbingConfig {
    /// The original datascrubbing settings as received from Sentry.
    repr: DataScrubbingConfigRepr,
    /// PII config derived from datascrubbing settings.
    ///
    /// Cached because the conversion process is expensive.
    pub pii_config: Option<PiiConfig>,
}

impl DataScrubbingConfig {
    /// Returns true if datascrubbing is disabled.
    pub fn is_disabled(&self) -> bool {
        !self.repr.scrub_data && !self.repr.scrub_ip_addresses
    }
}

impl TryFrom<DataScrubbingConfigRepr> for DataScrubbingConfig {
    type Error = PiiConfigError;

    fn try_from(repr: DataScrubbingConfigRepr) -> Result<Self, Self::Error> {
        let pii_config = convert::to_pii_config(&repr)?;
        Ok(Self { repr, pii_config })
    }
}

impl<'de> Deserialize<'de> for DataScrubbingConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let repr = DataScrubbingConfigRepr::deserialize(deserializer)?;
        repr.try_into().map_err(Error::custom)
    }
}

impl Serialize for DataScrubbingConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.repr.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_json_snapshot;

    use super::DataScrubbingConfig;

    #[test]
    fn test_datascrubbing_config_roundtrip() {
        let json = r#"{
            "excludeFields": ["a","b"],
            "scrubData": true,
            "scrubIpAddresses": false,
            "sensitiveFields": ["c", "d"],
            "scrubDefaults": true
        }"#;
        let config: DataScrubbingConfig = serde_json::from_str(json).unwrap();
        let reserialized = serde_json::to_string(&config).unwrap();
        assert_json_snapshot!(reserialized, @r###""{\"excludeFields\":[\"a\",\"b\"],\"scrubData\":true,\"sensitiveFields\":[\"c\",\"d\"],\"scrubDefaults\":true}""###);
    }
}
