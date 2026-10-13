//! Dynamic configuration for metrics extraction from sessions.

use serde::{Deserialize, Serialize};

/// Current version of metrics extraction.
const SESSION_EXTRACT_VERSION: u16 = 3;
const EXTRACT_ABNORMAL_MECHANISM_VERSION: u16 = 2;

/// Configuration for metric extraction from sessions.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SessionMetricsConfig {
    /// The revision of the extraction algorithm.
    ///
    /// Provided the revision is lower than or equal to the revision supported by this Relay,
    /// metrics are extracted. If the revision is higher than what this Relay supports, it does not
    /// extract metrics from sessions, and instead forwards them to the upstream.
    ///
    /// Version `0` (default) disables extraction.
    version: u16,
}

impl SessionMetricsConfig {
    /// Returns `true` if session metrics is enabled and compatible.
    pub fn is_enabled(&self) -> bool {
        self.version > 0 && self.version <= SESSION_EXTRACT_VERSION
    }

    /// Returns `true` if Relay should not extract metrics from sessions.
    pub fn is_disabled(&self) -> bool {
        !self.is_enabled()
    }

    /// Whether or not the abnormal mechanism should be extracted as a tag.
    pub fn should_extract_abnormal_mechanism(&self) -> bool {
        self.version >= EXTRACT_ABNORMAL_MECHANISM_VERSION
    }
}

/// Configuration for extracting custom measurements from transaction payloads.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CustomMeasurementConfig {
    /// The maximum number of custom measurements to extract. Defaults to zero.
    limit: usize,
}
