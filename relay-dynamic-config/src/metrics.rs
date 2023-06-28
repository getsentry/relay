//! Dynamic configuration for metrics extraction from sessions and transactions.

use std::collections::BTreeSet;

use relay_sampling::RuleCondition;
use serde::{Deserialize, Serialize};

/// Rule defining when a target tag should be set on a metric.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaggingRule {
    // note: could add relay_sampling::RuleType here, but right now we only support transaction
    // events
    /// Condition that defines when to set the tag.
    pub condition: RuleCondition,
    /// Metrics on which the tag is set.
    pub target_metrics: BTreeSet<String>,
    /// Name of the tag that is set.
    pub target_tag: String,
    /// Value of the tag that is set.
    pub tag_value: String,
}

/// Current version of metrics extraction.
const SESSION_EXTRACT_VERSION: u16 = 3;
const EXTRACT_ABNORMAL_MECHANISM_VERSION: u16 = 2;

/// Configuration for metric extraction from sessions.
#[derive(Debug, Clone, Copy, Default, serde::Deserialize, serde::Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct SessionMetricsConfig {
    /// The revision of the extraction algorithm.
    ///
    /// Provided the revision is lower than or equal to the revision supported by this Relay,
    /// metrics are extracted. If the revision is higher than what this Relay supports, it does not
    /// extract metrics from sessions, and instead forwards them to the upstream.
    ///
    /// Version `0` (default) disables extraction.
    pub(crate) version: u16,

    /// Drop sessions after successfully extracting metrics.
    pub(crate) drop: bool,
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

    /// Returns `true` if the session should be dropped after extracting metrics.
    pub fn should_drop(&self) -> bool {
        self.drop
    }
}

/// Configuration for extracting custom measurements from transaction payloads.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CustomMeasurementConfig {
    /// The maximum number of custom measurements to extract. Defaults to zero.
    limit: usize,
}

/// Maximum supported version of metrics extraction from transactions.
///
/// The version is an integer scalar, incremented by one on each new version.
const TRANSACTION_EXTRACT_VERSION: u16 = 1;

/// Defines whether URL transactions should be considered low cardinality.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum AcceptTransactionNames {
    /// For some SDKs, accept all transaction names, while for others, apply strict rules.
    ClientBased,

    /// Only accept transaction names with a low-cardinality source.
    /// Any value other than "clientBased" will be interpreted as "strict".
    #[serde(other)]
    Strict,
}

impl Default for AcceptTransactionNames {
    fn default() -> Self {
        Self::Strict
    }
}

/// Configuration for extracting metrics from transaction payloads.
#[derive(Default, Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TransactionMetricsConfig {
    /// The required version to extract transaction metrics.
    pub version: u16,
    /// Custom event tags that are transferred from the transaction to metrics.
    pub extract_custom_tags: BTreeSet<String>,
    /// Deprecated in favor of top-level config field. Still here to be forwarded to external relays.
    pub custom_measurements: CustomMeasurementConfig,
    /// Defines whether URL transactions should be considered low cardinality.
    pub accept_transaction_names: AcceptTransactionNames,
}

impl TransactionMetricsConfig {
    /// Creates an enabled configuration with empty defaults.
    pub fn new() -> Self {
        Self {
            version: 1,
            ..Self::default()
        }
    }

    /// Returns `true` if metrics extraction is enabled and compatible with this Relay.
    pub fn is_enabled(&self) -> bool {
        self.version > 0 && self.version <= TRANSACTION_EXTRACT_VERSION
    }
}
