//! Dynamic configuration passed down from Sentry to Relay.
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

// TODO(jjbayer): pub mod instead
mod error_boundary;
pub use error_boundary::*;
mod metrics;
pub use metrics::*;
use relay_quotas::Quota;

use std::collections::BTreeSet;

use relay_filter::FiltersConfig;
use relay_general::pii::{DataScrubbingConfig, PiiConfig};
use relay_general::store::{BreakdownsConfig, MeasurementsConfig, TransactionNameRule};
use relay_general::types::SpanAttribute;
use relay_sampling::SamplingConfig;
use serde::{Deserialize, Serialize};

use relay_auth::PublicKey;
use serde_json::Value;

/// Features exposed by project config.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Feature {
    /// Enables ingestion and normalization of profiles.
    #[serde(rename = "organizations:profiling")]
    Profiling,
    /// Enables ingestion of Session Replays (Replay Recordings and Replay Events).
    #[serde(rename = "organizations:session-replay")]
    SessionReplay,
    /// Enables data scrubbing of replay recording payloads.
    #[serde(rename = "organizations:session-replay-recording-scrubbing")]
    SessionReplayRecordingScrubbing,
    /// Enables transaction names normalization.
    ///
    /// Replacing UUIDs, SHAs and numerical IDs by placeholders.
    #[serde(rename = "organizations:transaction-name-normalize")]
    TransactionNameNormalize,

    /// Unused.
    ///
    /// This used to control the initial experimental metrics extraction for sessions and has been
    /// discontinued.
    #[serde(rename = "organizations:metrics-extraction")]
    Deprecated1,

    /// Forward compatibility.
    #[serde(other)]
    Unknown,
}

/// These are config values that the user can modify in the UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProjectConfig {
    /// URLs that are permitted for cross original JavaScript requests.
    pub allowed_domains: Vec<String>,
    /// List of relay public keys that are permitted to access this project.
    pub trusted_relays: Vec<PublicKey>,
    /// Configuration for PII stripping.
    pub pii_config: Option<PiiConfig>,
    /// The grouping configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouping_config: Option<Value>,
    /// Configuration for filter rules.
    #[serde(skip_serializing_if = "FiltersConfig::is_empty")]
    pub filter_settings: FiltersConfig,
    /// Configuration for data scrubbers.
    #[serde(skip_serializing_if = "DataScrubbingConfig::is_disabled")]
    pub datascrubbing_settings: DataScrubbingConfig,
    /// Maximum event retention for the organization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_retention: Option<u16>,
    /// Usage quotas for this project.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub quotas: Vec<Quota>,
    /// Configuration for sampling traces, if not present there will be no sampling.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dynamic_sampling: Option<SamplingConfig>,
    /// Configuration for measurements.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurements: Option<MeasurementsConfig>,
    /// Configuration for operation breakdown. Will be emitted only if present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns_v2: Option<BreakdownsConfig>,
    /// Configuration for extracting metrics from sessions.
    #[serde(skip_serializing_if = "SessionMetricsConfig::is_disabled")]
    pub session_metrics: SessionMetricsConfig,
    /// Configuration for extracting metrics from transaction events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_metrics: Option<ErrorBoundary<TransactionMetricsConfig>>,
    /// The span attributes configuration.
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub span_attributes: BTreeSet<SpanAttribute>,
    /// Rules for applying metrics tags depending on the event's content.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub metric_conditional_tagging: Vec<TaggingRule>,
    /// Exposable features enabled for this project.
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub features: BTreeSet<Feature>,
    /// Transaction renaming rules.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tx_name_rules: Vec<TransactionNameRule>,
}

impl Default for ProjectConfig {
    fn default() -> Self {
        ProjectConfig {
            allowed_domains: vec!["*".to_string()],
            trusted_relays: vec![],
            pii_config: None,
            grouping_config: None,
            filter_settings: FiltersConfig::default(),
            datascrubbing_settings: DataScrubbingConfig::default(),
            event_retention: None,
            quotas: Vec::new(),
            dynamic_sampling: None,
            measurements: None,
            breakdowns_v2: None,
            session_metrics: SessionMetricsConfig::default(),
            transaction_metrics: None,
            span_attributes: BTreeSet::new(),
            metric_conditional_tagging: Vec::new(),
            features: BTreeSet::new(),
            tx_name_rules: Vec::new(),
        }
    }
}

/// These are config values that are passed to external Relays.
///
/// For documentation of the fields, see [`ProjectConfig`].
#[allow(missing_docs)]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase", remote = "ProjectConfig")]
pub struct LimitedProjectConfig {
    pub allowed_domains: Vec<String>,
    pub trusted_relays: Vec<PublicKey>,
    pub pii_config: Option<PiiConfig>,
    #[serde(skip_serializing_if = "FiltersConfig::is_empty")]
    pub filter_settings: FiltersConfig,
    #[serde(skip_serializing_if = "DataScrubbingConfig::is_disabled")]
    pub datascrubbing_settings: DataScrubbingConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dynamic_sampling: Option<SamplingConfig>,
    #[serde(skip_serializing_if = "SessionMetricsConfig::is_disabled")]
    pub session_metrics: SessionMetricsConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_metrics: Option<ErrorBoundary<TransactionMetricsConfig>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub metric_conditional_tagging: Vec<TaggingRule>,
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub span_attributes: BTreeSet<SpanAttribute>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurements: Option<MeasurementsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns_v2: Option<BreakdownsConfig>,
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    pub features: BTreeSet<Feature>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tx_name_rules: Vec<TransactionNameRule>,
}
