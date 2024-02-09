use relay_auth::PublicKey;
use relay_event_normalization::{
    BreakdownsConfig, MeasurementsConfig, PerformanceScoreConfig, SpanDescriptionRule,
    TransactionNameRule,
};
use relay_filter::FiltersConfig;
use relay_pii::{DataScrubbingConfig, PiiConfig};
use relay_quotas::Quota;
use relay_sampling::SamplingConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::defaults;
use crate::error_boundary::ErrorBoundary;
use crate::feature::FeatureSet;
use crate::metrics::{
    self, MetricExtractionConfig, Metrics, SessionMetricsConfig, TaggingRule,
    TransactionMetricsConfig,
};

/// Dynamic, per-DSN configuration passed down from Sentry.
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
    #[serde(alias = "dynamicSampling", skip_serializing_if = "Option::is_none")]
    pub sampling: Option<ErrorBoundary<SamplingConfig>>,
    /// Configuration for measurements.
    /// NOTE: do not access directly, use [`relay_event_normalization::DynamicMeasurementsConfig`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurements: Option<MeasurementsConfig>,
    /// Configuration for operation breakdown. Will be emitted only if present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns_v2: Option<BreakdownsConfig>,
    /// Configuration for performance score calculations. Will be emitted only if present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub performance_score: Option<PerformanceScoreConfig>,
    /// Configuration for extracting metrics from sessions.
    #[serde(skip_serializing_if = "SessionMetricsConfig::is_disabled")]
    pub session_metrics: SessionMetricsConfig,
    /// Configuration for extracting metrics from transaction events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_metrics: Option<ErrorBoundary<TransactionMetricsConfig>>,
    /// Configuration for generic metrics extraction from all data categories.
    #[serde(default, skip_serializing_if = "skip_metrics_extraction")]
    pub metric_extraction: ErrorBoundary<MetricExtractionConfig>,
    /// Rules for applying metrics tags depending on the event's content.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub metric_conditional_tagging: Vec<TaggingRule>,
    /// Exposable features enabled for this project.
    #[serde(skip_serializing_if = "FeatureSet::is_empty")]
    pub features: FeatureSet,
    /// Transaction renaming rules.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tx_name_rules: Vec<TransactionNameRule>,
    /// Whether or not a project is ready to mark all URL transactions as "sanitized".
    #[serde(skip_serializing_if = "is_false")]
    pub tx_name_ready: bool,
    /// Span description renaming rules.
    ///
    /// These are currently not used by Relay, and only here to be forwarded to old
    /// relays that might still need them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_description_rules: Option<Vec<SpanDescriptionRule>>,
    /// Configuration for metrics.
    #[serde(default, skip_serializing_if = "skip_metrics")]
    pub metrics: ErrorBoundary<Metrics>,
}

impl ProjectConfig {
    /// Validates fields in this project config and removes values that are partially invalid.
    pub fn sanitize(&mut self) {
        self.quotas.retain(Quota::is_valid);

        metrics::convert_conditional_tagging(self);
        defaults::add_span_metrics(self);

        if let Some(ErrorBoundary::Ok(ref mut sampling_config)) = self.sampling {
            sampling_config.normalize();
        }
    }
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
            sampling: None,
            measurements: None,
            breakdowns_v2: None,
            performance_score: Default::default(),
            session_metrics: SessionMetricsConfig::default(),
            transaction_metrics: None,
            metric_extraction: Default::default(),
            metric_conditional_tagging: Vec::new(),
            features: Default::default(),
            tx_name_rules: Vec::new(),
            tx_name_ready: false,
            span_description_rules: None,
            metrics: Default::default(),
        }
    }
}

fn skip_metrics_extraction(boundary: &ErrorBoundary<MetricExtractionConfig>) -> bool {
    match boundary {
        ErrorBoundary::Err(_) => true,
        ErrorBoundary::Ok(config) => !config.is_enabled(),
    }
}

fn skip_metrics(boundary: &ErrorBoundary<Metrics>) -> bool {
    match boundary {
        ErrorBoundary::Err(_) => true,
        ErrorBoundary::Ok(metrics) => metrics.is_empty(),
    }
}

/// Subset of [`ProjectConfig`] that is passed to external Relays.
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
    pub sampling: Option<ErrorBoundary<SamplingConfig>>,
    #[serde(skip_serializing_if = "SessionMetricsConfig::is_disabled")]
    pub session_metrics: SessionMetricsConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_metrics: Option<ErrorBoundary<TransactionMetricsConfig>>,
    #[serde(default, skip_serializing_if = "skip_metrics_extraction")]
    pub metric_extraction: ErrorBoundary<MetricExtractionConfig>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub metric_conditional_tagging: Vec<TaggingRule>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurements: Option<MeasurementsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns_v2: Option<BreakdownsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub performance_score: Option<PerformanceScoreConfig>,
    #[serde(skip_serializing_if = "FeatureSet::is_empty")]
    pub features: FeatureSet,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tx_name_rules: Vec<TransactionNameRule>,
    /// Whether or not a project is ready to mark all URL transactions as "sanitized".
    #[serde(skip_serializing_if = "is_false")]
    pub tx_name_ready: bool,
    /// Span description renaming rules.
    ///
    /// These are currently not used by Relay, and only here to be forwarded to old
    /// relays that might still need them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_description_rules: Option<Vec<SpanDescriptionRule>>,
}

fn is_false(value: &bool) -> bool {
    !*value
}
