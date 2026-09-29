use relay_auth::PublicKey;
use relay_event_normalization::{
    BreakdownsConfig, MeasurementsConfig, PerformanceScoreConfig, SpanDescriptionRule,
    TransactionNameRule,
};
use relay_filter::ProjectFiltersConfig;
use relay_pii::{DataScrubbingConfig, PiiConfig};
use relay_quotas::Quota;
use relay_sampling::SamplingConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error_boundary::ErrorBoundary;
use crate::feature::FeatureSet;
use crate::metrics::{self, MetricExtractionConfig, SessionMetricsConfig, TaggingRule};
use crate::trusted_relay::TrustedRelayConfig;
use crate::{GRADUATED_FEATURE_FLAGS, defaults};

/// Dynamic, per-DSN configuration passed down from Sentry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProjectConfig {
    /// URLs that are permitted for cross original JavaScript requests.
    pub allowed_domains: Box<[String]>,
    /// List of relay public keys that are permitted to access this project.
    pub trusted_relays: Box<[PublicKey]>,
    /// Configuration for trusted Relay behaviour.
    #[serde(skip_serializing_if = "TrustedRelayConfig::is_empty")]
    pub trusted_relay_settings: TrustedRelayConfig,
    /// Configuration for PII stripping.
    pub pii_config: Option<PiiConfig>,
    /// The grouping configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grouping_config: Option<Value>,
    /// Configuration for filter rules.
    #[serde(skip_serializing_if = "ProjectFiltersConfig::is_empty")]
    pub filter_settings: ProjectFiltersConfig,
    /// Configuration for data scrubbers.
    #[serde(skip_serializing_if = "DataScrubbingConfig::is_disabled")]
    pub datascrubbing_settings: DataScrubbingConfig,
    /// Maximum event retention for the organization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_retention: Option<u16>,
    /// Maximum sampled event retention for the organization.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub downsampled_event_retention: Option<u16>,
    /// Retention settings for different products.
    #[serde(default, skip_serializing_if = "RetentionsConfig::is_empty")]
    pub retentions: RetentionsConfig,
    /// Trimming settings for different products.
    #[serde(default, skip_serializing_if = "TrimmingConfigs::is_empty")]
    pub trimming: TrimmingConfigs,
    /// Usage quotas for this project.
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub quotas: Box<[Quota]>,
    /// Configuration for sampling traces, if not present there will be no sampling.
    #[serde(alias = "dynamicSampling", skip_serializing_if = "Option::is_none")]
    pub sampling: Option<ErrorBoundary<SamplingConfig>>,
    /// Configuration for measurements.
    /// NOTE: do not access directly, use [`relay_event_normalization::CombinedMeasurementsConfig`].
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
    /// Configuration for generic metrics extraction from all data categories.
    #[serde(default, skip_serializing_if = "skip_metrics_extraction")]
    pub metric_extraction: ErrorBoundary<MetricExtractionConfig>,
    /// Rules for applying metrics tags depending on the event's content.
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub metric_conditional_tagging: Box<[TaggingRule]>,
    /// Exposable features enabled for this project.
    #[serde(skip_serializing_if = "FeatureSet::is_empty")]
    pub features: FeatureSet,
    /// Transaction renaming rules.
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub tx_name_rules: Box<[TransactionNameRule]>,
    /// Whether or not a project is ready to mark all URL transactions as "sanitized".
    #[serde(skip_serializing_if = "is_false")]
    pub tx_name_ready: bool,
    /// Span description renaming rules.
    ///
    /// These are currently not used by Relay, and only here to be forwarded to old
    /// relays that might still need them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_description_rules: Option<Box<[SpanDescriptionRule]>>,
}

impl ProjectConfig {
    /// Validates fields in this project config and removes values that are partially invalid.
    pub fn sanitize(&mut self, report_errors: bool) {
        self.remove_invalid_quotas(report_errors);

        metrics::convert_conditional_tagging(self);
        defaults::add_span_metrics(self);

        if let Some(ErrorBoundary::Ok(ref mut sampling_config)) = self.sampling {
            sampling_config.normalize();
        }

        for flag in GRADUATED_FEATURE_FLAGS {
            self.features.0.insert(*flag);
        }
    }

    fn remove_invalid_quotas(&mut self, report_errors: bool) {
        let mut quotas = std::mem::take(&mut self.quotas).into_vec();
        let invalid_quotas: Vec<_> = quotas.extract_if(.., |q| !q.is_valid()).collect();
        self.quotas = quotas.into_boxed_slice();
        if report_errors {
            if !invalid_quotas.is_empty() {
                {
                    relay_log::warn!(
                        invalid_quotas = ?invalid_quotas,
                        "Found an invalid quota definition",
                    );
                }
            }
            // Check if indexed and non-indexed are double-counting towards the same ID.
            // This is probably not intended behavior.
            for quota in &self.quotas {
                if let Some(id) = quota.id.as_deref() {
                    for category in quota.categories.iter() {
                        if let Some(indexed) = category.index_category()
                            && quota.categories.contains(&indexed)
                        {
                            relay_log::error!(
                                tags.id = id,
                                "Categories {category} and {indexed} share the same quota ID. This will double-count items.",
                            );
                        }
                    }
                }
            }
        }
    }
}

impl Default for ProjectConfig {
    fn default() -> Self {
        ProjectConfig {
            allowed_domains: vec!["*".to_owned()].into_boxed_slice(),
            trusted_relays: Box::new([]),
            trusted_relay_settings: TrustedRelayConfig::default(),
            pii_config: None,
            grouping_config: None,
            filter_settings: ProjectFiltersConfig::default(),
            datascrubbing_settings: DataScrubbingConfig::default(),
            event_retention: None,
            downsampled_event_retention: None,
            retentions: Default::default(),
            trimming: Default::default(),
            quotas: Box::new([]),
            sampling: None,
            measurements: None,
            breakdowns_v2: None,
            performance_score: Default::default(),
            session_metrics: SessionMetricsConfig::default(),
            metric_extraction: Default::default(),
            metric_conditional_tagging: Box::new([]),
            features: Default::default(),
            tx_name_rules: Box::new([]),
            tx_name_ready: false,
            span_description_rules: None,
        }
    }
}

fn skip_metrics_extraction(boundary: &ErrorBoundary<MetricExtractionConfig>) -> bool {
    match boundary {
        ErrorBoundary::Err(_) => true,
        ErrorBoundary::Ok(config) => !config.is_enabled(),
    }
}

/// Subset of [`ProjectConfig`] that is passed to external Relays.
///
/// For documentation of the fields, see [`ProjectConfig`].
#[allow(missing_docs)]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase", remote = "ProjectConfig")]
pub struct LimitedProjectConfig {
    pub allowed_domains: Box<[String]>,
    pub trusted_relays: Box<[PublicKey]>,
    pub pii_config: Option<PiiConfig>,
    #[serde(skip_serializing_if = "ProjectFiltersConfig::is_empty")]
    pub filter_settings: ProjectFiltersConfig,
    #[serde(skip_serializing_if = "DataScrubbingConfig::is_disabled")]
    pub datascrubbing_settings: DataScrubbingConfig,
    #[serde(skip_serializing_if = "TrimmingConfigs::is_empty")]
    pub trimming: TrimmingConfigs,
    #[serde(skip_serializing_if = "SessionMetricsConfig::is_disabled")]
    pub session_metrics: SessionMetricsConfig,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub metric_conditional_tagging: Box<[TaggingRule]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub measurements: Option<MeasurementsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub breakdowns_v2: Option<BreakdownsConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub performance_score: Option<PerformanceScoreConfig>,
    #[serde(skip_serializing_if = "FeatureSet::is_empty")]
    pub features: FeatureSet,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    pub tx_name_rules: Box<[TransactionNameRule]>,
    /// Whether or not a project is ready to mark all URL transactions as "sanitized".
    #[serde(skip_serializing_if = "is_false")]
    pub tx_name_ready: bool,
    /// Span description renaming rules.
    ///
    /// These are currently not used by Relay, and only here to be forwarded to old
    /// relays that might still need them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_description_rules: Option<Box<[SpanDescriptionRule]>>,
}

/// Per-Category settings for retention policy.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct RetentionConfig {
    /// Standard / full fidelity retention policy in days.
    pub standard: u16,
    /// Downsampled retention policy in days.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub downsampled: Option<u16>,
}

/// Settings for retention policy.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RetentionsConfig {
    /// Retention settings for logs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<RetentionConfig>,
    /// Retention settings for spans.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<RetentionConfig>,
    /// Retention settings for metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_metric: Option<RetentionConfig>,
    /// Retention settings for attachments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_attachment: Option<RetentionConfig>,
}

impl RetentionsConfig {
    fn is_empty(&self) -> bool {
        let Self {
            log,
            span,
            trace_metric,
            trace_attachment,
        } = self;

        log.is_none() && span.is_none() && trace_metric.is_none() && trace_attachment.is_none()
    }
}

/// Per-category settings for item trimming.
#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrimmingConfig {
    /// The maximum size in bytes above which an item should be trimmed.
    pub max_size: u32,
}

/// Settings for item trimming.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct TrimmingConfigs {
    /// Trimming settings for spans.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<TrimmingConfig>,
}

impl TrimmingConfigs {
    fn is_empty(&self) -> bool {
        let Self { span } = self;
        span.is_none()
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn graduated_feature_flag_gets_inserted() {
        let mut project_config = ProjectConfig::default();
        for feature in GRADUATED_FEATURE_FLAGS {
            assert!(!project_config.features.has(*feature));
        }

        project_config.sanitize(false);

        for feature in GRADUATED_FEATURE_FLAGS {
            assert!(project_config.features.has(*feature));
        }
    }

    #[test]
    fn sanitize_removes_invalid_quotas() {
        let mut config: ProjectConfig =
            serde_json::from_str(r#"{"quotas":[{"limit":0},{"limit":1},{"limit":0}]}"#).unwrap();

        config.sanitize(false);

        assert_eq!(config.quotas.len(), 2);
        assert!(config.quotas.iter().all(Quota::is_valid));
    }

    #[test]
    fn sanitize_extends_metric_extraction() {
        let mut config: ProjectConfig = serde_json::from_value(serde_json::json!({
            "metricConditionalTagging": [{
                "condition": {"op": "and", "inner": []},
                "targetMetrics": ["c:spans/custom@none"],
                "targetTag": "key",
                "tagValue": "value"
            }],
            "metricExtraction": {
                "version": MetricExtractionConfig::MAX_SUPPORTED_VERSION,
                "metrics": [{"category": "span", "mri": "c:spans/custom@none"}]
            }
        }))
        .unwrap();

        config.sanitize(false);

        let extraction = config.metric_extraction.ok().unwrap();
        assert_eq!(extraction.metrics.len(), 2);
        assert_eq!(extraction.metrics[1].mri, "c:spans/usage@none");
        assert_eq!(extraction.tags.len(), 1);
        assert_eq!(extraction.tags[0].tags[0].key, "key");
    }
}
