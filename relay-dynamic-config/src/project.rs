use std::collections::BTreeSet;

use relay_auth::PublicKey;
use relay_filter::FiltersConfig;
use relay_general::pii::{DataScrubbingConfig, PiiConfig};
use relay_general::store::{
    BreakdownsConfig, MeasurementsConfig, SpanDescriptionRule, TransactionNameRule,
};
use relay_general::types::SpanAttribute;
use relay_quotas::Quota;
use relay_sampling::SamplingConfig;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::feature::Feature;
use crate::{ErrorBoundary, SessionMetricsConfig, TaggingRule, TransactionMetricsConfig};

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
    /// Whether or not a project is ready to mark all URL transactions as "sanitized".
    #[serde(skip_serializing_if = "is_false")]
    pub tx_name_ready: bool,
    /// Span description renaming rules.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_description_rules: Option<Vec<SpanDescriptionRule>>,
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
            tx_name_ready: false,
            span_description_rules: None,
        }
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
    /// Whether or not a project is ready to mark all URL transactions as "sanitized".
    #[serde(skip_serializing_if = "is_false")]
    pub tx_name_ready: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_description_rules: Option<Vec<SpanDescriptionRule>>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

type SparseConfig = ProjectConfig;

#[derive(Debug)]
struct DynamicConfig {
    global: SparseConfig,
    org: SparseConfig,
    project: SparseConfig,
    dsn: SparseConfig,
}

impl From<ProjectConfig> for DynamicConfig {
    fn from(value: ProjectConfig) -> Self {
        Self::split_project_config(value)
    }
}

impl DynamicConfig {
    fn split_project_config(config: ProjectConfig) -> Self {
        let global = ProjectConfig {
            measurements: config.measurements,
            metric_conditional_tagging: config.metric_conditional_tagging, // ?
            ..Default::default()
        };

        let org = ProjectConfig {
            trusted_relays: config.trusted_relays,
            ..Default::default()
        };

        let project = ProjectConfig {
            pii_config: config.pii_config,
            transaction_metrics: config.transaction_metrics,
            span_attributes: config.span_attributes,
            session_metrics: config.session_metrics,
            allowed_domains: config.allowed_domains,
            features: config.features,
            breakdowns_v2: config.breakdowns_v2,
            dynamic_sampling: config.dynamic_sampling,
            datascrubbing_settings: config.datascrubbing_settings,
            filter_settings: config.filter_settings,
            grouping_config: config.grouping_config,
            span_description_rules: config.span_description_rules,
            tx_name_rules: config.tx_name_rules,
            ..Default::default()
        };

        let dsn = ProjectConfig {
            quotas: config.quotas,
            ..Default::default()
        };

        Self {
            global,
            org,
            project,
            dsn,
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_auth::generate_key_pair;

    use super::*;
    use crate::ProjectConfig;

    fn is_equal_dbg<T: std::fmt::Debug, U: std::fmt::Debug>(a: T, b: U) -> bool {
        let a = format!("{a:?}");
        let b = format!("{b:?}");
        a == b
    }

    fn mock_config() -> ProjectConfig {
        ProjectConfig {
            allowed_domains: vec!["foo".to_string(), "bar".to_string()],
            trusted_relays: vec![generate_key_pair().1],
            pii_config: Some(PiiConfig::default()),
            grouping_config: Some(Value::String("hey".into())),
            filter_settings: FiltersConfig::default(),
            datascrubbing_settings: {
                let mut dataconf = DataScrubbingConfig::default();
                dataconf.exclude_fields = vec!["barfoo".into()];
                dataconf
            },
            event_retention: Some(42),
            quotas: vec![],
            dynamic_sampling: None,
            measurements: Some(MeasurementsConfig::default()),
            breakdowns_v2: Some(BreakdownsConfig::default()),
            session_metrics: SessionMetricsConfig::default(),
            transaction_metrics: None,
            span_attributes: {
                let mut set = BTreeSet::new();
                set.insert(SpanAttribute::ExclusiveTime);
                set
            },
            metric_conditional_tagging: vec![],
            features: {
                let mut set = BTreeSet::new();
                set.insert(Feature::SessionReplay);
                set
            },
            tx_name_rules: vec![],
            tx_name_ready: true,
            span_description_rules: Some(vec![]),
        }
    }

    #[test]
    fn test_foo() {
        let config = mock_config();
        let dynamic_config: DynamicConfig = config.clone().into();

        // Checking global
        assert!(is_equal_dbg(
            config.measurements,
            dynamic_config.global.measurements
        ));

        assert!(is_equal_dbg(
            config.metric_conditional_tagging,
            dynamic_config.global.metric_conditional_tagging
        ));

        // Checking org
        assert!(is_equal_dbg(
            config.trusted_relays,
            dynamic_config.org.trusted_relays
        ));

        // Checking project
        assert!(is_equal_dbg(
            config.datascrubbing_settings,
            dynamic_config.project.datascrubbing_settings
        ));

        assert!(is_equal_dbg(
            config.allowed_domains,
            dynamic_config.project.allowed_domains
        ));

        // Checking DSN
        assert!(is_equal_dbg(config.quotas, dynamic_config.dsn.quotas));
    }
}
