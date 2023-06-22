use std::collections::BTreeSet;
use std::sync::Arc;

use relay_auth::PublicKey;
use relay_filter::FiltersConfig;
use relay_general::pii::{DataScrubbingConfig, PiiConfig};
use relay_general::store::{
    BreakdownsConfig, MeasurementsConfig, SpanDescriptionRule, TransactionNameRule,
};
use relay_general::types::SpanAttribute;
use relay_quotas::Quota;
use relay_sampling::SamplingConfig;
use serde_json::Value;

use crate::{
    ErrorBoundary, Feature, ProjectConfig, SessionMetricsConfig, TaggingRule,
    TransactionMetricsConfig,
};

pub struct CombinedConfig {
    global: Arc<ProjectConfig>,
    organization: Arc<ProjectConfig>,
    project: Arc<ProjectConfig>,
    key: ProjectConfig,
}

impl CombinedConfig {
    pub fn allowed_domains(&self) -> impl Iterator<Item = &str> {
        self.configs()
            .flat_map(|c| c.allowed_domains.iter().map(|s| s.as_str()))
    }

    pub fn trusted_relays(&self) -> impl Iterator<Item = &PublicKey> {
        self.configs().flat_map(|c| c.trusted_relays.iter())
    }

    pub fn pii_config(&self) -> Option<PiiConfig> {
        todo!()
    }

    /// The grouping configuration.
    pub fn grouping_config(&self) -> &Option<Value> {
        todo!()
    }

    /// Configuration for filter rules.
    pub fn filter_settings(&self) -> &FiltersConfig {
        todo!()
    }

    /// Configuration for data scrubbers.
    pub fn datascrubbing_settings(&self) -> &DataScrubbingConfig {
        todo!()
    }

    /// Maximum event retention for the organization.
    pub fn event_retention(&self) -> &Option<u16> {
        todo!()
    }

    /// Usage quotas for this project.
    pub fn quotas(&self) -> impl Iterator<Item = &Quota> {
        self.configs().flat_map(|c| c.quotas.iter())
    }

    /// Configuration for sampling traces, if not present there will be no sampling.
    pub fn dynamic_sampling(&self) -> &Option<SamplingConfig> {
        todo!()
    }

    /// Configuration for measurements.
    pub fn measurements(&self) -> &Option<MeasurementsConfig> {
        todo!()
    }

    /// Configuration for operation breakdown. Will be emitted only if present.
    pub fn breakdowns_v2(&self) -> &Option<BreakdownsConfig> {
        todo!()
    }

    /// Configuration for extracting metrics from sessions.
    pub fn session_metrics(&self) -> &SessionMetricsConfig {
        todo!()
    }

    /// Configuration for extracting metrics from transaction events.
    pub fn transaction_metrics(&self) -> &Option<ErrorBoundary<TransactionMetricsConfig>> {
        todo!()
    }

    /// The span attributes configuration.
    pub fn span_attributes(&self) -> BTreeSet<&SpanAttribute> {
        // Generate new set every time to guarantee uniqueness:
        BTreeSet::from_iter(self.configs().flat_map(|c| c.span_attributes.iter()))
    }

    /// Rules for applying metrics tags depending on the event's content.
    pub fn metric_conditional_tagging(&self) -> &Vec<TaggingRule> {
        todo!()
    }

    /// Exposable features enabled for this project.
    pub fn features(&self) -> BTreeSet<&Feature> {
        BTreeSet::from_iter(self.configs().flat_map(|c| c.features.iter()))
    }

    /// Transaction renaming rules.
    pub fn tx_name_rules(&self) -> &Vec<TransactionNameRule> {
        todo!()
    }

    /// Whether or not a project is ready to mark all URL transactions as "sanitized".
    pub fn tx_name_ready(&self) -> &bool {
        todo!()
    }

    /// Span description renaming rules.
    pub fn span_description_rules(&self) -> &Option<Vec<SpanDescriptionRule>> {
        todo!()
    }

    fn configs(&self) -> std::array::IntoIter<&ProjectConfig, 4> {
        [
            self.global.as_ref(),
            self.organization.as_ref(),
            self.project.as_ref(),
            &self.key,
        ]
        .into_iter()
    }
}
