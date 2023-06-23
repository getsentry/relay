use std::collections::BTreeSet;

use itertools::Itertools;
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
    TransactionMetricsConfig, DEFAULT_ALLOWED_DOMAINS,
};

pub struct MergedConfig {
    global: ProjectConfig,
    organization: ProjectConfig,
    project: ProjectConfig,
    public_key: ProjectConfig,
}

impl MergedConfig {
    pub fn allowed_domains(&self) -> impl Iterator<Item = &str> {
        // TODO: double-check that overwriting is the behavior that we want.
        let config = [&self.public_key, &self.project, &self.organization]
            .into_iter()
            .find(|slice| slice.allowed_domains.as_slice() != DEFAULT_ALLOWED_DOMAINS)
            .unwrap_or(&self.global);

        config.allowed_domains.iter().map(String::as_str)
    }

    pub fn trusted_relays(&self) -> impl Iterator<Item = &PublicKey> {
        self.all_slices()
            .flat_map(|c| c.trusted_relays.iter())
            .unique()
    }

    pub fn pii_config(&self) -> Option<PiiConfig> {
        todo!()
    }

    /// The grouping configuration.
    pub fn grouping_config(&self) -> &Option<Value> {
        // Grouping config is opaque so we cannot merge it easily.
        // Assume that grouping will be per-project for the foreseeable future.
        // See https://github.com/getsentry/sentry/blob/254cfc0bd2f13dd794ea5bce43c0f77c217eecda/src/sentry/relay/config/__init__.py#L407-L409.
        &self.project.grouping_config
    }

    /// Configuration for filter rules.
    pub fn filter_settings(&self) -> &FiltersConfig {
        // To decide:
        // Do we want to define e.g.
        //   browser_extensions.is_enabled := any(scope.browser_extensions.is_enabled)
        // or make it a tri-state per scope and let lower levels override higher levels?
        todo!()
    }

    /// Configuration for data scrubbers.
    pub fn datascrubbing_settings(&self) -> &DataScrubbingConfig {
        todo!()
    }

    /// Maximum event retention for the organization.
    pub fn event_retention(&self) -> Option<u16> {
        todo!()
    }

    /// Usage quotas for this project.
    pub fn quotas(&self) -> impl Iterator<Item = &Quota> {
        // TODO: Verify if order matters.
        self.all_slices().flat_map(|c| c.quotas.iter())
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
        BTreeSet::from_iter(self.all_slices().flat_map(|c| c.span_attributes.iter()))
    }

    /// Rules for applying metrics tags depending on the event's content.
    pub fn metric_conditional_tagging(&self) -> &Vec<TaggingRule> {
        todo!()
    }

    /// Exposable features enabled for this project.
    pub fn features(&self) -> BTreeSet<&Feature> {
        BTreeSet::from_iter(self.all_slices().flat_map(|c| c.features.iter()))
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

    fn all_slices(&self) -> std::array::IntoIter<&ProjectConfig, 4> {
        // TODO: name this function to make clear it goes from global to local scope.
        [
            &self.global,
            &self.organization,
            &self.project,
            &self.public_key,
        ]
        .into_iter()
    }
}
