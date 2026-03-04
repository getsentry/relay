use relay_base_schema::data_category::DataCategory;
use relay_protocol::RuleCondition;

use crate::metrics::MetricSpec;
use crate::{MetricExtractionConfig, ProjectConfig, Tag};

/// Adds span metric extraction rules to the project config.
///
/// This pushes the usage metric spec into the generic metric extraction config
/// so that downstream Relays (which still use generic extraction) can extract it.
/// The spec also keeps the config in an "enabled" state for `is_enabled()` checks
/// that gate metric creation in standalone span processing.
pub fn add_span_metrics(project_config: &mut ProjectConfig) {
    let config = project_config
        .metric_extraction
        .get_or_insert_with(MetricExtractionConfig::empty);

    if !config.is_supported() || config._span_metrics_extended {
        return;
    }
    config._span_metrics_extended = true;

    config.metrics.push(MetricSpec {
        category: DataCategory::Span,
        mri: "c:spans/usage@none".into(),
        field: None,
        condition: None,
        tags: vec![
            Tag::with_key("is_segment")
                .with_value("true")
                .when(RuleCondition::eq("span.is_segment", true)),
            Tag::with_key("was_transaction").with_value("true").when(
                RuleCondition::eq("span.is_segment", true)
                    .and(RuleCondition::eq("span.was_transaction", true)),
            ),
            Tag::with_key("is_segment").with_value("false").always(),
        ],
    });

    if config.version == 0 {
        config.version = MetricExtractionConfig::MAX_SUPPORTED_VERSION;
    }
}
