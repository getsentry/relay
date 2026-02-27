use relay_base_schema::data_category::DataCategory;
use relay_protocol::RuleCondition;

use crate::metrics::MetricSpec;
use crate::{MetricExtractionConfig, ProjectConfig, Tag};

/// Ensures the metric extraction config exists and is enabled.
///
/// This is needed so that downstream checks on `metric_extraction.is_enabled()`
/// continue to gate usage and count_per_root metric creation for standalone spans.
/// The usage metric spec itself is not consumed by generic extraction (which has been
/// removed), but its presence keeps the config in an "enabled" state.
pub fn add_span_metrics(project_config: &mut ProjectConfig) {
    let config = project_config
        .metric_extraction
        .get_or_insert_with(MetricExtractionConfig::empty);

    if !config.is_supported() || config._span_metrics_extended {
        return;
    }
    config._span_metrics_extended = true;

    // Push a metric spec to keep the config non-empty (required for is_enabled()).
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
