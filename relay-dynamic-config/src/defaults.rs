use relay_base_schema::data_category::DataCategory;
use relay_protocol::RuleCondition;

use crate::metrics::MetricSpec;
use crate::{MetricExtractionConfig, ProjectConfig, Tag};

/// Conditionally enables metrics extraction groups from the global config.
///
/// Depending on feature flags, groups are either enabled or not.
pub fn add_span_metrics(project_config: &mut ProjectConfig) {
    let config = project_config
        .metric_extraction
        .get_or_insert_with(MetricExtractionConfig::empty);

    if !config.is_supported() || config._span_metrics_extended {
        return;
    }
    config._span_metrics_extended = true;

    // If there are any spans in the system, extract the usage metric for them.
    //
    // The metric is always tagged with `is_segment` (`true`/`false`) and additionally
    // segment spans are gain the additional tag `was_transaction` if the segment span was created
    // from a transaction.
    config.metrics.push(MetricSpec {
        category: DataCategory::Span,
        mri: "c:spans/usage@none".into(),
        field: None,
        condition: None,
        tags: vec![
            Tag::with_key("is_segment")
                .with_value("true")
                .when(RuleCondition::eq("span.is_segment", true)),
            // Only tag transaction status for segment spans.
            Tag::with_key("was_transaction").with_value("true").when(
                RuleCondition::eq("span.is_segment", true)
                    .and(RuleCondition::eq("span.was_transaction", true)),
            ),
            // Fallback, for all non segment spans.
            Tag::with_key("is_segment").with_value("false").always(),
        ],
    });

    if config.version == 0 {
        config.version = MetricExtractionConfig::MAX_SUPPORTED_VERSION;
    }
}
