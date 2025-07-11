use relay_base_schema::data_category::DataCategory;

use crate::metrics::MetricSpec;
use crate::{MetricExtractionConfig, ProjectConfig};

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

    let features = &project_config.features;

    // If there are any spans in the system, extract the usage metric for them:
    if features.produces_spans() {
        config.metrics.push(MetricSpec {
            category: DataCategory::Span,
            mri: "c:spans/usage@none".into(),
            field: None,
            condition: None,
            tags: vec![],
        });
    }

    config._span_metrics_extended = true;
    if config.version == 0 {
        config.version = MetricExtractionConfig::MAX_SUPPORTED_VERSION;
    }
}
