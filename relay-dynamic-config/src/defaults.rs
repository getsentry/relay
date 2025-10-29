use relay_base_schema::data_category::DataCategory;
use relay_protocol::RuleCondition;

use crate::metrics::MetricSpec;
use crate::{GroupKey, MetricExtractionConfig, ProjectConfig, Tag, TagMapping};

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

    // If there are any spans in the system, extract the usage metric for them:
    config.metrics.push(MetricSpec {
        category: DataCategory::Span,
        mri: "c:spans/usage@none".into(),
        field: None,
        condition: None,
        tags: vec![],
    });

    config
        .global_groups
        .entry(GroupKey::SpanMetricsCommon)
        .or_default()
        .is_enabled = true;
    config
        .global_groups
        .entry(GroupKey::SpanMetricsTx)
        .or_default()
        .is_enabled = true;

    if config.version == 0 {
        config.version = MetricExtractionConfig::MAX_SUPPORTED_VERSION;
    }
}

/// Configuration for extracting metrics from spans.
///
/// These metrics are added to [`crate::GlobalConfig`] by the service and enabled
/// by project configs in sentry.
pub fn hardcoded_span_metrics() -> Vec<(GroupKey, Vec<MetricSpec>, Vec<TagMapping>)> {
    let is_allowed_browser = RuleCondition::eq(
        "span.sentry_tags.browser.name",
        vec![
            "Google Chrome",
            "Chrome",
            "Firefox",
            "Safari",
            "Edge",
            "Opera",
            // Mobile Browsers
            "Chrome Mobile",
            "Firefox Mobile",
            "Mobile Safari",
            "Edge Mobile",
            "Opera Mobile",
        ],
    );
    vec![
        (
            GroupKey::SpanMetricsCommon,
            vec![MetricSpec {
                category: DataCategory::Span,
                mri: "d:spans/webvital.inp@millisecond".into(),
                field: Some("span.measurements.inp.value".into()),
                condition: Some(is_allowed_browser.clone()),
                tags: vec![
                    Tag::with_key("span.op")
                        .from_field("span.sentry_tags.op")
                        .always(),
                    Tag::with_key("transaction")
                        .from_field("span.sentry_tags.transaction")
                        .always(),
                    Tag::with_key("environment")
                        .from_field("span.sentry_tags.environment")
                        .always(),
                    Tag::with_key("release")
                        .from_field("span.sentry_tags.release")
                        .always(),
                    Tag::with_key("browser.name")
                        .from_field("span.sentry_tags.browser.name")
                        .always(), // already guarded by condition on metric
                    Tag::with_key("user.geo.subregion")
                        .from_field("span.sentry_tags.user.geo.subregion")
                        .always(), // already guarded by condition on metric
                ],
            }],
            vec![],
        ),
        (
            GroupKey::SpanMetricsTx,
            vec![
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:transactions/measurements.score.total@ratio".into(),
                    field: Some("span.measurements.score.total.value".into()),
                    condition: Some(
                        // If transactions are extracted from spans, the transaction processing pipeline
                        // will take care of this metric.
                        is_allowed_browser.clone()
                            & RuleCondition::eq("span.was_transaction", false),
                    ),
                    tags: vec![
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("transaction.op")
                            .from_field("span.sentry_tags.transaction.op")
                            .always(),
                        Tag::with_key("transaction")
                            .from_field("span.sentry_tags.transaction")
                            .always(),
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("release")
                            .from_field("span.sentry_tags.release")
                            .always(),
                        Tag::with_key("browser.name")
                            .from_field("span.browser.name")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("user.geo.subregion")
                            .from_field("span.sentry_tags.user.geo.subregion")
                            .always(), // already guarded by condition on metric
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:transactions/measurements.score.cls@ratio".into(),
                    field: Some("span.measurements.score.cls.value".into()),
                    condition: Some(is_allowed_browser.clone()),
                    tags: vec![
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("transaction")
                            .from_field("span.sentry_tags.transaction")
                            .always(),
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("release")
                            .from_field("span.sentry_tags.release")
                            .always(),
                        Tag::with_key("browser.name")
                            .from_field("span.sentry_tags.browser.name")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("user.geo.subregion")
                            .from_field("span.sentry_tags.user.geo.subregion")
                            .always(), // already guarded by condition on metric
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:transactions/measurements.score.weight.cls@ratio".into(),
                    field: Some("span.measurements.score.weight.cls.value".into()),
                    condition: Some(is_allowed_browser.clone()),
                    tags: vec![
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("transaction")
                            .from_field("span.sentry_tags.transaction")
                            .always(),
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("release")
                            .from_field("span.sentry_tags.release")
                            .always(),
                        Tag::with_key("browser.name")
                            .from_field("span.sentry_tags.browser.name")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("user.geo.subregion")
                            .from_field("span.sentry_tags.user.geo.subregion")
                            .always(), // already guarded by condition on metric
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:transactions/measurements.cls@none".into(),
                    field: Some("span.measurements.cls.value".into()),
                    condition: Some(is_allowed_browser.clone()),
                    tags: vec![
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("transaction")
                            .from_field("span.sentry_tags.transaction")
                            .always(),
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("release")
                            .from_field("span.sentry_tags.release")
                            .always(),
                        Tag::with_key("browser.name")
                            .from_field("span.sentry_tags.browser.name")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("user.geo.subregion")
                            .from_field("span.sentry_tags.user.geo.subregion")
                            .always(), // already guarded by condition on metric
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:transactions/measurements.score.lcp@ratio".into(),
                    field: Some("span.measurements.score.lcp.value".into()),
                    condition: Some(is_allowed_browser.clone()),
                    tags: vec![
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("transaction")
                            .from_field("span.sentry_tags.transaction")
                            .always(),
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("release")
                            .from_field("span.sentry_tags.release")
                            .always(),
                        Tag::with_key("browser.name")
                            .from_field("span.sentry_tags.browser.name")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("user.geo.subregion")
                            .from_field("span.sentry_tags.user.geo.subregion")
                            .always(), // already guarded by condition on metric
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:transactions/measurements.score.weight.lcp@ratio".into(),
                    field: Some("span.measurements.score.weight.lcp.value".into()),
                    condition: Some(is_allowed_browser.clone()),
                    tags: vec![
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("transaction")
                            .from_field("span.sentry_tags.transaction")
                            .always(),
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("release")
                            .from_field("span.sentry_tags.release")
                            .always(),
                        Tag::with_key("browser.name")
                            .from_field("span.sentry_tags.browser.name")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("user.geo.subregion")
                            .from_field("span.sentry_tags.user.geo.subregion")
                            .always(), // already guarded by condition on metric
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:transactions/measurements.lcp@millisecond".into(),
                    field: Some("span.measurements.lcp.value".into()),
                    condition: Some(is_allowed_browser.clone()),
                    tags: vec![
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("transaction")
                            .from_field("span.sentry_tags.transaction")
                            .always(),
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("release")
                            .from_field("span.sentry_tags.release")
                            .always(),
                        Tag::with_key("browser.name")
                            .from_field("span.sentry_tags.browser.name")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("user.geo.subregion")
                            .from_field("span.sentry_tags.user.geo.subregion")
                            .always(), // already guarded by condition on metric
                    ],
                },
            ],
            vec![],
        ),
    ]
}
