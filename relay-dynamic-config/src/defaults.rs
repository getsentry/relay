use relay_base_schema::data_category::DataCategory;
use relay_common::glob2::LazyGlob;
use relay_sampling::condition::RuleCondition;

use crate::feature::Feature;
use crate::metrics::{MetricExtractionConfig, MetricSpec, TagMapping, TagSpec};
use crate::project::ProjectConfig;

/// A list of `span.op` patterns that indicate databases that should be skipped.
const DISABLED_DATABASES: &[&str] = &["*clickhouse*", "*mongodb*", "*redis*", "*compiler*"];

/// A list of span.op` patterns we want to enable for mobile.
const MOBILE_OPS: &[&str] = &["app.*", "ui.load*"];

/// Adds configuration for extracting metrics from spans.
///
/// This configuration is temporarily hard-coded here. It will later be provided by the upstream.
/// This requires the `SpanMetricsExtraction` feature. This feature should be set to `false` if the
/// default should no longer be placed.
pub fn add_span_metrics(project_config: &mut ProjectConfig) {
    if !project_config.features.has(Feature::SpanMetricsExtraction) {
        return;
    }

    let config = project_config
        .metric_extraction
        .get_or_insert_with(MetricExtractionConfig::empty);

    if !config.is_supported() || config._span_metrics_extended {
        return;
    }

    // Add conditions to filter spans if a specific module is enabled.
    // By default, this will extract all spans.
    let span_op_conditions = if project_config
        .features
        .has(Feature::SpanMetricsExtractionAllModules)
    {
        None
    } else {
        let is_mongo = RuleCondition::eq("span.system", "mongodb")
            | RuleCondition::glob("span.description", "*\"$*");

        let condition = RuleCondition::eq("span.op", "http.client")
            | (RuleCondition::glob("span.op", "db*")
                & !RuleCondition::glob("span.op", DISABLED_DATABASES)
                & !(RuleCondition::eq("span.op", "db.sql.query") & is_mongo))
            | RuleCondition::glob("span.op", MOBILE_OPS);

        Some(condition)
    };

    config.metrics.extend([
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: span_op_conditions.clone(),
            tags: vec![TagSpec {
                key: "transaction".into(),
                field: Some("span.sentry_tags.transaction".into()),
                value: None,
                condition: None,
            }],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time_light@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: span_op_conditions,
            tags: Default::default(),
        },
    ]);

    config.tags.extend([
        TagMapping {
            metrics: vec![LazyGlob::new("d:spans/exclusive_time*@millisecond".into())],
            tags: [
                ("", "environment"),
                ("", "http.status_code"),
                ("span.", "action"),
                ("span.", "category"),
                ("span.", "description"),
                ("span.", "domain"),
                ("span.", "group"),
                ("span.", "module"),
                ("span.", "op"),
                ("span.", "status_code"),
                ("span.", "system"),
                ("", "transaction.method"),
                ("", "transaction.op"),
            ]
            .map(|(prefix, key)| TagSpec {
                key: format!("{prefix}{key}"),
                field: Some(format!("span.sentry_tags.{}", key)),
                value: None,
                condition: None,
            })
            .into_iter()
            // Tags taken directly from the span payload:
            .chain(std::iter::once(TagSpec {
                key: "span.status".into(),
                field: Some("span.status".into()),
                value: None,
                condition: None,
            }))
            .collect(),
        },
        TagMapping {
            metrics: vec![LazyGlob::new("d:spans/exclusive_time*@millisecond".into())],
            tags: ["release", "device.class"] // TODO: sentry PR for static strings
                .map(|key| TagSpec {
                    key: key.into(),
                    field: Some(format!("span.sentry_tags.{}", key)),
                    value: None,
                    condition: Some(RuleCondition::eq("span.sentry_tags.mobile", "true")),
                })
                .into(),
        },
        TagMapping {
            metrics: vec![LazyGlob::new("d:spans/exclusive_time*@millisecond".into())],
            tags: [
                ("", "http.decoded_response_body_length"),
                ("", "http.response_content_length"),
                ("", "http.response_transfer_size"),
                ("", "resource.render_blocking_status"),
                ("", "transaction"),
                ("", "type"),
                ("span.", "domain"),
            ]
            .map(|(prefix, key)| TagSpec {
                key: format!("{prefix}{key}"),
                field: Some(format!("span.sentry_tags.{}", key)),
                value: None,
                condition: Some(RuleCondition::glob("span.op", "resource.*")),
            })
            .into(),
        },
    ]);

    config._span_metrics_extended = true;
    if config.version == 0 {
        config.version = MetricExtractionConfig::VERSION;
    }
}
