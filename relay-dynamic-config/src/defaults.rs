use relay_base_schema::data_category::DataCategory;
use relay_common::glob2::LazyGlob;
use relay_protocol::RuleCondition;

use crate::feature::Feature;
use crate::metrics::{MetricExtractionConfig, MetricSpec, TagMapping, TagSpec};
use crate::project::ProjectConfig;

/// A list of `span.op` patterns that indicate databases that should be skipped.
const DISABLED_DATABASES: &[&str] = &["*clickhouse*", "*mongodb*", "*redis*", "*compiler*"];

/// A list of span.op` patterns we want to enable for mobile.
const MOBILE_OPS: &[&str] = &["app.*", "ui.load*"];

/// A list of patterns found in MongoDB queries
const MONGODB_QUERIES: &[&str] = &["*\"$*", "{*", "*({*", "*[{*"];

/// A list of patterns for resource span ops we'd like to ingest.
const RESOURCE_SPAN_OPS: &[&str] = &["resource.script", "resource.css", "resource.link"];

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

    let resource_condition = RuleCondition::glob("span.op", RESOURCE_SPAN_OPS);

    // Add conditions to filter spans if a specific module is enabled.
    // By default, this will extract all spans.
    let span_op_conditions = if project_config
        .features
        .has(Feature::SpanMetricsExtractionAllModules)
    {
        RuleCondition::all()
    } else {
        let is_disabled = RuleCondition::glob("span.op", DISABLED_DATABASES);
        let is_mongo = RuleCondition::eq("span.system", "mongodb")
            | RuleCondition::glob("span.description", MONGODB_QUERIES);

        let mut conditions = RuleCondition::eq("span.op", "http.client")
            | RuleCondition::glob("span.op", MOBILE_OPS)
            | (RuleCondition::glob("span.op", "db*") & !is_disabled & !is_mongo);

        if project_config
            .features
            .has(Feature::SpanMetricsExtractionResource)
        {
            conditions = conditions | resource_condition.clone();
        }

        conditions
    };

    config.metrics.extend([
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: Some(span_op_conditions.clone()),
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
            condition: Some(span_op_conditions.clone()),
            tags: Default::default(),
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/http.response_content_length@byte".into(),
            field: Some("span.data.http\\.response_content_length".into()),
            condition: Some(
                span_op_conditions.clone()
                    & resource_condition.clone()
                    & RuleCondition::gt("span.data.http\\.response_content_length", 0),
            ),
            tags: Default::default(),
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/http.decoded_response_body_length@byte".into(),
            field: Some("span.data.http\\.decoded_response_body_length".into()),
            condition: Some(
                span_op_conditions.clone()
                    & resource_condition.clone()
                    & RuleCondition::gt("span.data.http\\.decoded_response_body_length", 0),
            ),
            tags: Default::default(),
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/http.decoded_response_body_length@byte".into(),
            field: Some("span.data.http\\.decoded_response_content_length".into()),
            condition: Some(
                span_op_conditions.clone()
                    & resource_condition.clone()
                    & RuleCondition::gt("span.data.http\\.decoded_response_content_length", 0),
            ),
            tags: Default::default(),
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/http.response_transfer_size@byte".into(),
            field: Some("span.data.http\\.response_transfer_size".into()),
            condition: Some(
                span_op_conditions.clone()
                    & resource_condition.clone()
                    & RuleCondition::gt("span.data.http\\.response_transfer_size", 0),
            ),
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
                ("", "resource.render_blocking_status"), // only set for resource spans.
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
        // Resource-specific tags:
        TagMapping {
            metrics: vec![
                LazyGlob::new("d:spans/http.response_content_length@byte".into()),
                LazyGlob::new("d:spans/http.decoded_response_body_length@byte".into()),
                LazyGlob::new("d:spans/http.response_transfer_size@byte".into()),
            ],
            tags: [
                ("", "environment"),
                ("span.", "description"),
                ("span.", "domain"),
                ("span.", "group"),
                ("span.", "op"),
                ("", "transaction"),
                ("", "resource.render_blocking_status"),
            ]
            .map(|(prefix, key)| TagSpec {
                key: format!("{prefix}{key}"),
                field: Some(format!("span.sentry_tags.{}", key)),
                value: None,
                condition: None,
            })
            .into(),
        },
    ]);

    config._span_metrics_extended = true;
    if config.version == 0 {
        config.version = MetricExtractionConfig::VERSION;
    }
}
