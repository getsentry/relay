use relay_base_schema::data_category::DataCategory;
use relay_common::glob2::LazyGlob;
use relay_event_normalization::utils::MAX_DURATION_MOBILE_MS;
use relay_protocol::RuleCondition;
use serde_json::Number;

use crate::feature::Feature;
use crate::metrics::{MetricExtractionConfig, MetricSpec, TagMapping, TagSpec};
use crate::project::ProjectConfig;
use crate::Tag;

/// A list of `span.op` patterns that indicate databases that should be skipped.
const DISABLED_DATABASES: &[&str] = &[
    "*clickhouse*",
    "*compile*",
    "*mongodb*",
    "*redis*",
    "db.orm",
];

/// A list of span.op` patterns we want to enable for mobile.
const MOBILE_OPS: &[&str] = &["app.*", "ui.load*"];

/// A list of patterns found in MongoDB queries
const MONGODB_QUERIES: &[&str] = &["*\"$*", "{*", "*({*", "*[{*"];

/// A list of patterns for resource span ops we'd like to ingest.
const RESOURCE_SPAN_OPS: &[&str] = &["resource.script", "resource.css"];

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
    let (span_op_conditions, resource_condition) = if project_config
        .features
        .has(Feature::SpanMetricsExtractionAllModules)
    {
        (
            RuleCondition::all(),
            RuleCondition::glob("span.op", "resource.*"),
        )
    } else {
        let is_disabled = RuleCondition::glob("span.op", DISABLED_DATABASES);
        let is_mongo = RuleCondition::eq("span.system", "mongodb")
            | RuleCondition::glob("span.description", MONGODB_QUERIES);
        let resource_condition = RuleCondition::glob("span.op", RESOURCE_SPAN_OPS);

        (
            RuleCondition::eq("span.op", "http.client")
                | RuleCondition::glob("span.op", MOBILE_OPS)
                | (RuleCondition::glob("span.op", "db*") & !is_disabled & !is_mongo)
                | resource_condition.clone(),
            resource_condition,
        )
    };

    // For mobile spans, only extract duration metrics when they are below a threshold.
    let duration_condition = RuleCondition::negate(RuleCondition::glob("span.op", MOBILE_OPS))
        | RuleCondition::lte(
            "span.exclusive_time",
            Number::from_f64(MAX_DURATION_MOBILE_MS).unwrap_or(0.into()),
        );

    config.metrics.extend([
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: Some(span_op_conditions.clone() & duration_condition.clone()),
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
            condition: Some(span_op_conditions.clone() & duration_condition.clone()),
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
            mri: "d:spans/http.decoded_response_content_length@byte".into(),
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
        MetricSpec {
            category: DataCategory::Span,
            mri: "c:spans/count_per_op@none".into(),
            field: None,
            condition: Some(duration_condition.clone()),
            tags: ["category", "op", "system"]
                .map(|key| TagSpec {
                    key: format!("span.{key}"),
                    field: Some(format!("span.sentry_tags.{key}")),
                    value: None,
                    condition: None,
                })
                .into(),
        },
    ]);

    config.tags.extend([
        TagMapping {
            metrics: vec![LazyGlob::new("d:spans/exclusive_time*@millisecond".into())],
            tags: [
                ("", "environment"),
                ("", "transaction.method"),
                ("", "transaction.op"),
                ("span.", "action"),
                ("span.", "category"),
                ("span.", "description"),
                ("span.", "domain"),
                ("span.", "group"),
                ("span.", "module"),
                ("span.", "op"),
                ("span.", "status_code"),
                ("span.", "system"),
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
        // Mobile-specific tags:
        TagMapping {
            metrics: vec![LazyGlob::new("d:spans/exclusive_time*@millisecond".into())],
            tags: [
                ("", "device.class"),
                ("", "release"),
                ("", "ttfd"),
                ("", "ttid"),
                ("span.", "main_thread"),
            ]
            .map(|(prefix, key)| TagSpec {
                key: format!("{prefix}{key}"),
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
                LazyGlob::new("d:spans/http.decoded_response_content_length@byte".into()),
                LazyGlob::new("d:spans/http.response_transfer_size@byte".into()),
            ],
            tags: [
                ("", "environment"),
                ("", "file_extension"),
                ("", "resource.render_blocking_status"),
                ("", "transaction"),
                ("span.", "description"),
                ("span.", "domain"),
                ("span.", "group"),
                ("span.", "op"),
            ]
            .map(|(prefix, key)| TagSpec {
                key: format!("{prefix}{key}"),
                field: Some(format!("span.sentry_tags.{}", key)),
                value: None,
                condition: Some(resource_condition.clone()),
            })
            .into(),
        },
        TagMapping {
            metrics: vec![LazyGlob::new("d:spans/exclusive_time*@millisecond".into())],
            tags: [
                ("", "file_extension"),
                ("", "resource.render_blocking_status"),
            ]
            .map(|(prefix, key)| TagSpec {
                key: format!("{prefix}{key}"),
                field: Some(format!("span.sentry_tags.{}", key)),
                value: None,
                condition: Some(resource_condition.clone()),
            })
            .into(),
        },
    ]);

    let module_enabled = RuleCondition::never(); // TODO
    let is_mobile = module_enabled & RuleCondition::never(); // TODO
    config.metrics.extend([
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time_light@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: Some(module_enabled),
            tags: vec![
                Tag::with_key("environment")
                    .from_field("sentry_tags.environment")
                    .when(module_enabled),
                Tag::with_key("transaction.method")
                    .from_field("sentry_tags.transaction.method")
                    .when(module_enabled),
                Tag::with_key("transaction.op")
                    .from_field("sentry_tags.transaction.op")
                    .when(module_enabled),
                Tag::with_key("span.action")
                    .from_field("sentry_tags.action")
                    .when(module_enabled),
                Tag::with_key("span.category")
                    .from_field("sentry_tags.category")
                    .when(module_enabled),
                Tag::with_key("span.description")
                    .from_field("sentry_tags.description")
                    .when(module_enabled),
                Tag::with_key("span.domain")
                    .from_field("sentry_tags.domain")
                    .when(module_enabled),
                Tag::with_key("span.group")
                    .from_field("sentry_tags.group")
                    .when(module_enabled),
                Tag::with_key("span.module")
                    .from_field("sentry_tags.module")
                    .when(module_enabled),
                Tag::with_key("span.op")
                    .from_field("sentry_tags.op")
                    .when(module_enabled),
                Tag::with_key("span.status_code")
                    .from_field("sentry_tags.status_code")
                    .when(module_enabled),
                Tag::with_key("span.system")
                    .from_field("sentry_tags.system")
                    .when(module_enabled),
                // Mobile:
                Tag::with_key("device.class")
                    .from_field("span.sentry_tags.device.class")
                    .when(is_mobile),
                Tag::with_key("release")
                    .from_field("span.sentry_tags.release")
                    .when(is_mobile),
                Tag::with_key("span.main_thread")
                    .from_field("span.sentry_tags.main_thread")
                    .when(is_mobile),
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: Some(module_enabled),
            tags: vec![
                // Common tags:
                Tag::with_key("environment")
                    .from_field("span.sentry_tags.environment")
                    .when(module_enabled),
                Tag::with_key("transaction.method")
                    .from_field("span.sentry_tags.transaction.method")
                    .when(module_enabled),
                Tag::with_key("transaction.op")
                    .from_field("span.sentry_tags.transaction.op")
                    .when(module_enabled),
                Tag::with_key("span.action")
                    .from_field("span.sentry_tags.action")
                    .when(module_enabled),
                Tag::with_key("span.category")
                    .from_field("span.sentry_tags.category")
                    .when(module_enabled),
                Tag::with_key("span.description")
                    .from_field("span.sentry_tags.description")
                    .when(module_enabled),
                Tag::with_key("span.domain")
                    .from_field("span.sentry_tags.domain")
                    .when(module_enabled),
                Tag::with_key("span.group")
                    .from_field("span.sentry_tags.group")
                    .when(module_enabled),
                Tag::with_key("span.module")
                    .from_field("span.sentry_tags.module")
                    .when(module_enabled),
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .when(module_enabled),
                Tag::with_key("span.status_code")
                    .from_field("span.sentry_tags.status_code")
                    .when(module_enabled),
                Tag::with_key("span.system")
                    .from_field("span.sentry_tags.system")
                    .when(module_enabled),
                Tag::with_key("transaction")
                    .from_field("span.sentry_tags.transaction")
                    .when(module_enabled),
                // Mobile:
                Tag::with_key("device.class")
                    .from_field("span.sentry_tags.device.class")
                    .when(is_mobile),
                Tag::with_key("release")
                    .from_field("span.sentry_tags.release")
                    .when(is_mobile),
                Tag::with_key("ttfd")
                    .from_field("span.sentry_tags.ttfd")
                    .when(is_mobile),
                Tag::with_key("ttid")
                    .from_field("span.sentry_tags.ttid")
                    .when(is_mobile),
                Tag::with_key("span.main_thread")
                    .from_field("span.sentry_tags.main_thread")
                    .when(is_mobile),
            ],
        },
    ]);

    config._span_metrics_extended = true;
    if config.version == 0 {
        config.version = MetricExtractionConfig::VERSION;
    }
}
