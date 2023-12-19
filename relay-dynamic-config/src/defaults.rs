use relay_base_schema::data_category::DataCategory;
use relay_event_normalization::utils::MAX_DURATION_MOBILE_MS;
use relay_protocol::RuleCondition;
use serde_json::Number;

use crate::feature::Feature;
use crate::metrics::{MetricExtractionConfig, MetricSpec};
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
const RESOURCE_SPAN_OPS: &[&str] = &["resource.script", "resource.css", "resource.img"];

/// A list of file span ops we'd like to ingest.
const FILE_OPS: &[&str] = &["file.read", "file.write"];

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
                | RuleCondition::glob("span.op", FILE_OPS)
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
    let mobile_condition = RuleCondition::eq("span.sentry_tags.mobile", "true");

    config.metrics.extend([
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: Some(span_op_conditions.clone() & duration_condition.clone()),
            tags: vec![
                // Common tags:
                Tag::with_key("environment")
                    .from_field("span.sentry_tags.environment")
                    .always(),
                Tag::with_key("transaction.method")
                    .from_field("span.sentry_tags.transaction.method")
                    .always(),
                Tag::with_key("transaction.op")
                    .from_field("span.sentry_tags.transaction.op")
                    .always(),
                Tag::with_key("span.action")
                    .from_field("span.sentry_tags.action")
                    .always(),
                Tag::with_key("span.category")
                    .from_field("span.sentry_tags.category")
                    .always(),
                Tag::with_key("span.description")
                    .from_field("span.sentry_tags.description")
                    .always(),
                Tag::with_key("span.domain")
                    .from_field("span.sentry_tags.domain")
                    .always(),
                Tag::with_key("span.group")
                    .from_field("span.sentry_tags.group")
                    .always(),
                Tag::with_key("span.module")
                    .from_field("span.sentry_tags.module")
                    .always(),
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .always(),
                Tag::with_key("span.status")
                    .from_field("span.status") // from top-level field
                    .always(),
                Tag::with_key("span.status_code")
                    .from_field("span.sentry_tags.status_code")
                    .always(),
                Tag::with_key("span.system")
                    .from_field("span.sentry_tags.system")
                    .always(),
                Tag::with_key("transaction")
                    .from_field("span.sentry_tags.transaction")
                    .always(),
                // Mobile:
                Tag::with_key("device.class")
                    .from_field("span.sentry_tags.device.class")
                    .when(mobile_condition.clone()),
                Tag::with_key("os.name") // TODO: might not be needed on both `exclusive_time` metrics
                    .from_field("span.sentry_tags.os.name")
                    .when(mobile_condition.clone()),
                Tag::with_key("release")
                    .from_field("span.sentry_tags.release")
                    .when(mobile_condition.clone()),
                Tag::with_key("ttfd")
                    .from_field("span.sentry_tags.ttfd")
                    .when(mobile_condition.clone()),
                Tag::with_key("ttid")
                    .from_field("span.sentry_tags.ttid")
                    .when(mobile_condition.clone()),
                Tag::with_key("span.main_thread")
                    .from_field("span.sentry_tags.main_thread")
                    .when(mobile_condition.clone()),
                // Resource module:
                Tag::with_key("file_extension")
                    .from_field("span.sentry_tags.file_extension")
                    .when(resource_condition.clone()),
                Tag::with_key("resource.render_blocking_status")
                    .from_field("span.sentry_tags.resource.render_blocking_status")
                    .when(resource_condition.clone()),
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time_light@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: Some(span_op_conditions.clone() & duration_condition.clone()),
            tags: vec![
                Tag::with_key("environment")
                    .from_field("span.sentry_tags.environment")
                    .always(),
                Tag::with_key("transaction.method")
                    .from_field("span.sentry_tags.transaction.method")
                    .always(),
                Tag::with_key("transaction.op")
                    .from_field("span.sentry_tags.transaction.op")
                    .always(),
                Tag::with_key("span.action")
                    .from_field("span.sentry_tags.action")
                    .always(),
                Tag::with_key("span.category")
                    .from_field("span.sentry_tags.category")
                    .always(),
                Tag::with_key("span.description")
                    .from_field("span.sentry_tags.description")
                    .always(),
                Tag::with_key("span.domain")
                    .from_field("span.sentry_tags.domain")
                    .always(),
                Tag::with_key("span.group")
                    .from_field("span.sentry_tags.group")
                    .always(),
                Tag::with_key("span.module")
                    .from_field("span.sentry_tags.module")
                    .always(),
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .always(),
                Tag::with_key("span.status")
                    .from_field("span.status") // from top-level field
                    .always(),
                Tag::with_key("span.status_code")
                    .from_field("span.sentry_tags.status_code")
                    .always(),
                Tag::with_key("span.system")
                    .from_field("span.sentry_tags.system")
                    .always(),
                // Mobile:
                Tag::with_key("device.class")
                    .from_field("span.sentry_tags.device.class")
                    .when(mobile_condition.clone()),
                Tag::with_key("os.name") // TODO: might not be needed on both `exclusive_time` metrics
                    .from_field("span.sentry_tags.os.name")
                    .when(mobile_condition.clone()),
                Tag::with_key("release")
                    .from_field("span.sentry_tags.release")
                    .when(mobile_condition.clone()),
                // Resource module:
                Tag::with_key("file_extension")
                    .from_field("span.sentry_tags.file_extension")
                    .when(resource_condition.clone()),
                Tag::with_key("resource.render_blocking_status")
                    .from_field("span.sentry_tags.resource.render_blocking_status")
                    .when(resource_condition.clone()),
            ],
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
            tags: vec![
                Tag::with_key("environment")
                    .from_field("span.sentry_tags.environment")
                    .always(), // already guarded by condition on metric
                Tag::with_key("file_extension")
                    .from_field("span.sentry_tags.file_extension")
                    .always(), // already guarded by condition on metric
                Tag::with_key("resource.render_blocking_status")
                    .from_field("span.sentry_tags.resource.render_blocking_status")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.description")
                    .from_field("span.sentry_tags.description")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.domain")
                    .from_field("span.sentry_tags.domain")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.group")
                    .from_field("span.sentry_tags.group")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .always(), // already guarded by condition on metric
                Tag::with_key("transaction")
                    .from_field("span.sentry_tags.transaction")
                    .always(), // already guarded by condition on metric
            ],
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
            tags: vec![
                Tag::with_key("environment")
                    .from_field("span.sentry_tags.environment")
                    .always(), // already guarded by condition on metric
                Tag::with_key("file_extension")
                    .from_field("span.sentry_tags.file_extension")
                    .always(), // already guarded by condition on metric
                Tag::with_key("resource.render_blocking_status")
                    .from_field("span.sentry_tags.resource.render_blocking_status")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.description")
                    .from_field("span.sentry_tags.description")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.domain")
                    .from_field("span.sentry_tags.domain")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.group")
                    .from_field("span.sentry_tags.group")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .always(), // already guarded by condition on metric
            ],
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
            tags: vec![
                Tag::with_key("environment")
                    .from_field("span.sentry_tags.environment")
                    .always(), // already guarded by condition on metric
                Tag::with_key("file_extension")
                    .from_field("span.sentry_tags.file_extension")
                    .always(), // already guarded by condition on metric
                Tag::with_key("resource.render_blocking_status")
                    .from_field("span.sentry_tags.resource.render_blocking_status")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.description")
                    .from_field("span.sentry_tags.description")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.domain")
                    .from_field("span.sentry_tags.domain")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.group")
                    .from_field("span.sentry_tags.group")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .always(), // already guarded by condition on metric
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "c:spans/count_per_op@none".into(),
            field: None,
            condition: Some(duration_condition.clone()),
            tags: vec![
                Tag::with_key("span.category")
                    .from_field("span.sentry_tags.category")
                    .always(),
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .always(),
                Tag::with_key("span.system")
                    .from_field("span.sentry_tags.system")
                    .always(),
            ],
        },
    ]);

    config._span_metrics_extended = true;
    if config.version == 0 {
        config.version = MetricExtractionConfig::VERSION;
    }
}
