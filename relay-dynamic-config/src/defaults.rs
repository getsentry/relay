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

/// A list of `span.op` patterns we want to enable for mobile.
const MOBILE_OPS: &[&str] = &[
    "activity.load",
    "app.*",
    "application.load",
    "contentprovider.load",
    "ui.load*",
];

/// A list of span descriptions that indicate top-level app start spans.
const APP_START_ROOT_SPAN_DESCRIPTIONS: &[&str] = &["Cold Start", "Warm Start"];

/// A list of patterns found in MongoDB queries.
const MONGODB_QUERIES: &[&str] = &["*\"$*", "{*", "*({*", "*[{*"];

/// A list of patterns for resource span ops we'd like to ingest.
const RESOURCE_SPAN_OPS: &[&str] = &["resource.script", "resource.css", "resource.img"];

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

    config.metrics.extend(span_metrics());

    config._span_metrics_extended = true;
    if config.version == 0 {
        config.version = MetricExtractionConfig::VERSION;
    }
}

/// Metrics with tags applied as required.
fn span_metrics() -> impl IntoIterator<Item = MetricSpec> {
    let is_db = RuleCondition::eq("span.sentry_tags.category", "db")
        & !(RuleCondition::eq("span.system", "mongodb")
            | RuleCondition::glob("span.op", DISABLED_DATABASES)
            | RuleCondition::glob("span.description", MONGODB_QUERIES));
    let is_resource = RuleCondition::glob("span.op", RESOURCE_SPAN_OPS);

    let is_mobile_op = RuleCondition::glob("span.op", MOBILE_OPS);

    let is_mobile_sdk = RuleCondition::eq("span.sentry_tags.mobile", "true");

    let is_allowed_browser = RuleCondition::eq(
        "span.sentry_tags.browser.name",
        vec![
            "Google Chrome",
            "Chrome",
            "Firefox",
            "Safari",
            "Edge",
            "Opera",
        ],
    );

    // This filter is based on
    // https://github.com/getsentry/sentry/blob/e01885215ff1a5b4e0da3046b4d929398a946360/static/app/views/starfish/views/screens/screenLoadSpans/spanOpSelector.tsx#L31-L34
    let is_screen = RuleCondition::eq("span.sentry_tags.transaction.op", "ui.load")
        & RuleCondition::eq(
            "span.op",
            vec![
                "file.read",
                "file.write",
                "ui.load",
                "http.client",
                "db",
                "db.sql.room",
                "db.sql.query",
                "db.sql.transaction",
            ],
        );

    let is_mobile = is_mobile_sdk.clone() & (is_mobile_op.clone() | is_screen);

    let is_interaction = RuleCondition::glob("span.op", "ui.interaction.*");

    // For mobile spans, only extract duration metrics when they are below a threshold.
    let duration_condition = RuleCondition::negate(is_mobile_op.clone())
        | RuleCondition::lte(
            "span.exclusive_time",
            Number::from_f64(MAX_DURATION_MOBILE_MS).unwrap_or(0.into()),
        );

    let app_start_condition = RuleCondition::glob("span.op", "app.start.*")
        & RuleCondition::eq("span.description", APP_START_ROOT_SPAN_DESCRIPTIONS);

    // `exclusive_time_light` is the metric with the most lenient condition.
    let exclusive_time_light_condition =
        (is_db.clone() | is_resource.clone() | is_mobile.clone() | is_interaction)
            & duration_condition.clone();

    [
        MetricSpec {
            category: DataCategory::Span,
            mri: "c:spans/usage@none".into(),
            field: None,
            condition: Some(exclusive_time_light_condition.clone()),
            tags: vec![],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: Some(
                (is_db.clone() | is_resource.clone() | is_mobile.clone())
                    & duration_condition.clone(),
            ),
            tags: vec![
                // Common tags:
                Tag::with_key("environment")
                    .from_field("span.sentry_tags.environment")
                    .when(is_db.clone() | is_resource.clone() | is_mobile.clone()),
                Tag::with_key("transaction.method")
                    .from_field("span.sentry_tags.transaction.method")
                    .when(is_db.clone() | is_mobile.clone()), // groups by method + txn, e.g. `GET /users`
                Tag::with_key("span.action")
                    .from_field("span.sentry_tags.action")
                    .when(is_db.clone()),
                Tag::with_key("span.category")
                    .from_field("span.sentry_tags.category")
                    .always(),
                Tag::with_key("span.description")
                    .from_field("span.sentry_tags.description")
                    .always(),
                Tag::with_key("span.domain")
                    .from_field("span.sentry_tags.domain")
                    .when(is_db.clone() | is_resource.clone()),
                Tag::with_key("span.group")
                    .from_field("span.sentry_tags.group")
                    .always(),
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .always(),
                Tag::with_key("transaction")
                    .from_field("span.sentry_tags.transaction")
                    .always(),
                // Mobile:
                Tag::with_key("transaction.op")
                    .from_field("span.sentry_tags.transaction.op")
                    .when(is_mobile.clone()), // filters by `transaction.op:ui.load`
                Tag::with_key("device.class")
                    .from_field("span.sentry_tags.device.class")
                    .when(is_mobile.clone()),
                Tag::with_key("os.name") // TODO: might not be needed on both `exclusive_time` metrics
                    .from_field("span.sentry_tags.os.name")
                    .when(is_mobile.clone()),
                Tag::with_key("release")
                    .from_field("span.sentry_tags.release")
                    .when(is_mobile.clone()),
                Tag::with_key("ttfd")
                    .from_field("span.sentry_tags.ttfd")
                    .when(is_mobile.clone()),
                Tag::with_key("ttid")
                    .from_field("span.sentry_tags.ttid")
                    .when(is_mobile.clone()),
                Tag::with_key("span.main_thread")
                    .from_field("span.sentry_tags.main_thread")
                    .when(is_mobile.clone()),
                Tag::with_key("app_start_type")
                    .from_field("span.sentry_tags.app_start_type")
                    .when(is_mobile.clone()),
                // Resource module:
                Tag::with_key("file_extension")
                    .from_field("span.sentry_tags.file_extension")
                    .when(is_resource.clone()),
                Tag::with_key("resource.render_blocking_status")
                    .from_field("span.sentry_tags.resource.render_blocking_status")
                    .when(is_resource.clone()),
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/exclusive_time_light@millisecond".into(),
            field: Some("span.exclusive_time".into()),
            condition: Some(exclusive_time_light_condition),
            tags: vec![
                Tag::with_key("environment")
                    .from_field("span.sentry_tags.environment")
                    .when(is_db.clone() | is_resource.clone() | is_mobile.clone()),
                Tag::with_key("transaction.op")
                    .from_field("span.sentry_tags.transaction.op")
                    .when(is_mobile.clone()),
                Tag::with_key("span.action")
                    .from_field("span.sentry_tags.action")
                    .when(is_db.clone()),
                Tag::with_key("span.category")
                    .from_field("span.sentry_tags.category")
                    .always(),
                Tag::with_key("span.description")
                    .from_field("span.sentry_tags.description")
                    .always(),
                Tag::with_key("span.domain")
                    .from_field("span.sentry_tags.domain")
                    .when(is_db.clone() | is_resource.clone()),
                Tag::with_key("span.group")
                    .from_field("span.sentry_tags.group")
                    .always(),
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .always(),
                // Mobile:
                Tag::with_key("device.class")
                    .from_field("span.sentry_tags.device.class")
                    .when(is_mobile.clone()),
                Tag::with_key("os.name") // TODO: might not be needed on both `exclusive_time` metrics
                    .from_field("span.sentry_tags.os.name")
                    .when(is_mobile.clone()),
                Tag::with_key("release")
                    .from_field("span.sentry_tags.release")
                    .when(is_mobile.clone()),
                // Resource module:
                Tag::with_key("file_extension")
                    .from_field("span.sentry_tags.file_extension")
                    .when(is_resource.clone()),
                Tag::with_key("resource.render_blocking_status")
                    .from_field("span.sentry_tags.resource.render_blocking_status")
                    .when(is_resource.clone()),
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/http.response_content_length@byte".into(),
            field: Some("span.data.http\\.response_content_length".into()),
            condition: Some(
                is_resource.clone()
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
                is_resource.clone()
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
                is_resource & RuleCondition::gt("span.data.http\\.response_transfer_size", 0),
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
        MetricSpec {
            category: DataCategory::Span,
            mri: "c:spans/count_per_segment@none".into(),
            field: None,
            condition: Some(is_mobile_sdk.clone() & duration_condition.clone()),
            tags: vec![
                Tag::with_key("transaction.op")
                    .from_field("span.sentry_tags.transaction.op")
                    .always(),
                Tag::with_key("transaction")
                    .from_field("span.sentry_tags.transaction")
                    .always(),
                Tag::with_key("release")
                    .from_field("span.sentry_tags.release")
                    .always(), // mobile only - already guarded by condition on metric
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/duration@millisecond".into(),
            field: Some("span.duration".into()),
            condition: Some(
                duration_condition.clone() & is_mobile.clone() & app_start_condition.clone(),
            ),
            tags: vec![
                Tag::with_key("span.op")
                    .from_field("span.sentry_tags.op")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.description")
                    .from_field("span.sentry_tags.description")
                    .always(), // already guarded by condition on metric
                Tag::with_key("span.group")
                    .from_field("span.sentry_tags.group")
                    .always(), // already guarded by condition on metric
                Tag::with_key("transaction")
                    .from_field("span.sentry_tags.transaction")
                    .always(), // already guarded by condition on metric
                Tag::with_key("device.class")
                    .from_field("span.sentry_tags.device.class")
                    .always(), // already guarded by condition on metric
                Tag::with_key("release")
                    .from_field("span.sentry_tags.release")
                    .always(), // already guarded by condition on metric
                Tag::with_key("os.name")
                    .from_field("span.sentry_tags.os.name")
                    .always(), // already guarded by condition on metric
                Tag::with_key("environment")
                    .from_field("span.sentry_tags.environment")
                    .always(), // already guarded by condition on metric
                Tag::with_key("app_start_type")
                    .from_field("span.sentry_tags.app_start_type")
                    .always(), // already guarded by condition on metric
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/webvital.score.total@ratio".into(),
            field: Some("span.measurements.score.total.value".into()),
            condition: Some(is_allowed_browser.clone()),
            tags: vec![
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
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/webvital.score.inp@ratio".into(),
            field: Some("span.measurements.score.inp.value".into()),
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
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/webvital.score.weight.inp@ratio".into(),
            field: Some("span.measurements.score.weight.inp.value".into()),
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
            ],
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "d:spans/webvital.inp@millisecond".into(),
            field: Some("span.measurements.inp.value".into()),
            condition: Some(is_allowed_browser),
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
            ],
        },
    ]
}
