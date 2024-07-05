use relay_base_schema::data_category::DataCategory;
use relay_common::glob2::LazyGlob;
use relay_event_normalization::utils::MAX_DURATION_MOBILE_MS;
use relay_protocol::RuleCondition;
use serde_json::Number;

use crate::metrics::MetricSpec;
use crate::{Feature, GroupKey, MetricExtractionConfig, ProjectConfig, Tag, TagMapping};

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
    "process.load",
    "ui.load*",
];

/// A list of span descriptions that indicate top-level app start spans.
const APP_START_ROOT_SPAN_DESCRIPTIONS: &[&str] = &["Cold Start", "Warm Start"];

/// A list of patterns found in MongoDB queries.
const MONGODB_QUERIES: &[&str] = &["*\"$*", "{*", "*({*", "*[{*"];

/// A list of patterns for resource span ops we'd like to ingest.
const RESOURCE_SPAN_OPS: &[&str] = &["resource.script", "resource.css", "resource.img"];

const CACHE_SPAN_OPS: &[&str] = &[
    "cache.get_item",
    "cache.save",
    "cache.clear",
    "cache.delete_item",
    "cache.get",
    "cache.put",
    "cache.remove",
    "cache.flush",
];

const QUEUE_SPAN_OPS: &[&str] = &[
    "queue.task.*",
    "queue.submit.*",
    "queue.task",
    "queue.submit",
    "queue.publish",
    "queue.process",
];

/// Conditionally enables metrics extraction groups from the global config.
///
/// Depending on feature flags, groups are either enabled or not.
/// This configuration is temporarily hard-coded here. It will later be provided by the upstream.
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

    // Common span metrics is a requirement for everything else:
    if !features.has(Feature::ExtractCommonSpanMetricsFromEvent) {
        return;
    }

    // Enable the common modules group:
    config
        .global_groups
        .entry(GroupKey::SpanMetricsCommon)
        .or_default()
        .is_enabled = true;

    if features.has(Feature::ExtractAddonsSpanMetricsFromEvent) {
        config
            .global_groups
            .entry(GroupKey::SpanMetricsAddons)
            .or_default()
            .is_enabled = true;
    }

    // Enable transaction metrics for span (score.total), but only if double-write to transactions
    // is disabled.
    if !project_config
        .features
        .has(Feature::ExtractTransactionFromSegmentSpan)
    {
        let span_metrics_tx = config
            .global_groups
            .entry(GroupKey::SpanMetricsTx)
            .or_default();
        span_metrics_tx.is_enabled = true;
    }

    config._span_metrics_extended = true;
    if config.version == 0 {
        config.version = MetricExtractionConfig::MAX_SUPPORTED_VERSION;
    }
}

/// Configuration for extracting metrics from spans.
///
/// These metrics are added to [`crate::GlobalConfig`] by the service and enabled
/// by project configs in sentry.
pub fn hardcoded_span_metrics() -> Vec<(GroupKey, Vec<MetricSpec>, Vec<TagMapping>)> {
    let is_ai = RuleCondition::glob("span.op", "ai.*");

    let is_db = RuleCondition::eq("span.sentry_tags.category", "db")
        & !(RuleCondition::eq("span.system", "mongodb")
            | RuleCondition::eq("span.system", "redis")
            | RuleCondition::glob("span.op", DISABLED_DATABASES)
            | RuleCondition::glob("span.description", MONGODB_QUERIES));
    let is_resource = RuleCondition::glob("span.op", RESOURCE_SPAN_OPS);

    let is_cache = RuleCondition::glob("span.op", CACHE_SPAN_OPS);

    let is_mobile_op = RuleCondition::glob("span.op", MOBILE_OPS);

    let is_mobile_sdk = RuleCondition::eq("span.sentry_tags.mobile", "true");

    let is_http = RuleCondition::eq("span.op", "http.client");

    let is_queue_op = RuleCondition::glob("span.op", QUEUE_SPAN_OPS);

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

    let is_app_start = RuleCondition::glob("span.op", "app.start.*")
        & RuleCondition::eq("span.description", APP_START_ROOT_SPAN_DESCRIPTIONS);

    // Metrics for addon modules are only extracted if the feature flag is enabled:
    let is_addon = is_ai.clone() | is_queue_op.clone() | is_cache.clone();

    vec![
        (
            GroupKey::SpanMetricsCommon,
            vec![
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:spans/exclusive_time@millisecond".into(),
                    field: Some("span.exclusive_time".into()),
                    condition: Some(!is_addon.clone()),
                    tags: vec![],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:spans/exclusive_time_light@millisecond".into(),
                    field: Some("span.exclusive_time".into()),
                    condition: Some(
                        // The `!is_addon` check might be redundant, but we want to make sure that
                        // `exclusive_time_light` is not extracted twice.
                        !is_addon.clone()
                            & (is_db.clone()
                                | is_resource.clone()
                                | is_mobile.clone()
                                | is_interaction.clone()
                                | is_http.clone())
                            & duration_condition.clone(),
                    ),
                    tags: vec![
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .when(
                                is_db.clone()
                                    | is_resource.clone()
                                    | is_mobile.clone()
                                    | is_http.clone(),
                            ),
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
                            .when(is_db.clone() | is_resource.clone() | is_http.clone()),
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
                        // HTTP module:
                        Tag::with_key("span.status_code")
                            .from_field("span.sentry_tags.status_code")
                            .when(is_http.clone()),
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:spans/duration@millisecond".into(),
                    field: Some("span.duration".into()),
                    condition: Some(!is_addon.clone()),
                    tags: vec![],
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
                            & RuleCondition::gt(
                                "span.data.http\\.decoded_response_content_length",
                                0,
                            ),
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
                        is_resource.clone()
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
                    mri: "d:spans/webvital.score.total@ratio".into(),
                    field: Some("span.measurements.score.total.value".into()),
                    condition: Some(is_allowed_browser.clone()),
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
                    mri: "g:spans/mobile.slow_frames@none".into(),
                    field: Some("span.measurements.frames.slow.value".into()),
                    condition: Some(is_mobile.clone() & duration_condition.clone()),
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
                        Tag::with_key("span.description")
                            .from_field("span.sentry_tags.description")
                            .always(),
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("span.group")
                            .from_field("span.sentry_tags.group")
                            .always(),
                        Tag::with_key("device.class")
                            .from_field("span.sentry_tags.device.class")
                            .always(),
                        Tag::with_key("os.name")
                            .from_field("span.sentry_tags.os.name")
                            .always(),
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "g:spans/mobile.frozen_frames@none".into(),
                    field: Some("span.measurements.frames.frozen.value".into()),
                    condition: Some(is_mobile.clone() & duration_condition.clone()),
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
                        Tag::with_key("span.description")
                            .from_field("span.sentry_tags.description")
                            .always(),
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("span.group")
                            .from_field("span.sentry_tags.group")
                            .always(),
                        Tag::with_key("device.class")
                            .from_field("span.sentry_tags.device.class")
                            .always(),
                        Tag::with_key("os.name")
                            .from_field("span.sentry_tags.os.name")
                            .always(),
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "g:spans/mobile.total_frames@none".into(),
                    field: Some("span.measurements.frames.total.value".into()),
                    condition: Some(is_mobile.clone() & duration_condition.clone()),
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
                        Tag::with_key("span.description")
                            .from_field("span.sentry_tags.description")
                            .always(),
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("span.group")
                            .from_field("span.sentry_tags.group")
                            .always(),
                        Tag::with_key("device.class")
                            .from_field("span.sentry_tags.device.class")
                            .always(),
                        Tag::with_key("os.name")
                            .from_field("span.sentry_tags.os.name")
                            .always(),
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "g:spans/mobile.frames_delay@second".into(),
                    field: Some("span.measurements.frames.delay.value".into()),
                    condition: Some(is_mobile.clone() & duration_condition.clone()),
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
                        Tag::with_key("span.description")
                            .from_field("span.sentry_tags.description")
                            .always(),
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("span.group")
                            .from_field("span.sentry_tags.group")
                            .always(),
                        Tag::with_key("device.class")
                            .from_field("span.sentry_tags.device.class")
                            .always(),
                        Tag::with_key("os.name")
                            .from_field("span.sentry_tags.os.name")
                            .always(),
                    ],
                },
            ],
            vec![TagMapping {
                metrics: vec![
                    LazyGlob::new("d:spans/duration@millisecond"),
                    LazyGlob::new("d:spans/exclusive_time@millisecond"),
                ],
                tags: vec![
                    // All modules:
                    Tag::with_key("environment")
                        .from_field("span.sentry_tags.environment")
                        .always(),
                    Tag::with_key("span.op")
                        .from_field("span.sentry_tags.op")
                        .always(),
                    Tag::with_key("transaction")
                        .from_field("span.sentry_tags.transaction")
                        .always(),
                    Tag::with_key("transaction.op")
                        .from_field("span.sentry_tags.transaction.op")
                        .always(),
                    Tag::with_key("span.group")
                        .from_field("span.sentry_tags.group")
                        .when(
                            (is_app_start.clone()
                                | is_db.clone()
                                | is_resource.clone()
                                | is_mobile.clone()
                                | is_http.clone()
                                | is_ai.clone() // guarded by is_addon
                                | is_queue_op.clone())  // guarded by is_addon
                                & duration_condition.clone(),
                        ),
                    Tag::with_key("span.category")
                        .from_field("span.sentry_tags.category")
                        .when(
                            (is_db.clone()
                                | is_ai.clone()
                                | is_resource.clone()
                                | is_mobile.clone()
                                | is_http.clone()
                                | is_queue_op.clone()
                            | is_ai.clone() // guarded by is_addon
                                | is_queue_op.clone())  // guarded by is_addon
                                & duration_condition.clone(),
                        ),
                    Tag::with_key("span.description")
                        .from_field("span.sentry_tags.description")
                        .when(
                            (is_app_start.clone()
                                | is_ai.clone()
                                | is_db.clone()
                                | is_resource.clone()
                                | is_mobile.clone()
                                | is_http.clone()
                                | is_queue_op.clone()| is_ai.clone() // guarded by is_addon
                                | is_queue_op.clone())  // guarded by is_addon)
                                & duration_condition.clone(),
                        ),
                    // Know modules:
                    Tag::with_key("transaction.method")
                        .from_field("span.sentry_tags.transaction.method")
                        .when(is_db.clone() | is_mobile.clone() | is_http.clone()), // groups by method + txn, e.g. `GET /users`
                    Tag::with_key("span.action")
                        .from_field("span.sentry_tags.action")
                        .when(is_db.clone()),
                    Tag::with_key("span.domain")
                        .from_field("span.sentry_tags.domain")
                        .when(is_db.clone() | is_resource.clone() | is_http.clone()),
                    // Mobile module:
                    Tag::with_key("device.class")
                        .from_field("span.sentry_tags.device.class")
                        .when(is_mobile.clone()),
                    Tag::with_key("os.name") // TODO: might not be needed on both `exclusive_time` metrics
                        .from_field("span.sentry_tags.os.name")
                        .when(
                            is_mobile.clone() | (is_app_start.clone() & duration_condition.clone()),
                        ),
                    Tag::with_key("release")
                        .from_field("span.sentry_tags.release")
                        .when(
                            is_mobile.clone() | (is_app_start.clone() & duration_condition.clone()),
                        ),
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
                    // HTTP module:
                    Tag::with_key("span.status_code")
                        .from_field("span.sentry_tags.status_code")
                        .when(is_http.clone()),
                ],
            }],
        ),
        (
            GroupKey::SpanMetricsAddons,
            vec![
                // all addon modules
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:spans/exclusive_time@millisecond".into(),
                    field: Some("span.exclusive_time".into()),
                    condition: Some(is_addon.clone()),
                    tags: vec![],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:spans/exclusive_time_light@millisecond".into(),
                    field: Some("span.exclusive_time".into()),
                    condition: Some(is_addon.clone() & duration_condition.clone()),
                    tags: vec![
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .when(is_cache.clone() | is_queue_op.clone()),
                        Tag::with_key("span.category")
                            .from_field("span.sentry_tags.category")
                            .always(),
                        Tag::with_key("span.description")
                            .from_field("span.sentry_tags.description")
                            .always(),
                        Tag::with_key("span.group")
                            .from_field("span.sentry_tags.group")
                            .always(),
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                    ],
                },
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:spans/duration@millisecond".into(),
                    field: Some("span.duration".into()),
                    condition: Some(is_addon),
                    tags: vec![],
                },
                // cache module
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "d:spans/cache.item_size@byte".into(),
                    field: Some("span.measurements.cache.item_size.value".into()),
                    condition: Some(is_cache.clone()),
                    tags: vec![
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("transaction")
                            .from_field("span.sentry_tags.transaction")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("cache.hit")
                            .from_field("span.sentry_tags.cache.hit")
                            .always(), // already guarded by condition on metric
                    ],
                },
                // ai module
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "c:spans/ai.total_tokens.used@none".into(),
                    field: Some("span.measurements.ai_total_tokens_used.value".into()),
                    condition: Some(is_ai.clone()),
                    tags: vec![
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("release")
                            .from_field("span.sentry_tags.release")
                            .always(),
                        Tag::with_key("span.origin")
                            .from_field("span.origin")
                            .always(),
                        Tag::with_key("span.category")
                            .from_field("span.sentry_tags.category")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("span.ai.pipeline.group")
                            .from_field("span.sentry_tags.ai_pipeline_group")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("span.description")
                            .from_field("span.sentry_tags.description")
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
                    mri: "c:spans/ai.total_cost@usd".into(),
                    field: Some("span.measurements.ai_total_cost.value".into()),
                    condition: Some(is_ai.clone()),
                    tags: vec![
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("release")
                            .from_field("span.sentry_tags.release")
                            .always(),
                        Tag::with_key("span.origin")
                            .from_field("span.origin")
                            .always(),
                        Tag::with_key("span.category")
                            .from_field("span.sentry_tags.category")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("span.ai.pipeline.group")
                            .from_field("span.sentry_tags.ai_pipeline_group")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("span.description")
                            .from_field("span.sentry_tags.description")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("span.group")
                            .from_field("span.sentry_tags.group")
                            .always(), // already guarded by condition on metric
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(), // already guarded by condition on metric
                    ],
                }, // queue module
                MetricSpec {
                    category: DataCategory::Span,
                    mri: "g:spans/messaging.message.receive.latency@millisecond".into(),
                    field: Some("span.measurements.messaging.message.receive.latency.value".into()),
                    condition: Some(is_queue_op.clone()),
                    tags: vec![
                        Tag::with_key("environment")
                            .from_field("span.sentry_tags.environment")
                            .always(),
                        Tag::with_key("span.op")
                            .from_field("span.sentry_tags.op")
                            .always(),
                        Tag::with_key("transaction")
                            .from_field("span.sentry_tags.transaction")
                            .always(),
                        Tag::with_key("messaging.destination.name")
                            .from_field("span.sentry_tags.messaging.destination.name")
                            .always(),
                    ],
                },
            ],
            vec![
                TagMapping {
                    metrics: vec![
                        LazyGlob::new("d:spans/duration@millisecond"),
                        LazyGlob::new("d:spans/exclusive_time_light@millisecond"),
                        LazyGlob::new("d:spans/exclusive_time@millisecond"),
                    ],
                    tags: vec![
                        // cache module
                        Tag::with_key("cache.hit")
                            .from_field("span.sentry_tags.cache.hit")
                            .when(is_cache.clone()),
                        // queue module
                        Tag::with_key("messaging.destination.name")
                            .from_field("span.sentry_tags.messaging.destination.name")
                            .when(is_queue_op.clone()),
                        Tag::with_key("trace.status")
                            .from_field("span.sentry_tags.trace.status")
                            .when(is_queue_op.clone()),
                    ],
                },
                TagMapping {
                    metrics: vec![LazyGlob::new("d:spans/exclusive_time_light@millisecond")],
                    tags: vec![Tag::with_key("environment")
                        .from_field("span.sentry_tags.environment")
                        .when(is_cache.clone())],
                },
            ],
        ),
        (
            GroupKey::SpanMetricsTx,
            vec![MetricSpec {
                category: DataCategory::Span,
                mri: "d:transactions/measurements.score.total@ratio".into(),
                field: Some("span.measurements.score.total.value".into()),
                condition: Some(
                    // If transactions are extracted from spans, the transaction processing pipeline
                    // will take care of this metric.
                    is_allowed_browser.clone() & RuleCondition::eq("span.was_transaction", false),
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
                ],
            }],
            vec![],
        ),
    ]
}
