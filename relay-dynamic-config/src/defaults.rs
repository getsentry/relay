use relay_base_schema::data_category::DataCategory;
use relay_common::glob2::LazyGlob;
use relay_event_normalization::utils::MAX_DURATION_MOBILE_MS;
use relay_protocol::RuleCondition;
use serde_json::Number;

use crate::feature::Feature;
use crate::metrics::{MetricExtractionConfig, MetricSpec, TagMapping, TagSpec};
use crate::project::ProjectConfig;

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

/// A list of AI Module span ops we want to ingest metrics for.
const LLM_OPS: &[&str] = &["ai.llm.*"];

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
                | RuleCondition::glob("span.op", LLM_OPS)
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
        MetricSpec {
            category: DataCategory::Span,
            mri: "c:spans/data.llm.llm_completion_tkens@none".into(),
            field: Some("span.data.llm_completion_tkens".into()),
            condition: Some(RuleCondition::eq("span.op", "ai.llm.completion")),
            tags: [
                ("span.sentry_tags.", "group"),
                ("span.sentry_tags.", "description"),
                ("span.sentry_tags.", "op"),
                ("span.data.", "language_model"),
                ("span.sentry_tags.", "environment"),
            ]
            .map(|(prefix, key)| TagSpec {
                key: format!("span.{key}"),
                field: format!("{prefix}{key}").into(),
                value: None,
                condition: None,
            })
            .into(),
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "c:spans/data.llm.llm_prompt_tkens@none".into(),
            field: Some("span.data.llm_prompt_tkens".into()),
            condition: Some(RuleCondition::eq("span.op", "ai.llm.completion")),
            tags: [
                ("span.sentry_tags.", "group"),
                ("span.sentry_tags.", "description"),
                ("span.sentry_tags.", "op"),
                ("span.data.", "language_model"),
                ("span.sentry_tags.", "environment"),
            ]
            .map(|(prefix, key)| TagSpec {
                key: format!("span.{key}"),
                field: format!("{prefix}{key}").into(),
                value: None,
                condition: None,
            })
            .into(),
        },
        MetricSpec {
            category: DataCategory::Span,
            mri: "c:spans/data.llm.llm_total_tkens@none".into(),
            field: Some("span.data.llm_total_tkens".into()),
            condition: Some(RuleCondition::eq("span.op", "ai.llm.completion")),
            tags: [
                ("span.sentry_tags.", "group"),
                ("span.sentry_tags.", "description"),
                ("span.sentry_tags.", "op"),
                ("span.data.", "language_model"),
                ("span.sentry_tags.", "environment"),
            ]
            .map(|(prefix, key)| TagSpec {
                key: format!("span.{key}"),
                field: format!("{prefix}{key}").into(),
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

    config._span_metrics_extended = true;
    if config.version == 0 {
        config.version = MetricExtractionConfig::VERSION;
    }
}
