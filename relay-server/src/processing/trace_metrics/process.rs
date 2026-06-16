use relay_event_normalization::eap::ClientUserAgentInfo;
use relay_event_normalization::{RequiredMode, SchemaProcessor, eap};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{TraceMetric, TraceMetricHeader};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, ItemContainer};
use crate::extractors::RequestTrust;
use crate::processing::Managed;
use crate::processing::trace_metrics::{Error, Result, Settings, utils::calculate_size};
use crate::processing::trace_metrics::{ExpandedTraceMetrics, SerializedTraceMetrics};
use crate::processing::{Context, utils};
use crate::services::outcome::DiscardReason;

/// Parses all serialized trace metrics into their [`ExpandedTraceMetrics`] representation.
///
/// Individual, invalid trace metrics will be discarded.
pub fn expand(metrics: Managed<SerializedTraceMetrics>) -> Managed<ExpandedTraceMetrics> {
    let trust = metrics.headers.meta().request_trust();

    metrics.map(|metrics, records| {
        records.lenient(DataCategory::TraceMetricByte);

        debug_assert!(
            metrics.invalid.is_empty(),
            "invalid items should already be rejected"
        );

        let SerializedTraceMetrics {
            headers,
            item,
            invalid: _,
        } = metrics;

        let expanded = expand_trace_metric_container(&item, trust);
        let (settings, metrics) = records.or_default(expanded, item);

        ExpandedTraceMetrics {
            headers,
            settings,
            metrics,
        }
    })
}

/// Normalizes individual trace metric entries.
///
/// Normalization must happen before any filters are applied or other procedures which rely on the
/// presence and well-formedness of attributes and fields.
pub fn normalize(metrics: &mut Managed<ExpandedTraceMetrics>, ctx: Context<'_>) {
    let settings = metrics.settings;

    metrics.retain_with_context(
        |metrics| (&mut metrics.metrics, &metrics.headers),
        |metric, headers, _| {
            normalize_trace_metric(metric, headers, settings, ctx).inspect_err(|err| {
                relay_log::debug!("failed to normalize trace metric: {err}");
            })
        },
    );
}

/// Applies PII scrubbing to individual trace metric entries.
pub fn scrub(metrics: &mut Managed<ExpandedTraceMetrics>, ctx: Context<'_>) {
    metrics.retain(
        |metrics| &mut metrics.metrics,
        |metric, _| {
            scrub_trace_metric(metric, ctx).inspect_err(|err| {
                relay_log::debug!("failed to scrub pii from trace metric: {err}")
            })
        },
    );
}

/// Parses a trace metric container into its [`ContainerItems<TraceMetric>`] representation.
fn expand_trace_metric_container(
    item: &Item,
    trust: RequestTrust,
) -> Result<(Settings, ContainerItems<TraceMetric>)> {
    let (metadata, mut metrics) = ItemContainer::parse(item)
        .map_err(|err| {
            relay_log::debug!("failed to parse trace metrics container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_parts();

    for metric in &mut metrics {
        let byte_size = metric
            .header
            .as_ref()
            .and_then(|h: &TraceMetricHeader| h.byte_size);
        if trust.is_untrusted() || matches!(byte_size, None | Some(0)) {
            let byte_size = metric.value().map(calculate_size).unwrap_or(1);
            metric.header.get_or_insert_default().byte_size = Some(byte_size);
        }
    }

    relay_log::trace!("trace metric container metadata: {metadata:?}");
    let settings = metadata
        .map(|metadata| {
            let is = metadata.ingest_settings.as_ref();

            match metadata.version {
                None => Settings::default(),
                // Technically invalid.
                Some(0 | 1) => Settings::default(),
                Some(2) => Settings {
                    infer_ip: is
                        .and_then(|is| is.infer_ip)
                        .is_some_and(|infer| infer.is_auto()),
                    infer_user_agent: is
                        .and_then(|is| is.infer_user_agent)
                        .is_some_and(|infer| infer.is_auto()),
                },
                // Unsupported, fall back to the safe default.
                Some(_) => Default::default(),
            }
        })
        .unwrap_or_default();

    Ok((settings, metrics))
}

/// Applies PII scrubbing to an individual trace metric entry.
fn scrub_trace_metric(metric: &mut Annotated<TraceMetric>, ctx: Context<'_>) -> Result<()> {
    let pii_config_from_scrubbing = ctx.project_info.config.datascrubbing_settings.pii_config();

    relay_pii::eap::scrub(
        ValueType::TraceMetric,
        metric,
        ctx.project_info.config.pii_config.as_ref(),
        pii_config_from_scrubbing.as_ref(),
    )?;

    Ok(())
}

/// Normalizes an individual trace metric entry.
fn normalize_trace_metric(
    metric: &mut Annotated<TraceMetric>,
    headers: &EnvelopeHeaders,
    settings: Settings,
    ctx: Context<'_>,
) -> Result<()> {
    let meta = headers.meta();

    eap::time::normalize(
        metric,
        utils::normalize::time_config(headers, |f| f.trace_metric.as_ref(), ctx),
    );

    if let Some(metric_value) = metric.value_mut() {
        let client_ua_info = settings.infer_user_agent.then(|| ClientUserAgentInfo {
            user_agent: meta.user_agent(),
            hints: meta.client_hints(),
        });

        eap::trace_metric::normalize_metric_name(metric_value)?;
        if ctx.is_processing() {
            eap::trace_metric::normalize_metric_unit(metric_value);
        }
        eap::normalize_attribute_types(&mut metric_value.attributes);
        eap::normalize_attribute_names(&mut metric_value.attributes);
        eap::normalize_received(&mut metric_value.attributes, meta.received_at());
        eap::normalize_client_address(&mut metric_value.attributes, meta.client_addr());
        if settings.infer_ip {
            eap::normalize_inject_client_address(&mut metric_value.attributes, meta.client_addr());
        }
        eap::normalize_user_agent(&mut metric_value.attributes, client_ua_info);
    };

    if let Annotated(None, meta) = metric {
        relay_log::debug!("empty metric: {meta:?}");
        return Err(Error::Invalid(DiscardReason::NoData));
    }

    Ok(())
}

/// Normalize derived fields and attributes.
///
/// This is separate from [`normalize`] because it needs to run
/// after PII scrubbing; PII might get leaked otherwise.
///
/// In practice, for trace metrics, it only performs schema validation.
pub fn normalize_derived(metrics: &mut Managed<ExpandedTraceMetrics>) {
    metrics.retain_with_context(
        |metrics| (&mut metrics.metrics, &()),
        |metric, _, _| {
            normalize_trace_metric_derived(metric).inspect_err(|err| {
                relay_log::debug!("failed to normalize trace metric: {err}");
            })
        },
    );
}

fn normalize_trace_metric_derived(metric: &mut Annotated<TraceMetric>) -> Result<()> {
    process_value(
        metric,
        &mut SchemaProcessor::new()
            .with_required(RequiredMode::DeleteParent)
            .with_verbose_errors(relay_log::enabled!(relay_log::Level::DEBUG)),
        ProcessingState::root(),
    )?;

    if let Annotated(None, meta) = metric {
        relay_log::debug!("empty span: {meta:?}");
        return Err(Error::Invalid(DiscardReason::NoData));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_pii::PiiConfig;
    use relay_protocol::assert_annotated_snapshot;

    use crate::services::projects::project::ProjectInfo;

    use super::*;

    #[test]
    fn test_scrub_trace_metric_base_fields() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "name": "test.metric",
            "type": "counter",
            "value": 1.0,
            "attributes": {
                "secret": {
                    "type": "string",
                    "value": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
                }
            }
        }
        "#;

        let mut data = Annotated::<TraceMetric>::from_json(json).unwrap();

        let mut datascrubbing_settings = relay_pii::DataScrubbingConfig::default();
        datascrubbing_settings.scrub_data = true;
        datascrubbing_settings.scrub_defaults = true;

        let ctx = Context {
            project_info: &ProjectInfo {
                config: relay_dynamic_config::ProjectConfig {
                    datascrubbing_settings,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };

        scrub_trace_metric(&mut data, ctx).unwrap();

        assert_annotated_snapshot!(data, @r#"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "name": "test.metric",
          "type": "counter",
          "value": 1.0,
          "attributes": {
            "secret": {
              "type": "string",
              "value": "[Filtered]"
            }
          },
          "_meta": {
            "attributes": {
              "secret": {
                "value": {
                  "": {
                    "rem": [
                      [
                        "@bearer:filter",
                        "s",
                        0,
                        10
                      ]
                    ],
                    "len": 43
                  }
                }
              }
            }
          }
        }
        "#);
    }

    #[test]
    fn test_scrub_trace_metric_deep_wild_cards() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "name": "test.metric",
            "type": "counter",
            "value": 1.0,
            "attributes": {
                "normal_field": {
                    "type": "string",
                    "value": "normal_value"
                }
            }
        }
        "#;

        let mut data = Annotated::<TraceMetric>::from_json(json).unwrap();

        let deep_wildcard_config = serde_json::from_value::<PiiConfig>(serde_json::json!({
            "rules": {
                "remove_normal_field": {
                    "type": "pattern",
                    "pattern": "normal_value",
                    "redaction": {
                        "method": "replace",
                        "text": "[REDACTED]"
                    }
                }
            },
            "applications": {
                "**": ["remove_normal_field"]
            }
        }))
        .unwrap();

        let ctx = Context {
            project_info: &ProjectInfo {
                config: relay_dynamic_config::ProjectConfig {
                    pii_config: Some(deep_wildcard_config),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };

        scrub_trace_metric(&mut data, ctx).unwrap();

        assert_annotated_snapshot!(data, @r#"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "name": "test.metric",
          "type": "counter",
          "value": 1.0,
          "attributes": {
            "normal_field": {
              "type": "string",
              "value": "[REDACTED]"
            }
          },
          "_meta": {
            "attributes": {
              "normal_field": {
                "value": {
                  "": {
                    "rem": [
                      [
                        "remove_normal_field",
                        "s",
                        0,
                        10
                      ]
                    ],
                    "len": 12
                  }
                }
              }
            }
          }
        }
        "#);

        data = Annotated::<TraceMetric>::from_json(json).unwrap();
        let config = serde_json::from_value::<PiiConfig>(serde_json::json!({
            "rules": {
                "should_not_remove_normal_field": {
                    "type": "pattern",
                    "pattern": "normal_value",
                    "redaction": {
                        "method": "replace",
                        "text": "[REDACTED]"
                    }
                }
            },
            "applications": {
                "** && !$trace_metric.**": ["should_not_remove_normal_field"]
            }
        }))
        .unwrap();

        let ctx = Context {
            project_info: &ProjectInfo {
                config: relay_dynamic_config::ProjectConfig {
                    pii_config: Some(config),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };

        scrub_trace_metric(&mut data, ctx).unwrap();

        assert_annotated_snapshot!(data, @r###"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "name": "test.metric",
          "type": "counter",
          "value": 1.0,
          "attributes": {
            "normal_field": {
              "type": "string",
              "value": "normal_value"
            }
          }
        }
        "###);
    }

    #[test]
    fn test_scrub_trace_metric_deep_wild_cards_anything() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "name": "test.metric",
            "type": "counter",
            "value": 1.0,
            "attributes": {
                "normal_field": {
                    "type": "string",
                    "value": "normal_value"
                }
            }
        }
        "#;

        let mut data = Annotated::<TraceMetric>::from_json(json).unwrap();

        let config = serde_json::from_value::<PiiConfig>(serde_json::json!({
            "rules": {
                "remove_normal_field": {
                    "type": "anything",
                    "redaction": {
                        "method": "replace",
                        "text": "[REDACTED]"
                    }
                }
            },
            "applications": {
                "**": ["remove_normal_field"]
            }
        }))
        .unwrap();

        let ctx = Context {
            project_info: &ProjectInfo {
                config: relay_dynamic_config::ProjectConfig {
                    pii_config: Some(config),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };

        scrub_trace_metric(&mut data, ctx).unwrap();

        assert_annotated_snapshot!(data, @r#"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "name": "test.metric",
          "type": "counter",
          "value": 1.0,
          "attributes": {
            "normal_field": {
              "type": "string",
              "value": "[REDACTED]"
            }
          },
          "_meta": {
            "attributes": {
              "normal_field": {
                "value": {
                  "": {
                    "rem": [
                      [
                        "remove_normal_field",
                        "s",
                        0,
                        10
                      ]
                    ],
                    "len": 12
                  }
                }
              }
            }
          }
        }
        "#);
    }
}
