use std::time::Duration;

use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, ItemContainer, WithHeader};
use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::spans::{self, Error, ExpandedSpans, Result, SampledSpans};
use crate::services::outcome::DiscardReason;
use relay_conventions::consts;
use relay_event_normalization::span::TABLE_NAME_REGEX;
use relay_event_normalization::span::description::scrub_db_query;
use relay_event_normalization::span::tag_extraction::{
    sql_action_from_query, sql_tables_from_query,
};
use relay_event_normalization::{
    GeoIpLookup, RequiredMode, SchemaProcessor, TimestampProcessor, TrimmingProcessor, eap,
};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{Attributes, Span, SpanV2};
use relay_protocol::Annotated;
use relay_spans::derive_op_for_v2_span;
use std::borrow::Cow;

/// Parses all serialized spans.
///
/// Individual, invalid spans are discarded.
pub fn expand(spans: Managed<SampledSpans>) -> Managed<ExpandedSpans> {
    spans.map(|spans, records| {
        let mut all_spans = Vec::new();

        for item in &spans.inner.spans {
            let expanded = expand_span_container(item);
            let expanded = records.or_default(expanded, item);
            all_spans.extend(expanded);
        }

        for item in &spans.inner.legacy {
            match expand_legacy_span(item) {
                Ok(span) => all_spans.push(span),
                Err(err) => drop(records.reject_err(err, item)),
            }
        }

        spans::integrations::expand_into(&mut all_spans, records, spans.inner.integrations);

        ExpandedSpans {
            headers: spans.inner.headers,
            server_sample_rate: spans.server_sample_rate,
            spans: all_spans,
            category: spans::TotalAndIndexed,
        }
    })
}

fn expand_span_container(item: &Item) -> Result<ContainerItems<SpanV2>> {
    let spans = ItemContainer::parse(item)
        .map_err(|err| {
            relay_log::debug!("failed to parse span container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_items();

    Ok(spans)
}

fn expand_legacy_span(item: &Item) -> Result<WithHeader<SpanV2>> {
    let payload = item.payload();

    let span = Annotated::<Span>::from_json_bytes(&payload)
        .map_err(|err| {
            relay_log::debug!("failed to parse span: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .map_value(relay_spans::span_v1_to_span_v2);

    Ok(WithHeader::new(span))
}

/// Normalizes individual spans.
pub fn normalize(spans: &mut Managed<ExpandedSpans>, geo_lookup: &GeoIpLookup, ctx: Context<'_>) {
    spans.retain_with_context(
        |spans| (&mut spans.spans, &spans.headers),
        |span, headers, _| {
            normalize_span(span, headers, geo_lookup, ctx).inspect_err(|err| {
                relay_log::debug!("failed to normalize span: {err}");
            })
        },
    );
}

fn normalize_span(
    span: &mut Annotated<SpanV2>,
    headers: &EnvelopeHeaders,
    geo_lookup: &GeoIpLookup,
    ctx: Context<'_>,
) -> Result<()> {
    process_value(span, &mut TimestampProcessor, ProcessingState::root())?;

    if let Some(span) = span.value_mut() {
        let meta = headers.meta();
        let dsc = headers.dsc();
        let duration = span_duration(span);
        let model_costs = ctx.global_config.ai_model_costs.as_ref().ok();

        if span
            .attributes
            .value()
            .and_then(|v| v.get_value(consts::OP))
            .is_none()
        {
            let inferred_op = derive_op_for_v2_span(span);
            span.attributes
                .get_or_insert_with(Default::default)
                .insert(consts::OP, inferred_op);
        }

        validate_timestamps(span)?;

        eap::normalize_attribute_types(&mut span.attributes);
        eap::normalize_attribute_names(&mut span.attributes);
        eap::normalize_received(&mut span.attributes, meta.received_at());
        eap::normalize_client_address(&mut span.attributes, meta.client_addr());
        eap::normalize_user_agent(&mut span.attributes, meta.user_agent(), meta.client_hints());
        eap::normalize_user_geo(&mut span.attributes, || {
            meta.client_addr().and_then(|ip| geo_lookup.lookup(ip))
        });
        if matches!(span.is_segment.value(), Some(true)) {
            eap::normalize_dsc(&mut span.attributes, dsc);
        }
        eap::normalize_ai(&mut span.attributes, duration, model_costs);

        normalize_attribute_values(&mut span.attributes);
    };

    process_value(span, &mut TrimmingProcessor::new(), ProcessingState::root())?;
    process_value(
        span,
        &mut SchemaProcessor::new().with_required(RequiredMode::DeleteParent),
        ProcessingState::root(),
    )?;

    if let Annotated(None, meta) = span {
        relay_log::debug!("empty span: {meta:?}");
        return Err(Error::Invalid(DiscardReason::NoData));
    }

    Ok(())
}

fn normalize_attribute_values(attributes: &mut Annotated<Attributes>) {
    normalize_db_attributes(attributes);
}

fn normalize_db_attributes(attributes: &mut Annotated<Attributes>) {
    if attributes
        .value()
        .is_some_and(|attributes| attributes.get_value(consts::NORMALIZED_DB_QUERY).is_some())
    {
        return;
    }

    let (normalized_db_query, db_operation, db_collection_name) = attributes
        .value()
        .map(|attributes| {
            let (op, sub_op) = attributes
                .get_value(consts::OP)
                .and_then(|v| v.as_str())
                .and_then(|op| op.split_once('.'))
                .unwrap_or_default();

            let raw_query = attributes
                .get_value(consts::DB_QUERY_TEXT)
                .or_else(|| {
                    if op == "db" {
                        attributes.get_value(consts::DESCRIPTION)
                    } else {
                        None
                    }
                })
                .and_then(|v| v.as_str());

            let db_system = attributes
                .get_value(consts::DB_SYSTEM_NAME)
                .and_then(|v| v.as_str());

            let db_operation = attributes
                .get_value(consts::DB_OPERATION)
                .and_then(|v| v.as_str());

            let collection_name = attributes
                .get_value(consts::DB_COLLECTION_NAME)
                .and_then(|v| v.as_str());

            let span_origin = attributes
                .get_value(consts::ORIGIN)
                .and_then(|v| v.as_str());

            let (normalized_db_query, parsed_sql) = if let Some(raw_query) = raw_query {
                scrub_db_query(
                    raw_query,
                    sub_op,
                    db_system,
                    db_operation,
                    collection_name,
                    span_origin,
                )
            } else {
                (None, None)
            };

            let db_operation = if db_operation.is_none() {
                if sub_op == "redis" || db_system == Some("redis") {
                    // This only works as long as redis span descriptions contain the command + " *"
                    if let Some(query) = normalized_db_query.as_ref() {
                        let command = query.replace(" *", "");
                        if command.is_empty() {
                            None
                        } else {
                            Some(command)
                        }
                    } else {
                        None
                    }
                } else if let Some(raw_query) = raw_query {
                    // For other database operations, try to get the operation from data
                    sql_action_from_query(raw_query).map(|a| a.to_uppercase())
                } else {
                    None
                }
            } else {
                db_operation.map(|db_operation| db_operation.to_uppercase())
            };

            let db_collection_name: Option<String> = if collection_name.is_none() {
                if span_origin == Some("auto.db.supabase") {
                    normalized_db_query
                        .as_ref()
                        .and_then(|query| query.strip_prefix("from("))
                        .and_then(|s| s.strip_suffix(")"))
                        .map(String::from)
                } else if let Some(raw_query) = raw_query {
                    sql_tables_from_query(raw_query, &parsed_sql)
                } else {
                    None
                }
            } else if let Some(collection_name) = collection_name
                && db_system == Some("mongodb")
            {
                if let Cow::Owned(s) = TABLE_NAME_REGEX.replace_all(collection_name, "{%s}") {
                    Some(s)
                } else {
                    Some(collection_name.to_owned())
                }
            } else {
                None
            };

            (normalized_db_query, db_operation, db_collection_name)
        })
        .unwrap_or_default();

    if let Some(attributes) = attributes.value_mut() {
        if let Some(normalized_db_query) = normalized_db_query {
            attributes.insert(consts::NORMALIZED_DB_QUERY, normalized_db_query);
        }
        if let Some(db_operation_name) = db_operation {
            attributes.insert(consts::DB_OPERATION, db_operation_name)
        }
        if let Some(db_collection_name) = db_collection_name {
            attributes.insert(consts::DB_COLLECTION_NAME, db_collection_name);
        }
    }
}

/// Validates the start and end timestamps of a span.
///
/// The start timestamp must not be after the end timestamp.
fn validate_timestamps(span: &SpanV2) -> Result<()> {
    match (span.start_timestamp.value(), span.end_timestamp.value()) {
        (Some(start), Some(end)) if start <= end => Ok(()),
        _ => Err(Error::Invalid(DiscardReason::Timestamp)),
    }
}

/// Applies PII scrubbing to individual spans.
pub fn scrub(spans: &mut Managed<ExpandedSpans>, ctx: Context<'_>) {
    spans.retain(
        |spans| &mut spans.spans,
        |span, _| {
            scrub_span(span, ctx)
                .inspect_err(|err| relay_log::debug!("failed to scrub pii from span: {err}"))
        },
    );
}

fn scrub_span(span: &mut Annotated<SpanV2>, ctx: Context<'_>) -> Result<()> {
    let pii_config_from_scrubbing = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| Error::PiiConfig(e.clone()))?;

    relay_pii::eap::scrub(
        ValueType::Span,
        span,
        ctx.project_info.config.pii_config.as_ref(),
        pii_config_from_scrubbing.as_ref(),
    )?;

    Ok(())
}

fn span_duration(span: &SpanV2) -> Option<Duration> {
    let start_timestamp = *span.start_timestamp.value()?;
    let end_timestamp = *span.end_timestamp.value()?;
    (end_timestamp - start_timestamp).to_std().ok()
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use relay_pii::{DataScrubbingConfig, PiiConfig};
    use relay_protocol::SerializableAnnotated;
    use relay_sampling::evaluation::ReservoirCounters;

    use crate::services::projects::project::ProjectInfo;

    use super::*;

    fn make_context(
        scrubbing_config: DataScrubbingConfig,
        pii_config: Option<PiiConfig>,
    ) -> Context<'static> {
        let config = Box::leak(Box::new(relay_config::Config::default()));
        let global_config = Box::leak(Box::new(relay_dynamic_config::GlobalConfig::default()));
        let project_info = Box::leak(Box::new(ProjectInfo {
            config: relay_dynamic_config::ProjectConfig {
                pii_config,
                datascrubbing_settings: scrubbing_config,
                ..Default::default()
            },
            ..Default::default()
        }));
        let rate_limits = Box::leak(Box::new(relay_quotas::RateLimits::default()));
        let reservoir_counters = Box::leak(Box::new(ReservoirCounters::default()));

        Context {
            config,
            global_config,
            project_info,
            rate_limits,
            sampling_project_info: None,
            reservoir_counters,
        }
    }

    #[test]
    fn test_scrub_span_pii_default_rules_links() {
        // `user.name`, `sentry.release`, and `url.path` are marked as follows in `sentry-conventions`:
        // * `user.name`: `true`
        // * `sentry.release`: `false`
        // * `url.path`: `maybe`
        // Therefore, `user.name` is the only one that should be scrubbed by default rules.
        let json = r#"{
            "start_timestamp": 1544719859.0,
            "end_timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "name": "test",
            "links": [{
                "trace_id": "5b8efff798038103d269b633813fc60c",
                "span_id": "eee19b7ec3c1b174",
                "attributes": {
                    "sentry.description": {
                        "type": "string",
                        "value": "secret123"
                    },
                    "user.name": {
                        "type": "string",
                        "value": "secret123"
                    },
                    "sentry.release": {
                        "type": "string",
                        "value": "secret123"
                    }
                }
            }]
        }"#;

        let mut data = Annotated::<SpanV2>::from_json(json).unwrap();

        let mut scrubbing_config = relay_pii::DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;
        scrubbing_config.scrub_defaults = true;
        scrubbing_config.scrub_ip_addresses = true;
        scrubbing_config.sensitive_fields = vec![
            "value".to_owned(), // Make sure the inner 'value' of the attribute object isn't scrubbed.
            "very_sensitive_data".to_owned(),
        ];
        scrubbing_config.exclude_fields = vec!["public_data".to_owned()];

        let ctx = make_context(scrubbing_config, None);
        scrub_span(&mut data, ctx).unwrap();

        let link = data.value().unwrap().links.value().unwrap()[0]
            .value()
            .unwrap();
        insta::assert_json_snapshot!(SerializableAnnotated(&link.attributes), @r###"
        {
          "sentry.description": {
            "type": "string",
            "value": "secret123"
          },
          "sentry.release": {
            "type": "string",
            "value": "secret123"
          },
          "user.name": {
            "type": "string",
            "value": "[Filtered]"
          },
          "_meta": {
            "user.name": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@password:filter",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 9
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_scrub_span_pii_custom_object_rules_links() {
        // `user.name`, `sentry.release`, and `url.path` are marked as follows in `sentry-conventions`:
        // * `user.name`: `true`
        // * `sentry.release`: `false`
        // * `url.path`: `maybe`
        // Therefore, `sentry.release` is the only one that should not be scrubbed by custom rules.
        let json = r#"
        {
            "start_timestamp": 1544719859.0,
            "end_timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "name": "test",
            "links": [{
                "attributes": {
                    "sentry.description": {
                        "type": "string",
                        "value": "secret123"
                    },
                    "user.name": {
                        "type": "string",
                        "value": "secret123"
                    },
                    "sentry.release": {
                        "type": "string",
                        "value": "secret123"
                    },
                    "url.path": {
                        "type": "string",
                        "value": "secret123"
                    },
                    "password": {
                        "type": "string",
                        "value": "default_scrubbing_rules_are_off"
                    },
                    "test_field_mac": {
                        "type": "string",
                        "value": "4a:00:04:10:9b:50"
                    },
                    "test_field_imei": {
                        "type": "string",
                        "value": "356938035643809"
                    },
                    "test_field_uuid": {
                        "type": "string",
                        "value": "123e4567-e89b-12d3-a456-426614174000"
                    },
                    "test_field_regex_passes": {
                        "type": "string",
                        "value": "wxyz"
                    },
                    "test_field_regex_fails": {
                        "type": "string",
                        "value": "abc"
                    }
                }
            }]
        }"#;

        let mut data = Annotated::<SpanV2>::from_json(json).unwrap();

        let mut scrubbing_config = relay_pii::DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;
        scrubbing_config.scrub_defaults = false;
        scrubbing_config.scrub_ip_addresses = false;

        let config = serde_json::from_value::<PiiConfig>(serde_json::json!(
        {
            "rules": {
                "project:0": {
                    "type": "mac",
                    "redaction": {
                        "method": "replace",
                        "text": "ITS_GONE"
                    }
                },
                "project:1": {
                    "type": "imei",
                    "redaction": {
                        "method": "replace",
                        "text": "ITS_GONE"
                    }
                },
                "project:2": {
                    "type": "uuid",
                    "redaction": {
                        "method": "replace",
                        "text": "BYE"
                    }
                },
                "project:3": {
                    "type": "pattern",
                    "pattern": "[w-z]+",
                    "redaction": {
                        "method": "replace",
                        "text": "REGEXED"
                    }
                },
                "project:4": {
                    "type": "anything",
                    "redaction": {
                        "method": "replace",
                        "text": "[USER NAME]"
                    }
                },
                "project:5": {
                    "type": "anything",
                    "redaction": {
                        "method": "replace",
                        "text": "[RELEASE]"
                    }
                },
                "project:6": {
                    "type": "anything",
                    "redaction": {
                        "method": "replace",
                        "text": "[URL PATH]"
                    }
                },
                "project:7": {
                    "type": "anything",
                    "redaction": {
                        "method": "replace",
                        "text": "[DESCRIPTION]"
                    }
                }
            },
            "applications": {
                "test_field_mac.value": [
                    "project:0"
                ],
                "test_field_imei.value": [
                    "project:1"
                ],
                "test_field_uuid.value": [
                    "project:2"
                ],
                "test_field_regex_passes.value || test_field_regex_fails.value": [
                    "project:3"
                ],
                "'user.name'.value": [
                    "project:4"
                ],
                "'sentry.release'.value": [
                    "project:5"
                ],
                "'url.path'.value": [
                    "project:6"
                ],
                "'sentry.description'.value": [
                    "project:7"
                ]
            }
        }
        ))
        .unwrap();

        let ctx = make_context(scrubbing_config, Some(config));
        scrub_span(&mut data, ctx).unwrap();
        let link = data.value().unwrap().links.value().unwrap()[0]
            .value()
            .unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&link.attributes), @r###"
        {
          "password": {
            "type": "string",
            "value": "default_scrubbing_rules_are_off"
          },
          "sentry.description": {
            "type": "string",
            "value": "[DESCRIPTION]"
          },
          "sentry.release": {
            "type": "string",
            "value": "secret123"
          },
          "test_field_imei": {
            "type": "string",
            "value": "ITS_GONE"
          },
          "test_field_mac": {
            "type": "string",
            "value": "ITS_GONE"
          },
          "test_field_regex_fails": {
            "type": "string",
            "value": "abc"
          },
          "test_field_regex_passes": {
            "type": "string",
            "value": "REGEXED"
          },
          "test_field_uuid": {
            "type": "string",
            "value": "BYE"
          },
          "url.path": {
            "type": "string",
            "value": "[URL PATH]"
          },
          "user.name": {
            "type": "string",
            "value": "[USER NAME]"
          },
          "_meta": {
            "sentry.description": {
              "value": {
                "": {
                  "rem": [
                    [
                      "project:7",
                      "s",
                      0,
                      13
                    ]
                  ],
                  "len": 9
                }
              }
            },
            "test_field_imei": {
              "value": {
                "": {
                  "rem": [
                    [
                      "project:1",
                      "s",
                      0,
                      8
                    ]
                  ],
                  "len": 15
                }
              }
            },
            "test_field_mac": {
              "value": {
                "": {
                  "rem": [
                    [
                      "project:0",
                      "s",
                      0,
                      8
                    ]
                  ],
                  "len": 17
                }
              }
            },
            "test_field_regex_passes": {
              "value": {
                "": {
                  "rem": [
                    [
                      "project:3",
                      "s",
                      0,
                      7
                    ]
                  ],
                  "len": 4
                }
              }
            },
            "test_field_uuid": {
              "value": {
                "": {
                  "rem": [
                    [
                      "project:2",
                      "s",
                      0,
                      3
                    ]
                  ],
                  "len": 36
                }
              }
            },
            "url.path": {
              "value": {
                "": {
                  "rem": [
                    [
                      "project:6",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 9
                }
              }
            },
            "user.name": {
              "value": {
                "": {
                  "rem": [
                    [
                      "project:4",
                      "s",
                      0,
                      11
                    ]
                  ],
                  "len": 9
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_validate_timestamp_start_before_end() {
        validate_timestamps(&SpanV2 {
            start_timestamp: Annotated::new(DateTime::from_timestamp_nanos(100).into()),
            end_timestamp: Annotated::new(DateTime::from_timestamp_nanos(200).into()),
            ..Default::default()
        })
        .unwrap();
    }

    #[test]
    fn test_validate_timestamp_start_is_end() {
        validate_timestamps(&SpanV2 {
            start_timestamp: Annotated::new(DateTime::from_timestamp_nanos(100).into()),
            end_timestamp: Annotated::new(DateTime::from_timestamp_nanos(100).into()),
            ..Default::default()
        })
        .unwrap();
    }

    #[test]
    fn test_validate_timestamp_start_after_end() {
        let r = validate_timestamps(&SpanV2 {
            start_timestamp: Annotated::new(DateTime::from_timestamp_nanos(101).into()),
            end_timestamp: Annotated::new(DateTime::from_timestamp_nanos(100).into()),
            ..Default::default()
        });
        assert!(r.is_err());
    }

    #[test]
    fn test_validate_timestamp_no_start() {
        let r = validate_timestamps(&SpanV2 {
            start_timestamp: Annotated::empty(),
            end_timestamp: Annotated::new(DateTime::from_timestamp_nanos(100).into()),
            ..Default::default()
        });
        assert!(r.is_err());
    }

    #[test]
    fn test_validate_timestamp_no_end() {
        let r = validate_timestamps(&SpanV2 {
            start_timestamp: Annotated::new(DateTime::from_timestamp_nanos(100).into()),
            end_timestamp: Annotated::empty(),
            ..Default::default()
        });
        assert!(r.is_err());
    }

    #[test]
    fn test_validate_timestamp_no_timestamp() {
        let r = validate_timestamps(&SpanV2 {
            start_timestamp: Annotated::empty(),
            end_timestamp: Annotated::empty(),
            ..Default::default()
        });
        assert!(r.is_err());
    }
    #[test]
    fn test_normalize_span_infers_op() {
        let mut span = Annotated::<SpanV2>::from_json(
            r#"
        {
            "start_timestamp": 1544719859.0,
            "end_timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "name": "db.query",
            "status": "ok",
            "attributes": {
                "db.system.name": {
                    "type": "string",
                    "value": "mysql"
                },
                "db.operation": {
                    "type": "string",
                    "value": "query"
                }
            }
        }"#,
        )
        .unwrap();

        let dsn = "https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"
            .parse()
            .unwrap();
        let request_meta = crate::extractors::RequestMeta::new(dsn);
        let envelope = crate::envelope::Envelope::from_request(None, request_meta);

        normalize_span(
            &mut span,
            envelope.headers(),
            &GeoIpLookup::default(),
            make_context(relay_pii::DataScrubbingConfig::default(), None),
        )
        .unwrap();
        assert_eq!(
            span.value()
                .unwrap()
                .attributes
                .value()
                .unwrap()
                .get_value(consts::OP)
                .unwrap()
                .as_str(),
            Some("db")
        );
    }

    #[test]
    fn test_normalize_mysql_db_query_attributes() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "sentry.op": {
            "type": "string",
            "value": "db.query"
          },
          "sentry.origin": {
            "type": "string",
            "value": "auto.otlp.spans"
          },
          "db.system.name": {
            "type": "string",
            "value": "mysql"
          },
          "db.query.text": {
            "type": "string",
            "value": "SELECT \"not an identifier\""
          }
        }
        "#,
        )
        .unwrap();

        normalize_db_attributes(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "db.operation": {
            "type": "string",
            "value": "SELECT"
          },
          "db.query.text": {
            "type": "string",
            "value": "SELECT \"not an identifier\""
          },
          "db.system.name": {
            "type": "string",
            "value": "mysql"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "SELECT %s"
          },
          "sentry.op": {
            "type": "string",
            "value": "db.query"
          },
          "sentry.origin": {
            "type": "string",
            "value": "auto.otlp.spans"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_mongodb_db_query_attributes() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "sentry.op": {
            "type": "string",
            "value": "db"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.operation": {
            "type": "string",
            "value": "find"
          },
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          }
        }
        "#,
        )
        .unwrap();

        normalize_db_attributes(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          },
          "db.operation": {
            "type": "string",
            "value": "FIND"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "{\"find\":\"documents\",\"foo\":\"?\"}"
          },
          "sentry.op": {
            "type": "string",
            "value": "db"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_db_attributes_does_not_update_attributes_if_already_normalized() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          },
          "db.operation": {
            "type": "string",
            "value": "FIND"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "{\"find\":\"documents\",\"foo\":\"?\"}"
          },
          "sentry.op": {
            "type": "string",
            "value": "db"
          }
        }
        "#,
        )
        .unwrap();

        normalize_db_attributes(&mut attributes);

        insta::assert_json_snapshot!(
            SerializableAnnotated(&attributes), @r#"
        {
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          },
          "db.operation": {
            "type": "string",
            "value": "FIND"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "{\"find\":\"documents\",\"foo\":\"?\"}"
          },
          "sentry.op": {
            "type": "string",
            "value": "db"
          }
        }
        "#
        );
    }

    #[test]
    fn test_normalize_db_attributes_does_not_change_non_db_spans() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "sentry.origin": {
            "type": "string",
            "value": "auto.otlp.spans"
          },
          "http.request.method": {
            "type": "string",
            "value": "GET"
          }
        }
      "#,
        )
        .unwrap();

        normalize_db_attributes(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "http.request.method": {
            "type": "string",
            "value": "GET"
          },
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "sentry.origin": {
            "type": "string",
            "value": "auto.otlp.spans"
          }
        }
        "#);
    }
}
