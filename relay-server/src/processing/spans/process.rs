use std::collections::BTreeMap;
use std::time::Duration;

use relay_event_normalization::{
    GeoIpLookup, RequiredMode, SchemaProcessor, TrimmingProcessor, eap,
};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{Span, SpanId, SpanV2};
use relay_protocol::Annotated;

use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, ItemContainer, ParentId, WithHeader};
use crate::managed::Managed;
use crate::processing::spans::{
    self, Error, ExpandedAttachment, ExpandedSpan, ExpandedSpans, Result, SerializedSpans,
};
use crate::processing::{Context, trace_attachments, utils};
use crate::services::outcome::DiscardReason;

/// Parses all serialized spans.
///
/// Individual, invalid spans are discarded.
pub fn expand(spans: Managed<SerializedSpans>) -> Managed<ExpandedSpans> {
    spans.map(|spans, records| {
        let SerializedSpans {
            headers,
            spans,
            legacy,
            integrations,
            attachments,
        } = spans;

        let mut all_spans = Vec::new();

        for item in &spans {
            let expanded = expand_span_container(item);
            let expanded = records.or_default(expanded, item);
            all_spans.extend(expanded);
        }

        for item in &legacy {
            match expand_legacy_span(item) {
                Ok(span) => all_spans.push(span),
                Err(err) => drop(records.reject_err(err, item)),
            }
        }

        spans::integrations::expand_into(&mut all_spans, records, integrations);

        let mut span_id_mapping: BTreeMap<_, _> = BTreeMap::new();
        for span in all_spans {
            if let Some(id) = span.value().and_then(|span| span.span_id.value().copied()) {
                // Although span_ids should be unique it could be that they collied in which case we
                // want to drop one of the offending spans.
                if let Some(old_span) = span_id_mapping.insert(id, ExpandedSpan::new(span)) {
                    relay_log::debug!("span id collision");
                    records.reject_err(Error::Invalid(DiscardReason::Duplicate), old_span);
                }
            } else {
                relay_log::debug!("failed to extract span id from span");
                records.reject_err(Error::Invalid(DiscardReason::InvalidSpan), span);
            }
        }

        let mut stand_alone_attachments: Vec<ExpandedAttachment> = Vec::new();
        for attachment in attachments {
            match parse_and_validate_span_attachment(&attachment) {
                Ok((None, attachment)) => {
                    stand_alone_attachments.push(attachment);
                }
                Ok((Some(span_id), attachment)) => {
                    if let Some(entry) = span_id_mapping.get_mut(&span_id) {
                        entry.attachments.push(attachment);
                    } else {
                        relay_log::debug!("span attachment invalid associated span id");
                        records.reject_err(
                            Error::Invalid(DiscardReason::InvalidSpanAttachment),
                            attachment,
                        );
                    }
                }
                Err(err) => {
                    records.reject_err(err, attachment);
                }
            }
        }

        ExpandedSpans {
            headers,
            spans: span_id_mapping.into_values().collect(),
            server_sample_rate: None,
            stand_alone_attachments,
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

/// Parses and validates a span attachment, converting it into a structured type.
fn parse_and_validate_span_attachment(item: &Item) -> Result<(Option<SpanId>, ExpandedAttachment)> {
    let associated_span_id = match item.parent_id() {
        Some(ParentId::SpanId(span_id)) => *span_id,
        None => {
            relay_log::debug!("span attachment missing associated span id");
            return Err(Error::Invalid(DiscardReason::InvalidSpanAttachment));
        }
    };

    let expanded_attachment =
        trace_attachments::process::parse_and_validate(item).map_err(Error::Invalid)?;

    Ok((associated_span_id, expanded_attachment))
}

/// Normalizes individual spans.
pub fn normalize(spans: &mut Managed<ExpandedSpans>, geo_lookup: &GeoIpLookup, ctx: Context<'_>) {
    spans.retain_with_context(
        |spans| (&mut spans.spans, &spans.headers),
        |span, headers, _| {
            normalize_span(&mut span.span, headers, geo_lookup, ctx).inspect_err(|err| {
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
    let meta = headers.meta();

    eap::time::normalize(
        span,
        utils::normalize::time_config(headers, |f| f.span.as_ref(), ctx),
    );

    if let Some(span) = span.value_mut() {
        let dsc = headers.dsc();
        let duration = span_duration(span);
        let model_costs = ctx.global_config.ai_model_costs.as_ref().ok();
        let allowed_hosts = ctx.global_config.options.http_span_allowed_hosts.as_slice();

        validate_timestamps(span)?;

        // normalize_sentry_op must be called before normalize_span_category
        // because category derivation depends on having the sentry.op attribute
        // available.
        eap::normalize_sentry_op(&mut span.attributes);
        eap::normalize_span_category(&mut span.attributes);
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
        if ctx.is_processing() {
            eap::normalize_ai(&mut span.attributes, duration, model_costs);
        }
        eap::normalize_attribute_values(&mut span.attributes, allowed_hosts);
        eap::write_legacy_attributes(&mut span.attributes);
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
        |span, r| {
            scrub_span(&mut span.span, ctx)
                .inspect_err(|err| relay_log::debug!("failed to scrub pii from span: {err}"))?;

            span.attachments.retain_mut(|attachment| {
                match trace_attachments::process::scrub_attachment(attachment, ctx) {
                    Ok(()) => true,
                    Err(err) => {
                        relay_log::debug!("failed to scrub pii from span attachment: {err}");
                        r.reject_err(Error::from(err), &*attachment);
                        false
                    }
                }
            });

            Ok::<(), Error>(())
        },
    );

    spans.retain(
        |spans| &mut spans.stand_alone_attachments,
        |attachment, _| {
            trace_attachments::process::scrub_attachment(attachment, ctx)
                .inspect_err(|err| {
                    relay_log::debug!("failed to scrub pii from span attachment: {err}")
                })
                .map_err(Error::from)
        },
    );
}

fn scrub_span(span: &mut Annotated<SpanV2>, ctx: Context<'_>) -> Result<()> {
    let pii_config_from_scrubbing = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|_| Error::PiiConfig)?;

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
    use relay_conventions::{
        DB_QUERY_TEXT, DB_SYSTEM, DB_SYSTEM_NAME, DESCRIPTION, HTTP_REQUEST_METHOD, OP,
        SENTRY_ACTION, SENTRY_CATEGORY, SENTRY_DOMAIN, SENTRY_NORMALIZED_DESCRIPTION, URL_FULL,
    };
    use relay_event_schema::protocol::{Attributes, EventId, SpanKind};
    use relay_pii::PiiConfig;
    use relay_protocol::SerializableAnnotated;

    use crate::Envelope;
    use crate::extractors::RequestMeta;
    use crate::services::projects::project::ProjectInfo;

    use super::*;

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

        let mut datascrubbing_settings = relay_pii::DataScrubbingConfig::default();
        datascrubbing_settings.scrub_data = true;
        datascrubbing_settings.scrub_defaults = true;
        datascrubbing_settings.scrub_ip_addresses = true;
        datascrubbing_settings.sensitive_fields = vec![
            "value".to_owned(), // Make sure the inner 'value' of the attribute object isn't scrubbed.
            "very_sensitive_data".to_owned(),
        ];
        datascrubbing_settings.exclude_fields = vec!["public_data".to_owned()];

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

        let mut datascrubbing_settings = relay_pii::DataScrubbingConfig::default();
        datascrubbing_settings.scrub_data = true;
        datascrubbing_settings.scrub_defaults = false;
        datascrubbing_settings.scrub_ip_addresses = false;

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

        let ctx = Context {
            project_info: &ProjectInfo {
                config: relay_dynamic_config::ProjectConfig {
                    pii_config: Some(config),
                    datascrubbing_settings,
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Context::for_test()
        };
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

    fn prepare_normalize_span_params(
        string_attributes: &[(&str, &str)],
        float_attributes: &[(&str, f64)],
    ) -> (
        Annotated<SpanV2>,
        EnvelopeHeaders,
        GeoIpLookup,
        Context<'static>,
    ) {
        let mut attributes = Attributes::new();
        string_attributes
            .iter()
            .for_each(|(key, value)| attributes.insert(*key, value.to_owned()));
        float_attributes
            .iter()
            .for_each(|(key, value)| attributes.insert(*key, *value));
        let attrs_json =
            serde_json::to_string(&SerializableAnnotated(&Annotated::new(attributes))).unwrap();
        let span_json = format!(
            r#"{{
             "trace_id": "5b8efff798038103d269b633813fc60c",
             "span_id": "eee19b7ec3c1b175",
             "start_timestamp": 1715000000.0,
             "end_timestamp": 1715000001.0,
             "name": "test",
             "status": "ok",
             "attributes": {attrs_json}
         }}"#
        );
        let span = Annotated::<SpanV2>::from_json(&span_json).unwrap();

        let dsn = "https://a94ae32be2584e0bbd7a4cbb95971fee:@sentry.io/42"
            .parse()
            .unwrap();
        let meta = RequestMeta::new(dsn);
        let envelope = Envelope::from_request(Some(EventId::new()), meta);
        let headers = envelope.headers().to_owned();

        (span, headers, GeoIpLookup::empty(), Context::for_test())
    }

    fn assert_attributes_contains(
        span: &Annotated<SpanV2>,
        string_attributes: &[(&str, &str)],
        float_attributes: &[(&str, f64)],
    ) {
        let attrs = span.value().unwrap().attributes.value().unwrap();
        string_attributes.iter().for_each(|(key, value)| {
            assert_eq!(attrs.get_value(*key).and_then(|v| v.as_str()), Some(*value),)
        });
        float_attributes.iter().for_each(|(key, value)| {
            assert_eq!(attrs.get_value(*key).and_then(|v| v.as_f64()), Some(*value),)
        });
    }

    #[test]
    fn test_insights_backend_queries_support_modern() {
        let (mut span, headers, geo_lookup, ctx) = prepare_normalize_span_params(
            &[
                (DB_SYSTEM_NAME, "postgresql"),
                (DB_QUERY_TEXT, "select * from users where id = 1"),
            ],
            &[],
        );

        normalize_span(&mut span, &headers, &geo_lookup, ctx).unwrap();

        assert_attributes_contains(
            &span,
            &[
                (DESCRIPTION, "select * from users where id = 1"),
                (
                    SENTRY_NORMALIZED_DESCRIPTION,
                    "SELECT * FROM users WHERE id = %s",
                ),
                (SENTRY_CATEGORY, "db"),
                (DB_SYSTEM, "postgresql"),
                (SENTRY_ACTION, "SELECT"),
                (SENTRY_DOMAIN, ",users,"),
            ],
            &[],
        );
    }

    #[test]
    fn test_insights_backend_queries_support_legacy() {
        let (mut span, headers, geo_lookup, ctx) = prepare_normalize_span_params(
            &[
                (DB_SYSTEM, "postgresql"),
                (DESCRIPTION, "select * from users where id = 1"),
            ],
            &[],
        );

        normalize_span(&mut span, &headers, &geo_lookup, ctx).unwrap();

        assert_attributes_contains(
            &span,
            &[
                (DESCRIPTION, "select * from users where id = 1"),
                (
                    SENTRY_NORMALIZED_DESCRIPTION,
                    "SELECT * FROM users WHERE id = %s",
                ),
                (SENTRY_CATEGORY, "db"),
                (DB_SYSTEM, "postgresql"),
                (SENTRY_ACTION, "SELECT"),
                (SENTRY_DOMAIN, ",users,"),
            ],
            &[],
        );
    }

    #[test]
    fn test_insights_backend_outbound_api_requests_support_modern() {
        let (mut span, headers, geo_lookup, ctx) = prepare_normalize_span_params(
            &[
                ("sentry.kind", SpanKind::Client.as_str()),
                (HTTP_REQUEST_METHOD, "GET"),
                (URL_FULL, "https://www.example.com/path?param=value"),
            ],
            &[("http.response.status_code", 502.)],
        );

        normalize_span(&mut span, &headers, &geo_lookup, ctx).unwrap();

        assert_attributes_contains(
            &span,
            &[
                (SENTRY_CATEGORY, "http"),
                (OP, "http.client"),
                (DESCRIPTION, "GET https://www.example.com/path?param=value"),
                (SENTRY_ACTION, "GET"),
                (SENTRY_DOMAIN, "*.example.com"),
            ],
            &[("sentry.status_code", 502.)],
        );
    }

    #[test]
    fn test_insights_backend_outbound_api_requests_support_legacy_absolute() {
        let (mut span, headers, geo_lookup, ctx) = prepare_normalize_span_params(
            &[
                (OP, "http.client"),
                (DESCRIPTION, "GET https://www.example.com/path?param=value"),
            ],
            &[("http.response.status_code", 502.)],
        );

        normalize_span(&mut span, &headers, &geo_lookup, ctx).unwrap();

        assert_attributes_contains(
            &span,
            &[
                (SENTRY_CATEGORY, "http"),
                (OP, "http.client"),
                (DESCRIPTION, "GET https://www.example.com/path?param=value"),
                (SENTRY_ACTION, "GET"),
                (SENTRY_DOMAIN, "*.example.com"),
            ],
            &[("sentry.status_code", 502.)],
        );
    }

    #[test]
    fn test_insights_backend_outbound_api_requests_support_legacy_relative() {
        let (mut span, headers, geo_lookup, ctx) = prepare_normalize_span_params(
            &[
                (OP, "http.client"),
                (DESCRIPTION, "GET /path?param=value"),
                ("server.address", "www.example.com"),
            ],
            &[("http.response.status_code", 502.)],
        );

        normalize_span(&mut span, &headers, &geo_lookup, ctx).unwrap();

        assert_attributes_contains(
            &span,
            &[
                (SENTRY_CATEGORY, "http"),
                (OP, "http.client"),
                (DESCRIPTION, "GET /path?param=value"),
                (SENTRY_ACTION, "GET"),
                (SENTRY_DOMAIN, "*.example.com"),
            ],
            &[("sentry.status_code", 502.)],
        );
    }
}
