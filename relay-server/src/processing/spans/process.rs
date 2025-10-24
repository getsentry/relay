use relay_event_normalization::{
    GeoIpLookup, RequiredMode, SchemaProcessor, TimestampProcessor, TrimmingProcessor, eap,
};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{Span, SpanV2};
use relay_protocol::Annotated;

use crate::envelope::{ContainerItems, Item, ItemContainer, WithHeader};
use crate::extractors::RequestMeta;
use crate::managed::Managed;
use crate::processing::Context;
use crate::processing::spans::{self, Error, ExpandedSpans, Result, SampledSpans};
use crate::services::outcome::DiscardReason;

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
pub fn normalize(spans: &mut Managed<ExpandedSpans>, geo_lookup: &GeoIpLookup) {
    spans.retain_with_context(
        |spans| (&mut spans.spans, spans.headers.meta()),
        |span, meta, _| {
            normalize_span(span, meta, geo_lookup).inspect_err(|err| {
                relay_log::debug!("failed to normalize span: {err}");
            })
        },
    );
}

fn normalize_span(
    span: &mut Annotated<SpanV2>,
    meta: &RequestMeta,
    geo_lookup: &GeoIpLookup,
) -> Result<()> {
    process_value(span, &mut TimestampProcessor, ProcessingState::root())?;

    // TODO: `validate_span()` (start/end timestamps)

    if let Some(span) = span.value_mut() {
        eap::normalize_received(&mut span.attributes, meta.received_at());
        eap::normalize_user_agent(&mut span.attributes, meta.user_agent(), meta.client_hints());
        eap::normalize_attribute_types(&mut span.attributes);
        eap::normalize_user_geo(&mut span.attributes, || {
            meta.client_addr().and_then(|ip| geo_lookup.lookup(ip))
        });

        // TODO: ai model costs
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

#[cfg(test)]
mod tests {
    use relay_pii::{DataScrubbingConfig, PiiConfig};
    use relay_protocol::SerializableAnnotated;

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

        Context {
            config,
            global_config,
            project_info,
            rate_limits,
            sampling_project_info: None,
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
}
