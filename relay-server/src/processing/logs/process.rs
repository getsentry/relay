use relay_event_normalization::{RequiredMode, SchemaProcessor, eap};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{OurLog, OurLogHeader};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, ItemContainer};
use crate::extractors::RequestTrust;
use crate::processing::logs::{self, Error, ExpandedLogs, Result, SerializedLogs};
use crate::processing::{Context, Managed, utils};
use crate::services::outcome::DiscardReason;

/// Parses all serialized logs into their [`ExpandedLogs`] representation.
///
/// Individual, invalid logs will be discarded.
pub fn expand(logs: Managed<SerializedLogs>) -> Managed<ExpandedLogs> {
    let trust = logs.headers.meta().request_trust();

    logs.map(|logs, records| {
        records.lenient(DataCategory::LogByte);

        let mut all_logs = Vec::new();
        for logs in logs.logs {
            let expanded = expand_log_container(&logs, trust);
            let expanded = records.or_default(expanded, logs);
            all_logs.extend(expanded);
        }

        logs::integrations::expand_into(&mut all_logs, records, logs.integrations);

        ExpandedLogs {
            headers: logs.headers,
            logs: all_logs,
        }
    })
}

/// Normalizes individual log entries.
///
/// Normalization must happen before any filters are applied or other procedures which rely on the
/// presence and well-formedness of attributes and fields.
pub fn normalize(logs: &mut Managed<ExpandedLogs>, ctx: Context<'_>) {
    logs.retain_with_context(
        |logs| (&mut logs.logs, &logs.headers),
        |log, headers, _| {
            normalize_log(log, headers, ctx).inspect_err(|err| {
                relay_log::debug!("failed to normalize log: {err}");
            })
        },
    );
}

/// Applies PII scrubbing to individual log entries.
pub fn scrub(logs: &mut Managed<ExpandedLogs>, ctx: Context<'_>) {
    logs.retain(
        |logs| &mut logs.logs,
        |log, _| {
            scrub_log(log, ctx)
                .inspect_err(|err| relay_log::debug!("failed to scrub pii from log: {err}"))
        },
    );
}

fn expand_log_container(item: &Item, trust: RequestTrust) -> Result<ContainerItems<OurLog>> {
    let mut logs = ItemContainer::parse(item)
        .map_err(|err| {
            relay_log::debug!("failed to parse logs container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_items();

    for log in &mut logs {
        // Calculate the received byte size and remember it as metadata, in the header.
        // This is to keep track of the originally received size even as Relay adds, removes or
        // modifies attributes.
        //
        // Since Relay can be deployed in multiple layers, we need to remember the size of the
        // first Relay which received the log, but at the same time we must be able to trust that
        // size.
        //
        // If there is no size calculated or we cannot trust the source, we re-calculate the size.
        let byte_size = log.header.as_ref().and_then(|h: &OurLogHeader| h.byte_size);
        if trust.is_untrusted() || matches!(byte_size, None | Some(0)) {
            let byte_size = log.value().map(relay_ourlogs::calculate_size).unwrap_or(1);
            log.header.get_or_insert_default().byte_size = Some(byte_size);
        }
    }

    Ok(logs)
}

fn scrub_log(log: &mut Annotated<OurLog>, ctx: Context<'_>) -> Result<()> {
    let pii_config_from_scrubbing = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| Error::PiiConfig(e.clone()))?;

    relay_pii::eap::scrub(
        ValueType::OurLog,
        log,
        ctx.project_info.config.pii_config.as_ref(),
        pii_config_from_scrubbing.as_ref(),
    )?;

    Ok(())
}

fn normalize_log(
    log: &mut Annotated<OurLog>,
    headers: &EnvelopeHeaders,
    ctx: Context<'_>,
) -> Result<()> {
    let meta = headers.meta();

    eap::time::normalize(
        log,
        utils::normalize::time_config(headers, |f| f.log.as_ref(), ctx),
    );

    if let Some(log) = log.value_mut() {
        eap::normalize_attribute_types(&mut log.attributes);
        eap::normalize_attribute_names(&mut log.attributes);
        eap::normalize_received(&mut log.attributes, meta.received_at());
        eap::normalize_client_address(&mut log.attributes, meta.client_addr());
        eap::normalize_user_agent(&mut log.attributes, meta.user_agent(), meta.client_hints());
    }

    process_value(
        log,
        &mut SchemaProcessor::new().with_required(RequiredMode::DeleteParent),
        ProcessingState::root(),
    )?;

    if let Annotated(None, meta) = log {
        relay_log::debug!("empty log: {meta:?}");
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
    fn test_scrub_log_base_fields() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            "attributes": {}
        }
        "#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

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

        scrub_log(&mut data, ctx).unwrap();

        assert_annotated_snapshot!(data, @r#"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "Bearer [token]",
          "attributes": {},
          "_meta": {
            "body": {
              "": {
                "rem": [
                  [
                    "@bearer:replace",
                    "s",
                    0,
                    14
                  ]
                ],
                "len": 43
              }
            }
          }
        }
        "#);
    }

    #[test]
    fn test_scrub_log_deep_wild_cards() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "normal_value",
            "attributes": {
                "normal_field": {
                    "type": "string",
                    "value": "normal_value"
                }
            }
        }
        "#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

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

        scrub_log(&mut data, ctx).unwrap();

        assert_annotated_snapshot!(data, @r#"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "normal_value",
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

        // If a log specific negation is used, then log attributes appear again.
        data = Annotated::<OurLog>::from_json(json).unwrap();
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
                "** && !$log.**": ["should_not_remove_normal_field"]
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

        scrub_log(&mut data, ctx).unwrap();

        assert_annotated_snapshot!(data, @r###"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "normal_value",
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
    fn test_scrub_log_deep_wild_cards_anything() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "normal_value",
            "attributes": {
                "normal_field": {
                    "type": "string",
                    "value": "normal_value"
                }
            }
        }
        "#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

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

        scrub_log(&mut data, ctx).unwrap();

        assert_annotated_snapshot!(data, @r#"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "normal_value",
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
