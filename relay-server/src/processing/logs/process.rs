use relay_event_normalization::eap::ClientUserAgentInfo;
use relay_event_normalization::{RequiredMode, SchemaProcessor, eap};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{OurLog, OurLogHeader};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, EnvelopeHeaders, Item, ItemContainer};
use crate::extractors::RequestTrust;
use crate::managed::Rejected;
use crate::processing::logs::{
    self, Error, ExpandedLogs, LogItems, Result, SerializedLogs, Settings,
};
use crate::processing::{Context, Managed, utils};
use crate::services::outcome::DiscardReason;

/// Parses all serialized logs into their [`ExpandedLogs`] representation.
///
/// Individual, invalid logs will be discarded.
pub fn expand(logs: Managed<SerializedLogs>) -> Result<Managed<ExpandedLogs>, Rejected<Error>> {
    let trust = logs.headers.meta().request_trust();

    logs.try_map(|logs, records| {
        let SerializedLogs {
            headers,
            items,
            invalid,
        } = logs;

        debug_assert!(
            invalid.is_empty(),
            "invalid items should already be rejected"
        );

        // Log byte counts will change here as we go from an estimated count based on the body, to
        // accurately counted bytes.
        records.lenient(DataCategory::LogByte);

        let (settings, logs) = match items {
            LogItems::Container(item) => expand_log_container(&item, trust)?,
            LogItems::Integration(item) => logs::integrations::expand(&[item], records),
        };

        Ok::<_, Error>(ExpandedLogs {
            headers,
            settings,
            logs,
        })
    })
}

/// Normalizes individual log entries.
///
/// Normalization must happen before any filters are applied or other procedures which rely on the
/// presence and well-formedness of attributes and fields.
pub fn normalize(logs: &mut Managed<ExpandedLogs>, ctx: Context<'_>) {
    let settings = logs.settings;

    logs.retain_with_context(
        |logs| (&mut logs.logs, &logs.headers),
        |log, headers, _| {
            normalize_log(log, headers, settings, ctx).inspect_err(|err| {
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

fn expand_log_container(
    item: &Item,
    trust: RequestTrust,
) -> Result<(Settings, ContainerItems<OurLog>)> {
    let (metadata, mut logs) = ItemContainer::parse(item)
        .map_err(|err| {
            relay_log::debug!("failed to parse logs container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_parts();

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

    relay_log::trace!("log container metadata: {metadata:?}");
    let settings = metadata
        .map(|metadata| {
            let is = metadata.ingest_settings.as_ref();

            match metadata.version {
                None | Some(1) => Settings {
                    infer_user_agent: true,
                    ..Default::default()
                },
                // Technically invalid.
                Some(0) => Settings::default(),
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

    Ok((settings, logs))
}

fn scrub_log(log: &mut Annotated<OurLog>, ctx: Context<'_>) -> Result<()> {
    let pii_config_from_scrubbing = ctx.project_info.config.datascrubbing_settings.pii_config();

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
    settings: Settings,
    ctx: Context<'_>,
) -> Result<()> {
    let meta = headers.meta();

    eap::time::normalize(
        log,
        utils::normalize::time_config(headers, |f| f.log.as_ref(), ctx),
    );

    if let Some(log) = log.value_mut() {
        let client_ua_info = settings.infer_user_agent.then(|| ClientUserAgentInfo {
            user_agent: meta.user_agent(),
            hints: meta.client_hints(),
        });

        eap::normalize_attribute_types(&mut log.attributes);
        eap::normalize_attribute_names(&mut log.attributes);
        eap::normalize_received(&mut log.attributes, meta.received_at());
        eap::normalize_client_address(&mut log.attributes, meta.client_addr());
        if settings.infer_ip {
            eap::normalize_inject_client_address(&mut log.attributes, meta.client_addr());
        }
        eap::normalize_user_agent(&mut log.attributes, client_ua_info);
    }

    process_value(
        log,
        &mut SchemaProcessor::new()
            .with_required(RequiredMode::DeleteParent)
            .with_verbose_errors(relay_log::enabled!(relay_log::Level::DEBUG)),
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
