use relay_event_normalization::{SchemaProcessor, eap};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{OurLog, OurLogHeader};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, Item, ItemContainer};
use crate::extractors::{RequestMeta, RequestTrust};
use crate::processing::logs::{self, Error, ExpandedLogs, Result, SerializedLogs};
use crate::processing::{Context, Managed};
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
pub fn normalize(logs: &mut Managed<ExpandedLogs>) {
    logs.retain_with_context(
        |logs| (&mut logs.logs, logs.headers.meta()),
        |log, meta, _| {
            normalize_log(log, meta).inspect_err(|err| {
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

fn normalize_log(log: &mut Annotated<OurLog>, meta: &RequestMeta) -> Result<()> {
    process_value(log, &mut SchemaProcessor, ProcessingState::root())?;

    let Some(log) = log.value_mut() else {
        return Err(Error::Invalid(DiscardReason::NoData));
    };

    eap::normalize_received(&mut log.attributes, meta.received_at());
    eap::normalize_user_agent(&mut log.attributes, meta.user_agent(), meta.client_hints());
    eap::normalize_attribute_types(&mut log.attributes);

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

        let config = serde_json::from_value::<PiiConfig>(serde_json::json!({
            "applications": {
                "$string": ["@password:filter"],
            }
        }))
        .unwrap();

        let mut scrubbing_config = relay_pii::DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;

        let ctx = make_context(scrubbing_config, Some(config));
        scrub_log(&mut data, ctx).unwrap();
        insta::assert_json_snapshot!(SerializableAnnotated(&data));
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

        let ctx = make_context(DataScrubbingConfig::default(), Some(deep_wildcard_config));
        scrub_log(&mut data, ctx).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "[REDACTED]",
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
            },
            "body": {
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
        "###);

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

        let ctx = make_context(DataScrubbingConfig::default(), Some(config));
        scrub_log(&mut data, ctx).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
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

        let ctx = make_context(DataScrubbingConfig::default(), Some(config));
        scrub_log(&mut data, ctx).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "[REDACTED]",
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
            },
            "body": {
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
        "###);
    }
}
