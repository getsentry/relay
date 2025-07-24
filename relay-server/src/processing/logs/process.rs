use chrono::{DateTime, Utc};
use relay_event_normalization::{
    ClientHints, FromUserAgentInfo as _, RawUserAgentInfo, SchemaProcessor,
};
use relay_event_schema::processor::{ProcessingState, process_value};
use relay_event_schema::protocol::{AttributeType, BrowserContext, OurLog, OurLogHeader};
use relay_ourlogs::OtelLog;
use relay_pii::PiiProcessor;
use relay_protocol::{Annotated, ErrorKind, Value};
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, Item, ItemContainer, WithHeader};
use crate::extractors::{RequestMeta, RequestTrust};
use crate::processing::logs::{Error, ExpandedLogs, Result, SerializedLogs};
use crate::processing::{Context, Managed};
use crate::services::outcome::DiscardReason;

/// Parses all serialized logs into their [`ExpandedLogs`] representation.
///
/// Individual, invalid logs will be discarded.
pub fn expand(logs: Managed<SerializedLogs>, _ctx: Context<'_>) -> Managed<ExpandedLogs> {
    let received_at = logs.received_at();
    let trust = logs.headers.meta().request_trust();

    logs.map(|logs, records| {
        records.lenient(DataCategory::LogByte);

        let mut all_logs = Vec::with_capacity(logs.count());
        for logs in logs.logs {
            let expanded = expand_log_container(&logs, received_at, trust);
            let expanded = records.or_default(expanded, logs);
            all_logs.extend(expanded);
        }

        for otel_log in logs.otel_logs {
            match expand_otel_log(&otel_log, received_at) {
                Ok(log) => all_logs.push(log),
                Err(err) => {
                    records.reject_err(err, otel_log);
                    continue;
                }
            }
        }

        ExpandedLogs {
            headers: logs.headers,
            #[cfg(feature = "processing")]
            retention: _ctx.project_info.config.event_retention,
            #[cfg(feature = "processing")]
            serialize_meta_attrs: _ctx
                .project_info
                .has_feature(relay_dynamic_config::Feature::OurLogsMetaAttributes),
            logs: all_logs,
        }
    })
}

/// Processes expanded logs.
///
/// Validates, scrubs, normalizes and enriches individual log entries.
pub fn process(logs: &mut Managed<ExpandedLogs>, ctx: Context<'_>) {
    logs.modify(|logs, records| {
        let meta = logs.headers.meta();
        logs.logs
            .retain_mut(|log| records.or_default(process_log(log, meta, ctx).map(|_| true), &*log));
    });
}

fn expand_otel_log(item: &Item, received_at: DateTime<Utc>) -> Result<WithHeader<OurLog>> {
    let log = serde_json::from_slice::<OtelLog>(&item.payload()).map_err(|err| {
        relay_log::debug!("failed to parse OTel Log: {err}");
        Error::Invalid(DiscardReason::InvalidJson)
    })?;

    let log = relay_ourlogs::otel_to_sentry_log(log, received_at).map_err(|err| {
        relay_log::debug!("failed to convert OTel Log to Sentry Log: {:?}", err);
        Error::Invalid(DiscardReason::InvalidLog)
    })?;

    // The OTel log conversion already adds certain Sentry attributes which are included in the
    // cost here.
    //
    // As OTel logs are deprecated and to be removed, this is okay for now.
    //
    // See: <https://github.com/getsentry/relay/issues/4884>.
    let byte_size = Some(relay_ourlogs::calculate_size(&log));
    Ok(WithHeader {
        value: Annotated::new(log),
        header: Some(OurLogHeader {
            byte_size,
            other: Default::default(),
        }),
    })
}

fn expand_log_container(
    item: &Item,
    received_at: DateTime<Utc>,
    trust: RequestTrust,
) -> Result<ContainerItems<OurLog>> {
    let mut logs = ItemContainer::parse(item)
        .map_err(|err| {
            relay_log::debug!("failed to parse logs container: {err}");
            Error::Invalid(DiscardReason::InvalidJson)
        })?
        .into_items();

    for log in &mut logs {
        // Calculate the received byte size and remember it as metadata, in the header.
        // As Relay may later modify the payload later, adding attributes or removing them
        // and in any case we want to track the originally received size.
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

        relay_ourlogs::ourlog_merge_otel(log, received_at);
    }

    Ok(logs)
}

fn process_log(log: &mut Annotated<OurLog>, meta: &RequestMeta, ctx: Context<'_>) -> Result<()> {
    scrub(log, ctx).inspect_err(|err| {
        relay_log::debug!("failed to scrub pii from log: {err}");
    })?;

    normalize(log, meta).inspect_err(|err| {
        relay_log::debug!("failed to normalize log: {err}");
    })?;

    Ok(())
}

fn scrub(log: &mut Annotated<OurLog>, ctx: Context<'_>) -> Result<()> {
    let pii_config = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| Error::PiiConfig(e.clone()))?;

    if let Some(ref config) = ctx.project_info.config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(log, &mut processor, ProcessingState::root())?;
    }

    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(log, &mut processor, ProcessingState::root())?;
    }

    Ok(())
}

fn normalize(log: &mut Annotated<OurLog>, meta: &RequestMeta) -> Result<()> {
    process_value(log, &mut SchemaProcessor, ProcessingState::root())?;

    let Some(log) = log.value_mut() else {
        return Err(Error::Invalid(DiscardReason::NoData));
    };

    process_attribute_types(log);
    populate_ua_fields(log, meta.user_agent(), meta.client_hints().as_deref());

    Ok(())
}

fn populate_ua_fields(log: &mut OurLog, user_agent: Option<&str>, client_hints: ClientHints<&str>) {
    let attributes = log.attributes.get_or_insert_with(Default::default);

    const BROWSER_NAME: &str = "sentry.browser.name";
    const BROWSER_VERSION: &str = "sentry.browser.version";

    if attributes.contains_key(BROWSER_NAME) && attributes.contains_key(BROWSER_VERSION) {
        return;
    }

    let Some(context) = BrowserContext::from_hints_or_ua(&RawUserAgentInfo {
        user_agent,
        client_hints,
    }) else {
        return;
    };

    if let Some(name) = context.name.into_value() {
        attributes.insert_if_missing(BROWSER_NAME, || name);
    }
    if let Some(version) = context.version.into_value() {
        attributes.insert_if_missing(BROWSER_VERSION, || version);
    }
}

fn process_attribute_types(ourlog: &mut OurLog) {
    let Some(attributes) = ourlog.attributes.value_mut() else {
        return;
    };

    let attributes = attributes.iter_mut().map(|(_, attr)| attr);

    for attribute in attributes {
        use AttributeType::*;

        let Some(inner) = attribute.value_mut() else {
            continue;
        };

        match (&mut inner.value.ty, &mut inner.value.value) {
            (Annotated(Some(Boolean), _), Annotated(Some(Value::Bool(_)), _)) => (),
            (Annotated(Some(Integer), _), Annotated(Some(Value::I64(_)), _)) => (),
            (Annotated(Some(Integer), _), Annotated(Some(Value::U64(_)), _)) => (),
            (Annotated(Some(Double), _), Annotated(Some(Value::I64(_)), _)) => (),
            (Annotated(Some(Double), _), Annotated(Some(Value::U64(_)), _)) => (),
            (Annotated(Some(Double), _), Annotated(Some(Value::F64(_)), _)) => (),
            (Annotated(Some(String), _), Annotated(Some(Value::String(_)), _)) => (),
            // Note: currently the mapping to Kafka requires that invalid or unknown combinations
            // of types and values are removed from the mapping.
            //
            // Usually Relay would only modify the offending values, but for now, until there
            // is better support in the pipeline here, we need to remove the entire attribute.
            (Annotated(Some(Unknown(_)), _), _) => {
                let original = attribute.value_mut().take();
                attribute.meta_mut().add_error(ErrorKind::InvalidData);
                attribute.meta_mut().set_original_value(original);
            }
            (Annotated(Some(_), _), Annotated(Some(_), _)) => {
                let original = attribute.value_mut().take();
                attribute.meta_mut().add_error(ErrorKind::InvalidData);
                attribute.meta_mut().set_original_value(original);
            }
            (Annotated(None, _), _) | (_, Annotated(None, _)) => {
                let original = attribute.value_mut().take();
                attribute.meta_mut().add_error(ErrorKind::MissingAttribute);
                attribute.meta_mut().set_original_value(original);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::SerializableAnnotated;

    use super::*;

    #[test]
    fn test_process_attribute_types() {
        let json = r#"{
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log message",
            "attributes": {
                "valid_bool": {
                    "type": "boolean",
                    "value": true
                },
                "valid_int_i64": {
                    "type": "integer",
                    "value": -42
                },
                "valid_int_u64": {
                    "type": "integer",
                    "value": 42
                },
                "valid_int_from_string": {
                    "type": "integer",
                    "value": "42"
                },
                "valid_double": {
                    "type": "double",
                    "value": 42.5
                },
                "double_with_i64": {
                    "type": "double",
                    "value": -42
                },
                "valid_double_with_u64": {
                    "type": "double",
                    "value": 42
                },
                "valid_string": {
                    "type": "string",
                    "value": "test"
                },
                "valid_string_with_other": {
                    "type": "string",
                    "value": "test",
                    "some_other_field": "some_other_value"
                },
                "unknown_type": {
                    "type": "custom",
                    "value": "test"
                },
                "invalid_int_from_invalid_string": {
                    "type": "integer",
                    "value": "abc"
                },
                "missing_type": {
                    "value": "value with missing type"
                },
                "missing_value": {
                    "type": "string"
                }
            }
        }"#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

        if let Some(log) = data.value_mut() {
            process_attribute_types(log);
        }

        insta::assert_json_snapshot!(SerializableAnnotated(&data.value().unwrap().attributes), @r###"
        {
          "double_with_i64": {
            "type": "double",
            "value": -42
          },
          "invalid_int_from_invalid_string": null,
          "missing_type": null,
          "missing_value": null,
          "unknown_type": null,
          "valid_bool": {
            "type": "boolean",
            "value": true
          },
          "valid_double": {
            "type": "double",
            "value": 42.5
          },
          "valid_double_with_u64": {
            "type": "double",
            "value": 42
          },
          "valid_int_from_string": null,
          "valid_int_i64": {
            "type": "integer",
            "value": -42
          },
          "valid_int_u64": {
            "type": "integer",
            "value": 42
          },
          "valid_string": {
            "type": "string",
            "value": "test"
          },
          "valid_string_with_other": {
            "type": "string",
            "value": "test",
            "some_other_field": "some_other_value"
          },
          "_meta": {
            "invalid_int_from_invalid_string": {
              "": {
                "err": [
                  "invalid_data"
                ],
                "val": {
                  "type": "integer",
                  "value": "abc"
                }
              }
            },
            "missing_type": {
              "": {
                "err": [
                  "missing_attribute"
                ],
                "val": {
                  "type": null,
                  "value": "value with missing type"
                }
              }
            },
            "missing_value": {
              "": {
                "err": [
                  "missing_attribute"
                ],
                "val": {
                  "type": "string",
                  "value": null
                }
              }
            },
            "unknown_type": {
              "": {
                "err": [
                  "invalid_data"
                ],
                "val": {
                  "type": "custom",
                  "value": "test"
                }
              }
            },
            "valid_int_from_string": {
              "": {
                "err": [
                  "invalid_data"
                ],
                "val": {
                  "type": "integer",
                  "value": "42"
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_populate_ua_fields() {
        let mut log = OurLog::default();
        populate_ua_fields(
            &mut log,
            Some(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ),
            ClientHints::default(),
        );

        let attributes = log.attributes.value().unwrap();
        assert_eq!(
            attributes.get_value("sentry.browser.name").unwrap(),
            &Value::String("Chrome".to_owned()),
        );
        assert_eq!(
            attributes.get_value("sentry.browser.version").unwrap(),
            &Value::String("131.0.0".to_owned()),
        );
    }
}
