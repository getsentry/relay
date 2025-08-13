use relay_event_normalization::{
    ClientHints, FromUserAgentInfo as _, RawUserAgentInfo, SchemaProcessor,
};
use relay_event_schema::processor::{ProcessingState, process_value};
use relay_event_schema::protocol::{AttributeType, BrowserContext, OurLog, OurLogHeader};
use relay_metrics::UnixTimestamp;
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
    let trust = logs.headers.meta().request_trust();

    logs.map(|logs, records| {
        records.lenient(DataCategory::LogByte);

        let mut all_logs = Vec::with_capacity(logs.count());
        for logs in logs.logs {
            let expanded = expand_log_container(&logs, trust);
            let expanded = records.or_default(expanded, logs);
            all_logs.extend(expanded);
        }

        for otel_log in logs.otel_logs {
            match expand_otel_log(&otel_log) {
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
            downsampled_retention: _ctx.project_info.config.downsampled_event_retention,
            logs: all_logs,
        }
    })
}

/// Normalizes individual log entries.
///
/// Normalization must happen before any filters are applied or other procedures which rely on the
/// presence and well-formedness of attributes and fields.
pub fn normalize(logs: &mut Managed<ExpandedLogs>) {
    logs.modify(|logs, records| {
        let meta = logs.headers.meta();
        logs.logs.retain_mut(|log| {
            let r = normalize_log(log, meta).inspect_err(|err| {
                relay_log::debug!("failed to normalize log: {err}");
            });

            records.or_default(r.map(|_| true), &*log)
        })
    });
}

/// Applies PII scrubbing to individual log entries.
pub fn scrub(logs: &mut Managed<ExpandedLogs>, ctx: Context<'_>) {
    logs.modify(|logs, records| {
        logs.logs.retain_mut(|log| {
            let r = scrub_log(log, ctx).inspect_err(|err| {
                relay_log::debug!("failed to scrub pii from log: {err}");
            });

            records.or_default(r.map(|_| true), &*log)
        })
    });
}

fn expand_otel_log(item: &Item) -> Result<WithHeader<OurLog>> {
    let log = serde_json::from_slice::<OtelLog>(&item.payload()).map_err(|err| {
        relay_log::debug!("failed to parse OTel Log: {err}");
        Error::Invalid(DiscardReason::InvalidJson)
    })?;

    let log = relay_ourlogs::otel_to_sentry_log(log).map_err(|err| {
        relay_log::debug!("failed to convert OTel Log to Sentry Log: {:?}", err);
        Error::Invalid(DiscardReason::InvalidLog)
    })?;

    let byte_size = Some(relay_ourlogs::calculate_size(&log));
    Ok(WithHeader {
        value: Annotated::new(log),
        header: Some(OurLogHeader {
            byte_size,
            other: Default::default(),
        }),
    })
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

fn normalize_log(log: &mut Annotated<OurLog>, meta: &RequestMeta) -> Result<()> {
    process_value(log, &mut SchemaProcessor, ProcessingState::root())?;

    let Some(log) = log.value_mut() else {
        return Err(Error::Invalid(DiscardReason::NoData));
    };

    log.attributes
        .get_or_insert_with(Default::default)
        .insert_if_missing("sentry.observed_timestamp_nanos", || {
            meta.received_at()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| UnixTimestamp::now().as_nanos() as i64)
                .to_string()
        });

    populate_ua_fields(log, meta.user_agent(), meta.client_hints().as_deref());
    process_attribute_types(log);

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
    use relay_pii::PiiConfig;
    use relay_protocol::SerializableAnnotated;

    use crate::services::projects::project::ProjectInfo;

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

    #[test]
    fn test_scrub_log_pii() {
        let json = r#"{
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log message with sensitive data: user@example.com and 4571234567890111",
            "attributes": {
                "password": {
                    "type": "string",
                    "value": "secret123"
                },
                "secret": {
                    "type": "string",
                    "value": "topsecret"
                },
                "api_key": {
                    "type": "string",
                    "value": "sk-1234567890abcdef"
                },
                "auth_token": {
                    "type": "string",
                    "value": "bearer_token_123"
                },
                "credit_card": {
                    "type": "string",
                    "value": "4571234567890111"
                },
                "visa_card": {
                    "type": "string",
                    "value": "4571 2345 6789 0111"
                },
                "bank_account": {
                    "type": "string",
                    "value": "DE89370400440532013000"
                },
                "iban_code": {
                    "type": "string",
                    "value": "NO9386011117945"
                },
                "authorization": {
                    "type": "string",
                    "value": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
                },
                "private_key": {
                    "type": "string",
                    "value": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKB\n-----END PRIVATE KEY-----"
                },
                "database_url": {
                    "type": "string",
                    "value": "https://username:password@example.com/db"
                },
                "file_path": {
                    "type": "string",
                    "value": "C:\\Users\\john\\Documents\\secret.txt"
                },
                "unix_path": {
                    "type": "string",
                    "value": "/home/alice/private/data.json"
                },
                "social_security": {
                    "type": "string",
                    "value": "078-05-1120"
                },
                "user_email": {
                    "type": "string",
                    "value": "john.doe@example.com"
                },
                "client_ip": {
                    "type": "string",
                    "value": "192.168.1.100"
                },
                "ipv6_addr": {
                    "type": "string",
                    "value": "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
                },
                "mac_address": {
                    "type": "string",
                    "value": "4a:00:04:10:9b:50"
                },
                "session_id": {
                    "type": "string",
                    "value": "ceee0822-ed8f-4622-b2a3-789e73e75cd1"
                },
                "device_imei": {
                    "type": "string",
                    "value": "356938035643809"
                },
                "public_data": {
                    "type": "string",
                    "value": "public_data"
                },
                "very_sensitive_data": {
                    "type": "string",
                    "value": "very_sensitive_data"
                }
            }
        }"#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

        let mut scrubbing_config = relay_pii::DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;
        scrubbing_config.scrub_defaults = true;
        scrubbing_config.scrub_ip_addresses = true;
        scrubbing_config.sensitive_fields = vec![
            "value".to_owned(), // Make sure the inner 'value' of the attribute object isn't scrubbed.
            "very_sensitive_data".to_owned(),
        ];
        scrubbing_config.exclude_fields = vec!["public_data".to_owned()];

        let context = Context {
            config: &relay_config::Config::default(),
            global_config: &relay_dynamic_config::GlobalConfig::default(),
            project_info: &ProjectInfo {
                config: relay_dynamic_config::ProjectConfig {
                    datascrubbing_settings: scrubbing_config,
                    ..Default::default()
                },
                ..Default::default()
            },
            rate_limits: &relay_quotas::RateLimits::default(),
        };
        scrub_log(&mut data, context).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data.value().unwrap().attributes), @r###"
        {
          "api_key": {
            "type": "string",
            "value": null
          },
          "auth_token": {
            "type": "string",
            "value": null
          },
          "authorization": {
            "type": "string",
            "value": null
          },
          "bank_account": {
            "type": "string",
            "value": "[Filtered]"
          },
          "client_ip": {
            "type": "string",
            "value": "[ip]"
          },
          "credit_card": {
            "type": "string",
            "value": "[Filtered]"
          },
          "database_url": {
            "type": "string",
            "value": "[Filtered]"
          },
          "device_imei": {
            "type": "string",
            "value": "356938035643809"
          },
          "file_path": {
            "type": "string",
            "value": "[Filtered]"
          },
          "iban_code": {
            "type": "string",
            "value": "[Filtered]"
          },
          "ipv6_addr": {
            "type": "string",
            "value": "[ip]"
          },
          "mac_address": {
            "type": "string",
            "value": "4a:00:04:10:9b:50"
          },
          "password": {
            "type": "string",
            "value": null
          },
          "private_key": {
            "type": "string",
            "value": null
          },
          "public_data": {
            "type": "string",
            "value": "public_data"
          },
          "secret": {
            "type": "string",
            "value": null
          },
          "session_id": {
            "type": "string",
            "value": "ceee0822-ed8f-4622-b2a3-789e73e75cd1"
          },
          "social_security": {
            "type": "string",
            "value": "[Filtered]"
          },
          "unix_path": {
            "type": "string",
            "value": "/home/alice/private/data.json"
          },
          "user_email": {
            "type": "string",
            "value": "john.doe@example.com"
          },
          "very_sensitive_data": {
            "type": "string",
            "value": null
          },
          "visa_card": {
            "type": "string",
            "value": "[Filtered]"
          },
          "_meta": {
            "api_key": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@password:filter",
                      "x"
                    ]
                  ]
                }
              }
            },
            "auth_token": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@password:filter",
                      "x"
                    ]
                  ]
                }
              }
            },
            "authorization": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@password:filter",
                      "x"
                    ]
                  ]
                }
              }
            },
            "bank_account": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@iban:filter",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 22
                }
              }
            },
            "client_ip": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@ip:replace",
                      "s",
                      0,
                      4
                    ]
                  ],
                  "len": 13
                }
              }
            },
            "credit_card": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@creditcard:filter",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 16
                }
              }
            },
            "database_url": {
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
                  "len": 40
                }
              }
            },
            "file_path": {
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
                  "len": 34
                }
              }
            },
            "iban_code": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@iban:filter",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 15
                }
              }
            },
            "ipv6_addr": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@ip:replace",
                      "s",
                      0,
                      4
                    ]
                  ],
                  "len": 39
                }
              }
            },
            "password": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@password:filter",
                      "x"
                    ]
                  ]
                }
              }
            },
            "private_key": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@password:filter",
                      "x"
                    ]
                  ]
                }
              }
            },
            "secret": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@password:filter",
                      "x"
                    ]
                  ]
                }
              }
            },
            "social_security": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@usssn:filter",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 11
                }
              }
            },
            "very_sensitive_data": {
              "value": {
                "": {
                  "rem": [
                    [
                      "strip-fields",
                      "x"
                    ]
                  ]
                }
              }
            },
            "visa_card": {
              "value": {
                "": {
                  "rem": [
                    [
                      "@creditcard:filter",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 19
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_scrub_log_pii_custom_rules() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log message with sensitive data: user@example.com and 4571234567890111",
            "attributes": {
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
        }"#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

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
                }
            },
            "applications": {
                "test_field_mac": [
                    "project:0"
                ],
                "test_field_imei": [
                    "project:1"
                ],
                "test_field_uuid": [
                    "project:2"
                ],
                "test_field_regex_passes || test_field_regex_fails": [
                    "project:3"
                ]
            }
        }
        ))
        .unwrap();

        let context = Context {
            config: &relay_config::Config::default(),
            global_config: &relay_dynamic_config::GlobalConfig::default(),
            project_info: &ProjectInfo {
                config: relay_dynamic_config::ProjectConfig {
                    pii_config: Some(config),
                    datascrubbing_settings: scrubbing_config,
                    ..Default::default()
                },
                ..Default::default()
            },
            rate_limits: &relay_quotas::RateLimits::default(),
        };
        scrub_log(&mut data, context).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data.value().unwrap().attributes), @r###"
        {
          "password": {
            "type": "string",
            "value": "default_scrubbing_rules_are_off"
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
          "_meta": {
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
            }
          }
        }
        "###);
    }

    #[test]
    fn test_string_pii_scrubbing_rules() {
        let test_cases = vec![
            // IP rules
            ("@ip", "127.0.0.1"),
            ("@ip:replace", "127.0.0.1"),
            ("@ip:remove", "127.0.0.1"),
            ("@ip:mask", "127.0.0.1"),
            // Email rules
            ("@email", "test@example.com"),
            ("@email:replace", "test@example.com"),
            ("@email:remove", "test@example.com"),
            ("@email:mask", "test@example.com"),
            // Credit card rules
            ("@creditcard", "4242424242424242"),
            ("@creditcard:replace", "4242424242424242"),
            ("@creditcard:remove", "4242424242424242"),
            ("@creditcard:mask", "4242424242424242"),
            // IBAN rules
            ("@iban", "DE89370400440532013000"),
            ("@iban:replace", "DE89370400440532013000"),
            ("@iban:remove", "DE89370400440532013000"),
            ("@iban:mask", "DE89370400440532013000"),
            // MAC address rules
            ("@mac", "4a:00:04:10:9b:50"),
            ("@mac:replace", "4a:00:04:10:9b:50"),
            ("@mac:remove", "4a:00:04:10:9b:50"),
            ("@mac:mask", "4a:00:04:10:9b:50"),
            // UUID rules
            ("@uuid", "ceee0822-ed8f-4622-b2a3-789e73e75cd1"),
            ("@uuid:replace", "ceee0822-ed8f-4622-b2a3-789e73e75cd1"),
            ("@uuid:remove", "ceee0822-ed8f-4622-b2a3-789e73e75cd1"),
            ("@uuid:mask", "ceee0822-ed8f-4622-b2a3-789e73e75cd1"),
            // IMEI rules
            ("@imei", "356938035643809"),
            ("@imei:replace", "356938035643809"),
            ("@imei:remove", "356938035643809"),
            // PEM key rules
            (
                "@pemkey",
                "-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----",
            ),
            (
                "@pemkey:replace",
                "-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----",
            ),
            (
                "@pemkey:remove",
                "-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----",
            ),
            // URL auth rules
            ("@urlauth", "https://username:password@example.com/"),
            ("@urlauth:replace", "https://username:password@example.com/"),
            ("@urlauth:remove", "https://username:password@example.com/"),
            ("@urlauth:mask", "https://username:password@example.com/"),
            // US SSN rules
            ("@usssn", "078-05-1120"),
            ("@usssn:replace", "078-05-1120"),
            ("@usssn:remove", "078-05-1120"),
            ("@usssn:mask", "078-05-1120"),
            // User path rules
            ("@userpath", "/Users/john/Documents"),
            ("@userpath:replace", "/Users/john/Documents"),
            ("@userpath:remove", "/Users/john/Documents"),
            ("@userpath:mask", "/Users/john/Documents"),
            // Password rules
            ("@password", "my_password_123"), // @password defaults to remove
            ("@password:remove", "my_password_123"),
            ("@password:replace", "my_password_123"),
            ("@password:mask", "my_password_123"),
            // Bearer token rules
            ("@bearer", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"),
            (
                "@bearer:replace",
                "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            ),
            (
                "@bearer:remove",
                "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            ),
            (
                "@bearer:mask",
                "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
            ),
        ];

        for (rule_type, test_value) in test_cases {
            let json = format!(
                r#"{{
                "timestamp": 1544719860.0,
                "trace_id": "5b8efff798038103d269b633813fc60c",
                "span_id": "eee19b7ec3c1b174",
                "level": "info",
                "body": "Test log",
                "attributes": {{
                    "{rule_type}|{value}": {{
                        "type": "string",
                        "value": "{value}"
                    }}
                }}
            }}"#,
                rule_type = rule_type,
                value = test_value
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
            );

            let mut data = Annotated::<OurLog>::from_json(&json).unwrap();

            let config = serde_json::from_value::<PiiConfig>(serde_json::json!({
                "applications": {
                    "$string": [rule_type]
                }
            }))
            .unwrap();

            let mut scrubbing_config = relay_pii::DataScrubbingConfig::default();
            scrubbing_config.scrub_data = true;
            scrubbing_config.scrub_defaults = false;
            scrubbing_config.scrub_ip_addresses = false;

            let context = Context {
                config: &relay_config::Config::default(),
                global_config: &relay_dynamic_config::GlobalConfig::default(),
                project_info: &ProjectInfo {
                    config: relay_dynamic_config::ProjectConfig {
                        pii_config: Some(config),
                        datascrubbing_settings: scrubbing_config,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                rate_limits: &relay_quotas::RateLimits::default(),
            };

            scrub_log(&mut data, context).unwrap();

            insta::assert_json_snapshot!(SerializableAnnotated(&data.value().unwrap().attributes));
        }
    }
}
