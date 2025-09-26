use relay_event_normalization::{SchemaProcessor, eap};
use relay_event_schema::processor::{ProcessingState, ValueType, process_value};
use relay_event_schema::protocol::{OurLog, OurLogHeader};
use relay_pii::{AttributeMode, PiiProcessor};
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
pub fn expand(logs: Managed<SerializedLogs>, _ctx: Context<'_>) -> Managed<ExpandedLogs> {
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
            // Hard code both retentions for logs launch until we have separate retentions for
            // different data categories.
            #[cfg(feature = "processing")]
            retention: Some(30),
            #[cfg(feature = "processing")]
            downsampled_retention: Some(30),
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

    let state = ProcessingState::root().enter_borrowed("", None, [ValueType::OurLog]);

    if let Some(ref config) = ctx.project_info.config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled())
            // For advanced rules we want to treat attributes as objects.
            .attribute_mode(AttributeMode::Object);
        process_value(log, &mut processor, &state)?;
    }

    if let Some(config) = pii_config_from_scrubbing {
        let mut processor = PiiProcessor::new(config.compiled())
            // For "legacy" rules we want to identify attributes with their values.
            .attribute_mode(AttributeMode::ValueOnly);
        process_value(log, &mut processor, &state)?;
    }

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
    fn test_scrub_log_pii_default_rules() {
        // `user.name`, `sentry.release`, and `url.path` are marked as follows in `sentry-conventions`:
        // * `user.name`: `true`
        // * `sentry.release`: `false`
        // * `url.path`: `maybe`
        // Therefore, `user.name` is the only one that should be scrubbed by default rules.
        let json = r#"{
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log message with sensitive data: user@example.com and 4571234567890111",
            "attributes": {
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

        let ctx = make_context(scrubbing_config, None);
        scrub_log(&mut data, ctx).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data.value().unwrap().attributes), @r###"
        {
          "api_key": {
            "type": "string",
            "value": "[Filtered]"
          },
          "auth_token": {
            "type": "string",
            "value": "[Filtered]"
          },
          "authorization": {
            "type": "string",
            "value": "[Filtered]"
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
            "value": "[Filtered]"
          },
          "private_key": {
            "type": "string",
            "value": "[Filtered]"
          },
          "public_data": {
            "type": "string",
            "value": "public_data"
          },
          "secret": {
            "type": "string",
            "value": "[Filtered]"
          },
          "sentry.release": {
            "type": "string",
            "value": "secret123"
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
          "url.path": {
            "type": "string",
            "value": "secret123"
          },
          "user.name": {
            "type": "string",
            "value": "[Filtered]"
          },
          "user_email": {
            "type": "string",
            "value": "john.doe@example.com"
          },
          "very_sensitive_data": {
            "type": "string",
            "value": "[Filtered]"
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
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 19
                }
              }
            },
            "auth_token": {
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
                  "len": 16
                }
              }
            },
            "authorization": {
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
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 9
                }
              }
            },
            "private_key": {
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
                  "len": 118
                }
              }
            },
            "secret": {
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
            },
            "very_sensitive_data": {
              "value": {
                "": {
                  "rem": [
                    [
                      "strip-fields",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 19
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
    fn test_scrub_log_pii_custom_object_rules() {
        // `user.name`, `sentry.release`, and `url.path` are marked as follows in `sentry-conventions`:
        // * `user.name`: `true`
        // * `sentry.release`: `false`
        // * `url.path`: `maybe`
        // Therefore, `sentry.release` is the only one that should not be scrubbed by custom rules.
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log message with sensitive data: user@example.com and 4571234567890111",
            "attributes": {
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
                ]
            }
        }
        ))
        .unwrap();

        let ctx = make_context(scrubbing_config, Some(config));
        scrub_log(&mut data, ctx).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data.value().unwrap().attributes), @r###"
        {
          "password": {
            "type": "string",
            "value": "default_scrubbing_rules_are_off"
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
    fn test_scrub_log_pii_string_rules() {
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

            let ctx = make_context(scrubbing_config, Some(config));

            scrub_log(&mut data, ctx).unwrap();

            insta::assert_json_snapshot!(SerializableAnnotated(&data.value().unwrap().attributes));
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
    fn test_scrub_log_implicit_attribute_value_does_not_match() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test",
            "attributes": {
                "remove_this_string_abc123": {
                    "type": "string",
                    "value": "abc123"
                }
            }
        }
        "#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

        let config = serde_json::from_value::<PiiConfig>(serde_json::json!({
            "rules": {
                "remove_abc123": {
                    "type": "pattern",
                    "pattern": "abc123",
                    "redaction": {
                        "method": "replace",
                        "text": "abc---"
                    }
                },
            },
            "applications": {
                "$log.attributes.remove_this_string_abc123": ["remove_abc123"], // This selector should NOT match
            }
        }))
        .unwrap();

        let mut scrubbing_config = relay_pii::DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;
        scrubbing_config.scrub_defaults = true;

        let ctx = make_context(scrubbing_config, Some(config));

        scrub_log(&mut data, ctx).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "Test",
          "attributes": {
            "remove_this_string_abc123": {
              "type": "string",
              "value": "abc123"
            }
          }
        }
        "###);
    }

    #[test]
    fn test_scrub_log_explicit_attribute_value_does_match() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test",
            "attributes": {
                "remove_this_string_abc123": {
                    "type": "string",
                    "value": "abc123"
                }
            }
        }
        "#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

        let config = serde_json::from_value::<PiiConfig>(serde_json::json!({
            "rules": {
                "remove_abc123": {
                    "type": "pattern",
                    "pattern": "abc123",
                    "redaction": {
                        "method": "replace",
                        "text": "abc---"
                    }
                },
            },
            "applications": {
                "$log.attributes.remove_this_string_abc123.value": ["remove_abc123"],
            }
        }))
        .unwrap();

        let mut scrubbing_config = relay_pii::DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;
        scrubbing_config.scrub_defaults = true;

        let ctx = make_context(scrubbing_config, Some(config));

        scrub_log(&mut data, ctx).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "timestamp": 1544719860.0,
          "trace_id": "5b8efff798038103d269b633813fc60c",
          "span_id": "eee19b7ec3c1b174",
          "level": "info",
          "body": "Test",
          "attributes": {
            "remove_this_string_abc123": {
              "type": "string",
              "value": "abc---"
            }
          },
          "_meta": {
            "attributes": {
              "remove_this_string_abc123": {
                "value": {
                  "": {
                    "rem": [
                      [
                        "remove_abc123",
                        "s",
                        0,
                        6
                      ]
                    ],
                    "len": 6
                  }
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_scrub_log_sensitive_fields() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log",
            "attributes": {
                "normal_field": {
                    "type": "string",
                    "value": "normal_data"
                },
                "sensitive_custom": {
                    "type": "string",
                    "value": "should_be_removed"
                },
                "another_sensitive": {
                    "type": "integer",
                    "value": 42
                },
                "my_value": {
                    "type": "string",
                    "value": "this_should_be_removed_as_sensitive"
                }
            }
        }
        "#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

        let mut scrubbing_config = DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;
        scrubbing_config.scrub_defaults = false;
        scrubbing_config.scrub_ip_addresses = false;
        scrubbing_config.sensitive_fields = vec![
            "value".to_owned(), // Make sure the inner 'value' of the attribute object isn't scrubbed.
            "sensitive_custom".to_owned(),
            "another_sensitive".to_owned(),
        ];

        let ctx = make_context(scrubbing_config, None);
        scrub_log(&mut data, ctx).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data.value().unwrap().attributes), @r###"
        {
          "another_sensitive": {
            "type": "integer",
            "value": null
          },
          "my_value": {
            "type": "string",
            "value": "[Filtered]"
          },
          "normal_field": {
            "type": "string",
            "value": "normal_data"
          },
          "sensitive_custom": {
            "type": "string",
            "value": "[Filtered]"
          },
          "_meta": {
            "another_sensitive": {
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
            "my_value": {
              "value": {
                "": {
                  "rem": [
                    [
                      "strip-fields",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 35
                }
              }
            },
            "sensitive_custom": {
              "value": {
                "": {
                  "rem": [
                    [
                      "strip-fields",
                      "s",
                      0,
                      10
                    ]
                  ],
                  "len": 17
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_scrub_log_safe_fields() {
        let json = r#"
        {
            "timestamp": 1544719860.0,
            "trace_id": "5b8efff798038103d269b633813fc60c",
            "span_id": "eee19b7ec3c1b174",
            "level": "info",
            "body": "Test log with email@example.com",
            "attributes": {
                "password": {
                    "type": "string",
                    "value": "secret123"
                },
                "credit_card": {
                    "type": "string",
                    "value": "4242424242424242"
                },
                "secret": {
                    "type": "string",
                    "value": "this_should_stay"
                }
            }
        }
        "#;

        let mut data = Annotated::<OurLog>::from_json(json).unwrap();

        let mut scrubbing_config = DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;
        scrubbing_config.scrub_defaults = true;
        scrubbing_config.scrub_ip_addresses = false;
        scrubbing_config.exclude_fields = vec!["secret".to_owned()]; // Only 'secret' is safe
        let ctx = make_context(scrubbing_config, None);
        scrub_log(&mut data, ctx).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data.value().unwrap().attributes), @r###"
        {
          "credit_card": {
            "type": "string",
            "value": "[Filtered]"
          },
          "password": {
            "type": "string",
            "value": "[Filtered]"
          },
          "secret": {
            "type": "string",
            "value": "this_should_stay"
          },
          "_meta": {
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
            "password": {
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
