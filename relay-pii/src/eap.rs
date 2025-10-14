use relay_event_schema::processor::{
    ProcessValue, ProcessingResult, ProcessingState, ValueType, process_value,
};
use relay_protocol::Annotated;

use crate::{AttributeMode, PiiConfig, PiiProcessor};

/// Scrubs an EAP item (such as an `OurLog` or `SpanV2`) with the given PII configs.
pub fn scrub_eap_item<T: ProcessValue>(
    value_type: ValueType,
    item: &mut Annotated<T>,
    pii_config: Option<&PiiConfig>,
    pii_config_from_scrubbing: Option<&PiiConfig>,
) -> ProcessingResult {
    let state = ProcessingState::root().enter_borrowed("", None, [value_type]);

    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled())
            // For advanced rules we want to treat attributes as objects.
            .attribute_mode(AttributeMode::Object);
        process_value(item, &mut processor, &state)?;
    }

    if let Some(config) = pii_config_from_scrubbing {
        let mut processor = PiiProcessor::new(config.compiled())
            // For "legacy" rules we want to identify attributes with their values.
            .attribute_mode(AttributeMode::ValueOnly);
        process_value(item, &mut processor, &state)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_event_schema::processor::ValueType;
    use relay_event_schema::protocol::{Attributes, OurLog, SpanV2};
    use relay_protocol::{Annotated, SerializableAnnotated};

    use crate::{DataScrubbingConfig, PiiConfig, scrub_eap_item};

    #[test]
    fn test_scrub_attributes_pii_default_rules() {
        // `user.name`, `sentry.release`, and `url.path` are marked as follows in `sentry-conventions`:
        // * `user.name`: `true`
        // * `sentry.release`: `false`
        // * `url.path`: `maybe`
        // Therefore, `user.name` is the only one that should be scrubbed by default rules.
        let json = r#"{
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
        }"#;

        let mut data = Annotated::<Attributes>::from_json(json).unwrap();

        let data_scrubbing_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            scrub_ip_addresses: true,
            exclude_fields: vec!["public_data".to_owned()],
            sensitive_fields: vec![
                "value".to_owned(), // Make sure the inner 'value' of the attribute object isn't scrubbed.
                "very_sensitive_data".to_owned(),
            ],
            ..Default::default()
        };

        let scrubbing_config = data_scrubbing_config.pii_config().unwrap();

        scrub_eap_item(ValueType::Span, &mut data, None, scrubbing_config.as_ref()).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
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
          "sentry.description": {
            "type": "string",
            "value": "secret123"
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
    fn test_scrub_attributes_pii_custom_object_rules() {
        // `user.name`, `sentry.release`, and `url.path` are marked as follows in `sentry-conventions`:
        // * `user.name`: `true`
        // * `sentry.release`: `false`
        // * `url.path`: `maybe`
        // Therefore, `sentry.release` is the only one that should not be scrubbed by custom rules.
        let json = r#"
        {
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
        }"#;

        let mut data = Annotated::<Attributes>::from_json(json).unwrap();

        let scrubbing_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: false,
            scrub_ip_addresses: false,
            ..Default::default()
        };

        let scrubbing_config = scrubbing_config.pii_config().unwrap();

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

        scrub_eap_item(
            ValueType::Span,
            &mut data,
            Some(&config),
            scrubbing_config.as_ref(),
        )
        .unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
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
    fn test_scrub_attributes_sensitive_fields() {
        let json = r#"
        {
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
        "#;

        let mut data = Annotated::<Attributes>::from_json(json).unwrap();

        let scrubbing_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: false,
            scrub_ip_addresses: false,
            sensitive_fields: vec![
                "value".to_owned(), // Make sure the inner 'value' of the attribute object isn't scrubbed.
                "sensitive_custom".to_owned(),
                "another_sensitive".to_owned(),
            ],
            ..Default::default()
        };

        let scrubbing_config = scrubbing_config.pii_config().unwrap();

        scrub_eap_item(ValueType::Span, &mut data, None, scrubbing_config.as_ref()).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
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
    fn test_scrub_attributes_safe_fields() {
        let json = r#"
        {
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
        "#;

        let mut data = Annotated::<Attributes>::from_json(json).unwrap();

        let scrubbing_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            scrub_ip_addresses: false,
            exclude_fields: vec!["secret".to_owned()], // Only 'secret' is safe
            ..Default::default()
        };

        let scrubbing_config = scrubbing_config.pii_config().unwrap();

        scrub_eap_item(ValueType::Span, &mut data, None, scrubbing_config.as_ref()).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
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
    fn test_scrub_attributes_implicit_attribute_value() {
        let json = r#"
        {
            "remove_this_string_abc123": {
                "type": "string",
                "value": "abc123"
            }
        }
        "#;

        let mut data = Annotated::<Attributes>::from_json(json).unwrap();

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
                "remove_this_string_abc123": ["remove_abc123"], // This selector should NOT match
            }
        }))
        .unwrap();

        let scrubbing_config = DataScrubbingConfig {
            scrub_data: true,
            scrub_defaults: true,
            ..Default::default()
        };

        let scrubbing_config = scrubbing_config.pii_config().unwrap();

        scrub_eap_item(
            ValueType::OurLog,
            &mut data,
            Some(&config),
            scrubbing_config.as_ref(),
        )
        .unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "remove_this_string_abc123": {
            "type": "string",
            "value": "abc123"
          }
        }
        "###);

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
                "remove_this_string_abc123.value": ["remove_abc123"],
            }
        }))
        .unwrap();

        scrub_eap_item(
            ValueType::OurLog,
            &mut data,
            Some(&config),
            scrubbing_config.as_ref(),
        )
        .unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&data), @r###"
        {
          "remove_this_string_abc123": {
            "type": "string",
            "value": "abc---"
          },
          "_meta": {
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
        "###);
    }

    macro_rules! attribute_rule_test {
        ($test_name:ident, $rule_type:expr, $test_value:expr, @$snapshot:literal) => {
            #[test]
            fn $test_name() {
                let config = serde_json::from_value::<PiiConfig>(serde_json::json!({
                    "applications": {
                        "$string": [$rule_type]
                    }
                }))
                .unwrap();

                let mut scrubbing_config = crate::DataScrubbingConfig::default();
                scrubbing_config.scrub_data = true;
                scrubbing_config.scrub_defaults = false;
                scrubbing_config.scrub_ip_addresses = false;

                let scrubbing_config = scrubbing_config.pii_config().unwrap();

                let span_json = format!(r#"{{
                    "start_timestamp": 1544719859.0,
                    "end_timestamp": 1544719860.0,
                    "trace_id": "5b8efff798038103d269b633813fc60c",
                    "span_id": "eee19b7ec3c1b174",
                    "name": "test",
                    "attributes": {{
                        "{rule_type}|{value}": {{
                            "type": "string",
                            "value": "{value}"
                        }}
                    }}
                }}"#,
                rule_type = $rule_type,
                value = $test_value
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                );

                let mut span = Annotated::<SpanV2>::from_json(&span_json).unwrap();

                crate::scrub_eap_item(ValueType::Span, &mut span, Some(&config), scrubbing_config.as_ref()).unwrap();

                insta::allow_duplicates!(insta::assert_json_snapshot!(SerializableAnnotated(&span.value().unwrap().attributes), @$snapshot));

                let log_json = format!(r#"{{
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
                rule_type = $rule_type,
                value = $test_value
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n")
                );

                let mut log = Annotated::<OurLog>::from_json(&log_json).unwrap();

                crate::scrub_eap_item(ValueType::OurLog, &mut log, Some(&config), scrubbing_config.as_ref()).unwrap();

                insta::allow_duplicates!(insta::assert_json_snapshot!(SerializableAnnotated(&log.value().unwrap().attributes), @$snapshot));
            }
        };
    }

    // IP rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_ip, "@ip", "127.0.0.1", @r###"
    {
      "@ip|127.0.0.1": {
        "type": "string",
        "value": "[ip]"
      },
      "_meta": {
        "@ip|127.0.0.1": {
          "value": {
            "": {
              "rem": [
                [
                  "@ip",
                  "s",
                  0,
                  4
                ]
              ],
              "len": 9
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_ip_replace, "@ip:replace", "127.0.0.1", @r###"
    {
      "@ip:replace|127.0.0.1": {
        "type": "string",
        "value": "[ip]"
      },
      "_meta": {
        "@ip:replace|127.0.0.1": {
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
              "len": 9
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_ip_remove, "@ip:remove", "127.0.0.1", @r###"
    {
      "@ip:remove|127.0.0.1": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@ip:remove|127.0.0.1": {
          "value": {
            "": {
              "rem": [
                [
                  "@ip:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 9
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_ip_mask, "@ip:mask", "127.0.0.1", @r###"
    {
      "@ip:mask|127.0.0.1": {
        "type": "string",
        "value": "*********"
      },
      "_meta": {
        "@ip:mask|127.0.0.1": {
          "value": {
            "": {
              "rem": [
                [
                  "@ip:mask",
                  "m",
                  0,
                  9
                ]
              ],
              "len": 9
            }
          }
        }
      }
    }
    "###);

    // Email rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_email, "@email", "test@example.com", @r###"
    {
      "@email|test@example.com": {
        "type": "string",
        "value": "[email]"
      },
      "_meta": {
        "@email|test@example.com": {
          "value": {
            "": {
              "rem": [
                [
                  "@email",
                  "s",
                  0,
                  7
                ]
              ],
              "len": 16
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_email_replace, "@email:replace", "test@example.com", @r###"
    {
      "@email:replace|test@example.com": {
        "type": "string",
        "value": "[email]"
      },
      "_meta": {
        "@email:replace|test@example.com": {
          "value": {
            "": {
              "rem": [
                [
                  "@email:replace",
                  "s",
                  0,
                  7
                ]
              ],
              "len": 16
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_email_remove, "@email:remove", "test@example.com", @r###"
    {
      "@email:remove|test@example.com": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@email:remove|test@example.com": {
          "value": {
            "": {
              "rem": [
                [
                  "@email:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 16
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_email_mask, "@email:mask", "test@example.com", @r###"
    {
      "@email:mask|test@example.com": {
        "type": "string",
        "value": "****************"
      },
      "_meta": {
        "@email:mask|test@example.com": {
          "value": {
            "": {
              "rem": [
                [
                  "@email:mask",
                  "m",
                  0,
                  16
                ]
              ],
              "len": 16
            }
          }
        }
      }
    }
    "###);

    // Credit card rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_creditcard, "@creditcard", "4242424242424242", @r###"
    {
      "@creditcard|4242424242424242": {
        "type": "string",
        "value": "[creditcard]"
      },
      "_meta": {
        "@creditcard|4242424242424242": {
          "value": {
            "": {
              "rem": [
                [
                  "@creditcard",
                  "s",
                  0,
                  12
                ]
              ],
              "len": 16
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_creditcard_replace, "@creditcard:replace", "4242424242424242", @r###"
    {
      "@creditcard:replace|4242424242424242": {
        "type": "string",
        "value": "[creditcard]"
      },
      "_meta": {
        "@creditcard:replace|4242424242424242": {
          "value": {
            "": {
              "rem": [
                [
                  "@creditcard:replace",
                  "s",
                  0,
                  12
                ]
              ],
              "len": 16
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_creditcard_remove, "@creditcard:remove", "4242424242424242", @r###"
    {
      "@creditcard:remove|4242424242424242": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@creditcard:remove|4242424242424242": {
          "value": {
            "": {
              "rem": [
                [
                  "@creditcard:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 16
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_creditcard_mask, "@creditcard:mask", "4242424242424242", @r###"
    {
      "@creditcard:mask|4242424242424242": {
        "type": "string",
        "value": "****************"
      },
      "_meta": {
        "@creditcard:mask|4242424242424242": {
          "value": {
            "": {
              "rem": [
                [
                  "@creditcard:mask",
                  "m",
                  0,
                  16
                ]
              ],
              "len": 16
            }
          }
        }
      }
    }
    "###);

    // IBAN rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_iban, "@iban", "DE89370400440532013000", @r###"
    {
      "@iban|DE89370400440532013000": {
        "type": "string",
        "value": "[iban]"
      },
      "_meta": {
        "@iban|DE89370400440532013000": {
          "value": {
            "": {
              "rem": [
                [
                  "@iban",
                  "s",
                  0,
                  6
                ]
              ],
              "len": 22
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_iban_replace, "@iban:replace", "DE89370400440532013000", @r###"
    {
      "@iban:replace|DE89370400440532013000": {
        "type": "string",
        "value": "[iban]"
      },
      "_meta": {
        "@iban:replace|DE89370400440532013000": {
          "value": {
            "": {
              "rem": [
                [
                  "@iban:replace",
                  "s",
                  0,
                  6
                ]
              ],
              "len": 22
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_iban_remove, "@iban:remove", "DE89370400440532013000", @r###"
    {
      "@iban:remove|DE89370400440532013000": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@iban:remove|DE89370400440532013000": {
          "value": {
            "": {
              "rem": [
                [
                  "@iban:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 22
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_iban_mask, "@iban:mask", "DE89370400440532013000", @r###"
    {
      "@iban:mask|DE89370400440532013000": {
        "type": "string",
        "value": "**********************"
      },
      "_meta": {
        "@iban:mask|DE89370400440532013000": {
          "value": {
            "": {
              "rem": [
                [
                  "@iban:mask",
                  "m",
                  0,
                  22
                ]
              ],
              "len": 22
            }
          }
        }
      }
    }
    "###);

    // MAC address rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_mac, "@mac", "4a:00:04:10:9b:50", @r###"
    {
      "@mac|4a:00:04:10:9b:50": {
        "type": "string",
        "value": "*****************"
      },
      "_meta": {
        "@mac|4a:00:04:10:9b:50": {
          "value": {
            "": {
              "rem": [
                [
                  "@mac",
                  "m",
                  0,
                  17
                ]
              ],
              "len": 17
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_mac_replace, "@mac:replace", "4a:00:04:10:9b:50", @r###"
    {
      "@mac:replace|4a:00:04:10:9b:50": {
        "type": "string",
        "value": "[mac]"
      },
      "_meta": {
        "@mac:replace|4a:00:04:10:9b:50": {
          "value": {
            "": {
              "rem": [
                [
                  "@mac:replace",
                  "s",
                  0,
                  5
                ]
              ],
              "len": 17
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_mac_remove, "@mac:remove", "4a:00:04:10:9b:50", @r###"
    {
      "@mac:remove|4a:00:04:10:9b:50": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@mac:remove|4a:00:04:10:9b:50": {
          "value": {
            "": {
              "rem": [
                [
                  "@mac:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 17
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_mac_mask, "@mac:mask", "4a:00:04:10:9b:50", @r###"
    {
      "@mac:mask|4a:00:04:10:9b:50": {
        "type": "string",
        "value": "*****************"
      },
      "_meta": {
        "@mac:mask|4a:00:04:10:9b:50": {
          "value": {
            "": {
              "rem": [
                [
                  "@mac:mask",
                  "m",
                  0,
                  17
                ]
              ],
              "len": 17
            }
          }
        }
      }
    }
    "###);

    // UUID rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_uuid, "@uuid", "ceee0822-ed8f-4622-b2a3-789e73e75cd1", @r###"
    {
      "@uuid|ceee0822-ed8f-4622-b2a3-789e73e75cd1": {
        "type": "string",
        "value": "************************************"
      },
      "_meta": {
        "@uuid|ceee0822-ed8f-4622-b2a3-789e73e75cd1": {
          "value": {
            "": {
              "rem": [
                [
                  "@uuid",
                  "m",
                  0,
                  36
                ]
              ],
              "len": 36
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_uuid_replace, "@uuid:replace", "ceee0822-ed8f-4622-b2a3-789e73e75cd1", @r###"
    {
      "@uuid:replace|ceee0822-ed8f-4622-b2a3-789e73e75cd1": {
        "type": "string",
        "value": "[uuid]"
      },
      "_meta": {
        "@uuid:replace|ceee0822-ed8f-4622-b2a3-789e73e75cd1": {
          "value": {
            "": {
              "rem": [
                [
                  "@uuid:replace",
                  "s",
                  0,
                  6
                ]
              ],
              "len": 36
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_uuid_remove, "@uuid:remove", "ceee0822-ed8f-4622-b2a3-789e73e75cd1", @r###"
    {
      "@uuid:remove|ceee0822-ed8f-4622-b2a3-789e73e75cd1": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@uuid:remove|ceee0822-ed8f-4622-b2a3-789e73e75cd1": {
          "value": {
            "": {
              "rem": [
                [
                  "@uuid:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 36
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_uuid_mask, "@uuid:mask", "ceee0822-ed8f-4622-b2a3-789e73e75cd1", @r###"
    {
      "@uuid:mask|ceee0822-ed8f-4622-b2a3-789e73e75cd1": {
        "type": "string",
        "value": "************************************"
      },
      "_meta": {
        "@uuid:mask|ceee0822-ed8f-4622-b2a3-789e73e75cd1": {
          "value": {
            "": {
              "rem": [
                [
                  "@uuid:mask",
                  "m",
                  0,
                  36
                ]
              ],
              "len": 36
            }
          }
        }
      }
    }
    "###);

    // IMEI rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_imei, "@imei", "356938035643809", @r###"
    {
      "@imei|356938035643809": {
        "type": "string",
        "value": "[imei]"
      },
      "_meta": {
        "@imei|356938035643809": {
          "value": {
            "": {
              "rem": [
                [
                  "@imei",
                  "s",
                  0,
                  6
                ]
              ],
              "len": 15
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_imei_replace, "@imei:replace", "356938035643809", @r###"
    {
      "@imei:replace|356938035643809": {
        "type": "string",
        "value": "[imei]"
      },
      "_meta": {
        "@imei:replace|356938035643809": {
          "value": {
            "": {
              "rem": [
                [
                  "@imei:replace",
                  "s",
                  0,
                  6
                ]
              ],
              "len": 15
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_imei_remove, "@imei:remove", "356938035643809", @r###"
    {
      "@imei:remove|356938035643809": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@imei:remove|356938035643809": {
          "value": {
            "": {
              "rem": [
                [
                  "@imei:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 15
            }
          }
        }
      }
    }
    "###);

    // PEM key rules
    attribute_rule_test!(
        test_scrub_attributes_pii_string_rules_pemkey,
        "@pemkey",
        "-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----",
    @r###"
    {
      "@pemkey|-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----": {
        "type": "string",
        "value": "-----BEGIN EC PRIVATE KEY-----\n[pemkey]\n-----END EC PRIVATE KEY-----"
      },
      "_meta": {
        "@pemkey|-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----": {
          "value": {
            "": {
              "rem": [
                [
                  "@pemkey",
                  "s",
                  31,
                  39
                ]
              ],
              "len": 124
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(
        test_scrub_attributes_pii_string_rules_pemkey_replace,
        "@pemkey:replace",
        "-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----",
    @r###"
    {
      "@pemkey:replace|-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----": {
        "type": "string",
        "value": "-----BEGIN EC PRIVATE KEY-----\n[pemkey]\n-----END EC PRIVATE KEY-----"
      },
      "_meta": {
        "@pemkey:replace|-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----": {
          "value": {
            "": {
              "rem": [
                [
                  "@pemkey:replace",
                  "s",
                  31,
                  39
                ]
              ],
              "len": 124
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(
        test_scrub_attributes_pii_string_rules_pemkey_remove,
        "@pemkey:remove",
        "-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----",
    @r###"
    {
      "@pemkey:remove|-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----": {
        "type": "string",
        "value": "-----BEGIN EC PRIVATE KEY-----\n\n-----END EC PRIVATE KEY-----"
      },
      "_meta": {
        "@pemkey:remove|-----BEGIN EC PRIVATE KEY-----\nMIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL\n-----END EC PRIVATE KEY-----": {
          "value": {
            "": {
              "rem": [
                [
                  "@pemkey:remove",
                  "x",
                  31,
                  31
                ]
              ],
              "len": 124
            }
          }
        }
      }
    }
    "###);

    // URL auth rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_urlauth, "@urlauth", "https://username:password@example.com/", @r###"
    {
      "@urlauth|https://username:password@example.com/": {
        "type": "string",
        "value": "https://[auth]@example.com/"
      },
      "_meta": {
        "@urlauth|https://username:password@example.com/": {
          "value": {
            "": {
              "rem": [
                [
                  "@urlauth",
                  "s",
                  8,
                  14
                ]
              ],
              "len": 38
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_urlauth_replace, "@urlauth:replace", "https://username:password@example.com/", @r###"
    {
      "@urlauth:replace|https://username:password@example.com/": {
        "type": "string",
        "value": "https://[auth]@example.com/"
      },
      "_meta": {
        "@urlauth:replace|https://username:password@example.com/": {
          "value": {
            "": {
              "rem": [
                [
                  "@urlauth:replace",
                  "s",
                  8,
                  14
                ]
              ],
              "len": 38
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_urlauth_remove, "@urlauth:remove", "https://username:password@example.com/", @r###"
    {
      "@urlauth:remove|https://username:password@example.com/": {
        "type": "string",
        "value": "https://@example.com/"
      },
      "_meta": {
        "@urlauth:remove|https://username:password@example.com/": {
          "value": {
            "": {
              "rem": [
                [
                  "@urlauth:remove",
                  "x",
                  8,
                  8
                ]
              ],
              "len": 38
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_urlauth_mask, "@urlauth:mask", "https://username:password@example.com/", @r###"
    {
      "@urlauth:mask|https://username:password@example.com/": {
        "type": "string",
        "value": "https://*****************@example.com/"
      },
      "_meta": {
        "@urlauth:mask|https://username:password@example.com/": {
          "value": {
            "": {
              "rem": [
                [
                  "@urlauth:mask",
                  "m",
                  8,
                  25
                ]
              ],
              "len": 38
            }
          }
        }
      }
    }
    "###);

    // US SSN rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_usssn, "@usssn", "078-05-1120", @r###"
    {
      "@usssn|078-05-1120": {
        "type": "string",
        "value": "***********"
      },
      "_meta": {
        "@usssn|078-05-1120": {
          "value": {
            "": {
              "rem": [
                [
                  "@usssn",
                  "m",
                  0,
                  11
                ]
              ],
              "len": 11
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_usssn_replace, "@usssn:replace", "078-05-1120", @r###"
    {
      "@usssn:replace|078-05-1120": {
        "type": "string",
        "value": "[us-ssn]"
      },
      "_meta": {
        "@usssn:replace|078-05-1120": {
          "value": {
            "": {
              "rem": [
                [
                  "@usssn:replace",
                  "s",
                  0,
                  8
                ]
              ],
              "len": 11
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_usssn_remove, "@usssn:remove", "078-05-1120", @r###"
    {
      "@usssn:remove|078-05-1120": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@usssn:remove|078-05-1120": {
          "value": {
            "": {
              "rem": [
                [
                  "@usssn:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 11
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_usssn_mask, "@usssn:mask", "078-05-1120", @r###"
    {
      "@usssn:mask|078-05-1120": {
        "type": "string",
        "value": "***********"
      },
      "_meta": {
        "@usssn:mask|078-05-1120": {
          "value": {
            "": {
              "rem": [
                [
                  "@usssn:mask",
                  "m",
                  0,
                  11
                ]
              ],
              "len": 11
            }
          }
        }
      }
    }
    "###);

    // User path rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_userpath, "@userpath", "/Users/john/Documents", @r###"
    {
      "@userpath|/Users/john/Documents": {
        "type": "string",
        "value": "/Users/[user]/Documents"
      },
      "_meta": {
        "@userpath|/Users/john/Documents": {
          "value": {
            "": {
              "rem": [
                [
                  "@userpath",
                  "s",
                  7,
                  13
                ]
              ],
              "len": 21
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_userpath_replace, "@userpath:replace", "/Users/john/Documents", @r###"
    {
      "@userpath:replace|/Users/john/Documents": {
        "type": "string",
        "value": "/Users/[user]/Documents"
      },
      "_meta": {
        "@userpath:replace|/Users/john/Documents": {
          "value": {
            "": {
              "rem": [
                [
                  "@userpath:replace",
                  "s",
                  7,
                  13
                ]
              ],
              "len": 21
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_userpath_remove, "@userpath:remove", "/Users/john/Documents", @r###"
    {
      "@userpath:remove|/Users/john/Documents": {
        "type": "string",
        "value": "/Users//Documents"
      },
      "_meta": {
        "@userpath:remove|/Users/john/Documents": {
          "value": {
            "": {
              "rem": [
                [
                  "@userpath:remove",
                  "x",
                  7,
                  7
                ]
              ],
              "len": 21
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_userpath_mask, "@userpath:mask", "/Users/john/Documents", @r###"
    {
      "@userpath:mask|/Users/john/Documents": {
        "type": "string",
        "value": "/Users/****/Documents"
      },
      "_meta": {
        "@userpath:mask|/Users/john/Documents": {
          "value": {
            "": {
              "rem": [
                [
                  "@userpath:mask",
                  "m",
                  7,
                  11
                ]
              ],
              "len": 21
            }
          }
        }
      }
    }
    "###);

    // Password rules
    // @password defaults to remove
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_password, "@password", "my_password_123", @r###"
    {
      "@password|my_password_123": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@password|my_password_123": {
          "value": {
            "": {
              "rem": [
                [
                  "@password",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 15
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_password_remove, "@password:remove", "my_password_123", @r###"
    {
      "@password:remove|my_password_123": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@password:remove|my_password_123": {
          "value": {
            "": {
              "rem": [
                [
                  "@password:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 15
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_password_replace, "@password:replace", "my_password_123", @r###"
    {
      "@password:replace|my_password_123": {
        "type": "string",
        "value": "[password]"
      },
      "_meta": {
        "@password:replace|my_password_123": {
          "value": {
            "": {
              "rem": [
                [
                  "@password:replace",
                  "s",
                  0,
                  10
                ]
              ],
              "len": 15
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(test_scrub_attributes_pii_string_rules_password_mask, "@password:mask", "my_password_123", @r###"
    {
      "@password:mask|my_password_123": {
        "type": "string",
        "value": "***************"
      },
      "_meta": {
        "@password:mask|my_password_123": {
          "value": {
            "": {
              "rem": [
                [
                  "@password:mask",
                  "m",
                  0,
                  15
                ]
              ],
              "len": 15
            }
          }
        }
      }
    }
    "###);

    // Bearer token rules
    attribute_rule_test!(test_scrub_attributes_pii_string_rules_bearer, "@bearer", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", @r###"
    {
      "@bearer|Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9": {
        "type": "string",
        "value": "Bearer [token]"
      },
      "_meta": {
        "@bearer|Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9": {
          "value": {
            "": {
              "rem": [
                [
                  "@bearer",
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
    }
    "###);

    attribute_rule_test!(
        test_scrub_attributes_pii_string_rules_bearer_replace,
        "@bearer:replace",
        "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
    @r###"
    {
      "@bearer:replace|Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9": {
        "type": "string",
        "value": "Bearer [token]"
      },
      "_meta": {
        "@bearer:replace|Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9": {
          "value": {
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
    }
    "###);

    attribute_rule_test!(
        test_scrub_attributes_pii_string_rules_bearer_remove,
        "@bearer:remove",
        "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
    @r###"
    {
      "@bearer:remove|Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9": {
        "type": "string",
        "value": ""
      },
      "_meta": {
        "@bearer:remove|Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9": {
          "value": {
            "": {
              "rem": [
                [
                  "@bearer:remove",
                  "x",
                  0,
                  0
                ]
              ],
              "len": 43
            }
          }
        }
      }
    }
    "###);

    attribute_rule_test!(
        test_scrub_attributes_pii_string_rules_bearer_mask, 
        "@bearer:mask",
        "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
    @r###"
    {
      "@bearer:mask|Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9": {
        "type": "string",
        "value": "*******************************************"
      },
      "_meta": {
        "@bearer:mask|Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9": {
          "value": {
            "": {
              "rem": [
                [
                  "@bearer:mask",
                  "m",
                  0,
                  43
                ]
              ],
              "len": 43
            }
          }
        }
      }
    }
    "###);
}
