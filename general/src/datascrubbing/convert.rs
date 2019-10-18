use std::collections::BTreeMap;

use regex::RegexBuilder;

use crate::datascrubbing::DataScrubbingConfig;
use crate::pii::{Pattern, PiiConfig, RedactPairRule, Redaction, RuleSpec, RuleType};
use crate::processor::{SelectorPathItem, SelectorSpec, ValueType};

lazy_static::lazy_static! {
    // XXX: Move to @ip rule for better IP address scrubbing. Right now we just try to keep
    // compatibility with Python.
    static ref KNOWN_IP_FIELDS: SelectorSpec = "($request.env.REMOTE_ADDR | $user.ip_address | $sdk.client_ip)".parse().unwrap();
}

pub fn to_pii_config(datascrubbing_config: &DataScrubbingConfig) -> Option<PiiConfig> {
    let mut custom_rules = BTreeMap::new();
    let mut applied_rules = Vec::new();
    let mut applications = BTreeMap::new();

    if datascrubbing_config.scrub_data && datascrubbing_config.scrub_defaults {
        applied_rules.push("@common:filter".to_owned());
    }

    if datascrubbing_config.scrub_ip_addresses {
        applications.insert(KNOWN_IP_FIELDS.clone(), vec!["@anything:remove".to_owned()]);
    }

    if datascrubbing_config.scrub_data {
        let mut sensitive_fields = datascrubbing_config
            .sensitive_fields
            .iter()
            .map(|x| x.trim())
            .filter(|x| !x.is_empty())
            .peekable();

        let sensitive_fields_re = if sensitive_fields.peek().is_some() {
            let mut re = ".*(".to_owned();

            for (idx, field) in sensitive_fields.enumerate() {
                if idx != 0 {
                    re.push('|');
                }

                // ugly: regex::escape returns owned string
                re.push_str(&regex::escape(field));
            }

            re.push_str(").*");
            Some(re)
        } else {
            None
        };

        if let Some(key_pattern) = sensitive_fields_re {
            custom_rules.insert(
                "strip-fields".to_owned(),
                RuleSpec {
                    ty: RuleType::RedactPair(RedactPairRule {
                        key_pattern: Pattern(
                            RegexBuilder::new(&key_pattern)
                                .case_insensitive(true)
                                .build()
                                .unwrap(),
                        ),
                    }),
                    redaction: Redaction::Replace("[Filtered]".to_owned().into()),
                },
            );

            applied_rules.push("strip-fields".to_owned());
        }
    }

    if applied_rules.is_empty() && applications.is_empty() {
        return None;
    }

    let mut applied_selector = SelectorSpec::Or(vec![
        SelectorSpec::from(ValueType::String),
        SelectorSpec::from(ValueType::Number),
        SelectorSpec::from(ValueType::Array),
    ]);

    let mut exclude_fields = datascrubbing_config
        .exclude_fields
        .iter()
        .map(|x| x.trim())
        .filter(|x| !x.is_empty())
        .peekable();

    if exclude_fields.peek().is_some() {
        let mut conjunctions = vec![applied_selector];

        for field in exclude_fields {
            conjunctions.push(SelectorSpec::Not(Box::new(SelectorSpec::Path(vec![
                SelectorPathItem::Key(field.to_owned()),
            ]))));
        }

        applied_selector = SelectorSpec::And(conjunctions);
    }

    if !applied_rules.is_empty() {
        applications.insert(applied_selector, applied_rules.clone());
    }

    Some(PiiConfig {
        rules: custom_rules,
        vars: Default::default(),
        applications,
    })
}

#[cfg(test)]
mod tests {
    use crate::datascrubbing::DataScrubbingConfig;
    /// These tests are ported from Sentry's Python testsuite (test_data_scrubber). Each testcase
    /// has an equivalent testcase in Python.
    use crate::pii::{PiiConfig, PiiProcessor};
    use crate::processor::{process_value, ProcessingState};
    use crate::protocol::Event;
    use crate::types::FromValue;

    use super::to_pii_config as to_pii_config_impl;

    fn to_pii_config(datascrubbing_config: &DataScrubbingConfig) -> Option<PiiConfig> {
        let rv = to_pii_config_impl(datascrubbing_config);
        if let Some(ref config) = rv {
            let roundtrip: PiiConfig =
                serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
            assert_eq_dbg!(&roundtrip, config);
        }
        rv
    }

    lazy_static::lazy_static! {
        static ref SENSITIVE_VARS: serde_json::Value = serde_json::json!({
            "foo": "bar",
            "password": "hello",
            "the_secret": "hello",
            "a_password_here": "hello",
            "api_key": "secret_key",
            "apiKey": "secret_key",
        });
    }

    static PUBLIC_KEY: &str = r#"""-----BEGIN PUBLIC KEY-----
MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA6A6TQjlPyMurLh/igZY4
izA9sJgeZ7s5+nGydO4AI9k33gcy2DObZuadWRMnDwc3uH/qoAPw/mo3KOcgEtxU
xdwiQeATa3HVPcQDCQiKm8xIG2Ny0oUbR0IFNvClvx7RWnPEMk05CuvsL0AA3eH5
xn02Yg0JTLgZEtUT3whwFm8CAwEAAQ==
-----END PUBLIC KEY-----"""#;

    static PRIVATE_KEY: &str = r#"""-----BEGIN PRIVATE KEY-----
MIIJRAIBADANBgkqhkiG9w0BAQEFAASCCS4wggkqAgEAAoICAQCoNFY4P+EeIXl0
mLpO+i8uFqAaEFQ8ZX2VVpA13kNEHuiWXC3HPlQ+7G+O3XmAsO+Wf/xY6pCSeQ8h
mLpO+i8uFqAaEFQ8ZX2VVpA13kNEHuiWXC3HPlQ+7G+O3XmAsO+Wf/xY6pCSeQ8h
-----END PRIVATE KEY-----"""#;

    static ENCRYPTED_PRIVATE_KEY: &str = r#"""-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIJjjBABgkqhkiG9w0BBQ0wMzAbBgkqhkiG9w0BBQwwDgQIWVhErdQOFVoCAggA
IrlYQUV1ig4U3viYh1Y8viVvRlANKICvgj4faYNH36UterkfDjzMonb/cXNeJEOS
YgorM2Pfuec5vtPRPKd88+Ds/ktIlZhjJwnJjHQMX+lSw5t0/juna2sLH2dpuAbi
PSk=
-----END ENCRYPTED PRIVATE KEY-----"""#;

    static RSA_PRIVATE_KEY: &str = r#"""-----BEGIN RSA PRIVATE KEY-----
+wn9Iu+zgamKDUu22xc45F2gdwM04rTITlZgjAs6U1zcvOzGxk8mWJD5MqFWwAtF
zN87YGV0VMTG6ehxnkI4Fg6i0JPU3QIDAQABAoICAQCoCPjlYrODRU+vd2YeU/gM
THd+9FBxiHLGXNKhG/FRSyREXEt+NyYIf/0cyByc9tNksat794ddUqnLOg0vwSkv
-----END RSA PRIVATE KEY-----"""#;

    fn simple_enabled_pii_config() -> PiiConfig {
        to_pii_config(&simple_enabled_config()).unwrap()
    }

    fn simple_enabled_config() -> DataScrubbingConfig {
        DataScrubbingConfig {
            scrub_data: true,
            scrub_ip_addresses: true,
            scrub_defaults: true,
            ..Default::default()
        }
    }

    #[test]
    fn test_datascrubbing_default() {
        insta::assert_json_snapshot!(to_pii_config(&Default::default()), @"null");
    }

    #[test]
    fn test_convert_default_pii_config() {
        insta::assert_json_snapshot!(simple_enabled_pii_config(), @r###"
        {
          "rules": {},
          "vars": {
            "hashKey": null
          },
          "applications": {
            "($string|$number|$array)": [
              "@common:filter"
            ],
            "($request.env.REMOTE_ADDR|$user.ip_address|$sdk.client_ip)": [
              "@anything:remove"
            ]
          }
        }
        "###);
    }

    #[test]
    fn test_convert_empty_sensitive_field() {
        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["".to_owned(), " ".to_owned()],
            ..simple_enabled_config()
        });

        insta::assert_json_snapshot!(pii_config, @r###"
        {
          "rules": {},
          "vars": {
            "hashKey": null
          },
          "applications": {
            "($string|$number|$array)": [
              "@common:filter"
            ],
            "($request.env.REMOTE_ADDR|$user.ip_address|$sdk.client_ip)": [
              "@anything:remove"
            ]
          }
        }
        "###);
    }

    #[test]
    fn test_convert_sensitive_fields() {
        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["fieldy_field".to_owned(), "moar_other_field".to_owned()],
            ..simple_enabled_config()
        });

        insta::assert_json_snapshot!(pii_config, @r###"
        {
          "rules": {
            "strip-fields": {
              "type": "redact_pair",
              "keyPattern": ".*(fieldy_field|moar_other_field).*",
              "redaction": {
                "method": "replace",
                "text": "[Filtered]"
              }
            }
          },
          "vars": {
            "hashKey": null
          },
          "applications": {
            "($string|$number|$array)": [
              "@common:filter",
              "strip-fields"
            ],
            "($request.env.REMOTE_ADDR|$user.ip_address|$sdk.client_ip)": [
              "@anything:remove"
            ]
          }
        }
        "###);
    }

    #[test]
    fn test_convert_exclude_field() {
        let pii_config = to_pii_config(&DataScrubbingConfig {
            exclude_fields: vec!["foobar".to_owned()],
            ..simple_enabled_config()
        });

        insta::assert_json_snapshot!(pii_config, @r###"
        {
          "rules": {},
          "vars": {
            "hashKey": null
          },
          "applications": {
            "(($string|$number|$array)&(~foobar))": [
              "@common:filter"
            ],
            "($request.env.REMOTE_ADDR|$user.ip_address|$sdk.client_ip)": [
              "@anything:remove"
            ]
          }
        }
        "###);
    }

    #[test]
    fn test_convert_scrub_ip_only() {
        let pii_config = to_pii_config(&DataScrubbingConfig {
            scrub_data: false,
            scrub_ip_addresses: true,
            scrub_defaults: false,
            ..Default::default()
        });

        insta::assert_json_snapshot!(pii_config, @r###"
        {
          "rules": {},
          "vars": {
            "hashKey": null
          },
          "applications": {
            "($request.env.REMOTE_ADDR|$user.ip_address|$sdk.client_ip)": [
              "@anything:remove"
            ]
          }
        }
        "###);
    }

    #[test]
    fn test_stacktrace() {
        let mut data = Event::from_value(
            serde_json::json!({
                "stacktrace": {
                    "frames": [
                    {
                        "vars": SENSITIVE_VARS.clone()
                    }
                    ]
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_http() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "data": SENSITIVE_VARS.clone(),
                    "env": SENSITIVE_VARS.clone(),
                    "headers": SENSITIVE_VARS.clone(),
                    "cookies": SENSITIVE_VARS.clone()
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_http_remote_addr_stripped() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "env": {
                        "REMOTE_ADDR": "127.0.0.1"
                    }
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sdk_client_ip_stripped() {
        let mut data = Event::from_value(
            serde_json::json!({
                "sdk": {
                    "client_ip": "127.0.0.1"
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_user() {
        let mut data = Event::from_value(
            serde_json::json!({
                "user": {
                    "username": "secret",
                    "ip_address": "73.133.27.120",
                    "data": SENSITIVE_VARS.clone()
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_user_ip_stripped() {
        let mut data = Event::from_value(
            serde_json::json!({
                "user": {
                    "username": "secret",
                    "ip_address": "73.133.27.120",
                    "data": SENSITIVE_VARS.clone()
                }
            })
            .into(),
        );

        let scrubbing_config = DataScrubbingConfig {
            scrub_data: false,
            scrub_ip_addresses: true,
            scrub_defaults: false,
            ..Default::default()
        };

        let pii_config = to_pii_config(&scrubbing_config).unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_extra() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": "bar",
                    "password": "hello",
                    "the_secret": "hello",
                    "a_password_here": "hello",
                    "api_key": "secret_key",
                    "apiKey": "secret_key",
                    "a_password_number": 42,
                    "a_password_array": [42, 43],
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_contexts() {
        let mut data = Event::from_value(
            serde_json::json!({
                "contexts": {
                    "secret": SENSITIVE_VARS.clone(),
                    "biz": SENSITIVE_VARS.clone()
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());

        // n.b.: This diverges from Python behavior because it would strip a context that is called
        // "secret", not just a string. We accept this difference.
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_querystring_as_string() {
        let mut data = Event::from_value(serde_json::json!({
            "request": {
                "query_string": "foo=bar&password=hello&the_secret=hello&a_password_here=hello&api_key=secret_key",
            }
        }).into());

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_querystring_as_pairlist() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "query_string": [
                        ["foo", "bar"],
                        ["password", "hello"],
                        ["the_secret", "hello"],
                        ["a_password_here", "hello"],
                        ["api_key", "secret_key"]
                    ]
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_querystring_as_string_with_partials() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "query_string": "foo=bar&password&baz=bar"
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_querystring_as_pairlist_with_partials() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "query_string": [["foo", "bar"], ["password", ""], ["baz", "bar"]]
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sanitize_additional_sensitive_fields() {
        let mut extra = SENSITIVE_VARS.clone();
        {
            let map = extra.as_object_mut().unwrap();
            map.insert("fieldy_field".to_owned(), serde_json::json!("value"));
            map.insert(
                "moar_other_field".to_owned(),
                serde_json::json!("another value"),
            );
        }

        let mut data = Event::from_value(serde_json::json!({ "extra": extra }).into());

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["fieldy_field".to_owned(), "moar_other_field".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sanitize_credit_card() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": "4571234567890111"
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sanitize_credit_card_amex() {
        // AMEX numbers are 15 digits, not 16
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": "378282246310005"
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sanitize_credit_card_discover() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": "6011111111111117"
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sanitize_credit_card_visa() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": "4111111111111111"
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sanitize_credit_card_mastercard() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": "5555555555554444"
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sanitize_credit_card_within_value_1() {
        sanitize_credit_card_within_value_test("'4571234567890111'");
    }

    #[test]
    fn test_sanitize_credit_card_within_value_2() {
        sanitize_credit_card_within_value_test("foo 4571234567890111");
    }

    fn sanitize_credit_card_within_value_test(cc: &str) {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": cc
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_does_not_sanitize_timestamp_looks_like_card() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": "1453843029218310"
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data, @r###"
        {
          "extra": {
            "foo": "1453843029218310"
          }
        }
        "###);
    }

    #[test]
    fn test_sanitize_url_1() {
        sanitize_url_test("pg://matt:pass@localhost/1");
    }

    #[test]
    fn test_sanitize_url_2() {
        sanitize_url_test("foo 'redis://redis:foo@localhost:6379/0' bar");
    }

    #[test]
    fn test_sanitize_url_3() {
        sanitize_url_test("'redis://redis:foo@localhost:6379/0'");
    }

    #[test]
    fn test_sanitize_url_4() {
        sanitize_url_test("foo redis://redis:foo@localhost:6379/0 bar");
    }

    #[test]
    fn test_sanitize_url_5() {
        sanitize_url_test("foo redis://redis:foo@localhost:6379/0 bar pg://matt:foo@localhost/1");
    }

    #[test]
    fn test_sanitize_url_6() {
        // Make sure we don't mess up any other url.
        // This url specifically if passed through urlunsplit(urlsplit()),
        // it'll change the value.
        sanitize_url_test("postgres:///path");
    }

    #[test]
    fn test_sanitize_url_7() {
        // Don't be too overly eager within JSON strings an catch the right field.
        sanitize_url_test(
            r#"{"a":"https://localhost","b":"foo@localhost","c":"pg://matt:pass@localhost/1","d":"lol"}"#,
        );
    }

    fn sanitize_url_test(url: &str) {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": url
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    /// Ensure that valid JSON as request body is parsed as such, and that the PII stripping is
    /// then more granular/sophisticated because we now understand the structure.
    #[test]
    fn test_sanitize_http_body() {
        use crate::store::StoreProcessor;

        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "data": r#"{"email":"zzzz@gmail.com","password":"zzzzz"}"#
                }
            })
            .into(),
        );

        // n.b.: In Rust we rely on store normalization to parse inline JSON

        let mut store_processor = StoreProcessor::new(Default::default(), None);
        process_value(&mut data, &mut store_processor, ProcessingState::root());

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data.value().unwrap().request);
    }

    /// Ensure that a request body that cannot be parsed by the store processor gets PII stripped
    /// nevertheless.
    #[test]
    fn test_sanitize_http_body_string() {
        use crate::store::StoreProcessor;

        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "data": r#"{"email":"zzzz@gmail.com","password":"zzzzz"}xxx"#
                }
            })
            .into(),
        );

        // n.b.: In Rust we rely on store normalization to parse inline JSON.

        let mut store_processor = StoreProcessor::new(Default::default(), None);
        process_value(&mut data, &mut store_processor, ProcessingState::root());

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data.value().unwrap().request);
    }

    #[test]
    fn test_does_not_fail_on_non_string() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "foo": 1
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());

        assert_annotated_snapshot!(data, @r###"
        {
          "extra": {
            "foo": 1
          }
        }
        "###);
    }

    #[test]
    fn test_does_sanitize_public_key() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "s": PUBLIC_KEY,
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_does_sanitize_private_key() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "s": PRIVATE_KEY,
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_does_sanitize_encrypted_private_key() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "s": ENCRYPTED_PRIVATE_KEY,
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_does_sanitize_rsa_private_key() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "s": RSA_PRIVATE_KEY,
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_does_sanitize_social_security_number() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "s": "123-45-6789"
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_exclude_fields_on_field_name() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {"password": "123-45-6789"}
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_explicit_fields() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {"mystuff": "xxx"}
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["mystuff".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_explicit_fields_case_insensitive() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {"MYSTUFF": "xxx"}
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["myStuff".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_exclude_fields_on_field_value() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {"foobar": "123-45-6789"}
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            exclude_fields: vec!["foobar".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());

        assert_annotated_snapshot!(data, @r###"
        {
          "extra": {
            "foobar": "123-45-6789"
          }
        }
        "###);
    }

    #[test]
    fn test_empty_field() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {"foobar": "xxx"}
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["".to_owned(), " ".to_owned()],
            exclude_fields: vec!["".to_owned(), " ".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());

        assert_annotated_snapshot!(data, @r###"
        {
          "extra": {
            "foobar": "xxx"
          }
        }
        "###);
    }

    #[test]
    fn test_should_have_mysql_pwd_as_a_default_1() {
        should_have_mysql_pwd_as_a_default_test("MYSQL_PWD");
    }

    #[test]
    fn test_should_have_mysql_pwd_as_a_default_2() {
        should_have_mysql_pwd_as_a_default_test("mysql_pwd");
    }

    fn should_have_mysql_pwd_as_a_default_test(key: &str) {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    *key: "the one",
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_authorization_scrubbing() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "authorization": "foobar",
                    "auth": "foobar",
                    "auXth": "foobar",
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_doesnt_scrub_not_scrubbed() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "test1": {
                        "is_authenticated": "foobar",
                    },

                    "test2": {
                        "is_authenticated": "null",
                    },

                    "test3": {
                        "is_authenticated": true,
                    },
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_csp_blocked_uri() {
        let mut data = Event::from_value(
            serde_json::json!({
                "csp": {"blocked_uri": "https://example.com/?foo=4571234567890111&bar=baz"}
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_breadcrumb_message() {
        let mut data = Event::from_value(
            serde_json::json!({
                "breadcrumbs": {
                    "values": [
                        {
                            "message": "SELECT session_key FROM django_session WHERE session_key = 'abcdefg'"
                        }
                    ]
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["session_key".to_owned()],
            ..simple_enabled_config()
        });

        insta::assert_json_snapshot!(pii_config, @r###"
        {
          "rules": {
            "strip-fields": {
              "type": "redact_pair",
              "keyPattern": ".*(session_key).*",
              "redaction": {
                "method": "replace",
                "text": "[Filtered]"
              }
            }
          },
          "vars": {
            "hashKey": null
          },
          "applications": {
            "($string|$number|$array)": [
              "@common:filter",
              "strip-fields"
            ],
            "($request.env.REMOTE_ADDR|$user.ip_address|$sdk.client_ip)": [
              "@anything:remove"
            ]
          }
        }
        "###);

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(&pii_config);
        process_value(&mut data, &mut pii_processor, ProcessingState::root());
        assert_annotated_snapshot!(data);
    }
}
