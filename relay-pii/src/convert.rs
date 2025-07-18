use std::collections::BTreeMap;

use once_cell::sync::Lazy;
use relay_event_schema::processor::ValueType;

use crate::selector::{SelectorPathItem, SelectorSpec};
use crate::{
    DataScrubbingConfig, LazyPattern, PiiConfig, PiiConfigError, RedactPairRule, Redaction,
    RuleSpec, RuleType, Vars,
};

/// Fields that the legacy data scrubber cannot strip.
///
/// We define this list independently of `metastructure(pii = true/false)` because the new PII
/// scrubber should be able to strip more.
static DATASCRUBBER_IGNORE: Lazy<SelectorSpec> = Lazy::new(|| {
    "(debug_meta.** | $frame.filename | $frame.abs_path | $logentry.formatted | $error.value | $request.headers.user-agent)"
        .parse()
        .unwrap()
});

/// Fields that are known to contain IPs. Used for legacy IP scrubbing.
static KNOWN_IP_FIELDS: Lazy<SelectorSpec> = Lazy::new(|| {
    "($request.env.REMOTE_ADDR | $user.ip_address | $sdk.client_ip | $span.sentry_tags.'user.ip')"
        .parse()
        .unwrap()
});

static SENSITIVE_COOKIES: Lazy<SelectorSpec> = Lazy::new(|| {
    [
        // Common session cookie names for popular web frameworks
        "*.cookies.sentrysid", // Sentry default session cookie name
        "*.cookies.sudo",      // Sentry default sudo cookie name
        "*.cookies.su",        // Sentry superuser cookie name
        "*.cookies.session",
        "*.cookies.__session",
        "*.cookies.sessionid",
        "*.cookies.user_session",
        "*.cookies.symfony",
        "*.cookies.phpsessid",
        "*.cookies.fasthttpsessionid",
        "*.cookies.mysession",
        "*.cookies.irissessionid",
        // Common CSRF/XSRF cookie names for popular web frameworks
        "*.cookies.csrf",
        "*.cookies.xsrf",
        "*.cookies._xsrf",
        "*.cookies._csrf",
        "*.cookies.csrf-token",
        "*.cookies.csrf_token",
        "*.cookies.xsrf-token",
        "*.cookies.xsrf_token",
        "*.cookies.fastcsrf",
        "*.cookies._iris_csrf",
    ]
    .join("|")
    .parse()
    .unwrap()
});

pub fn to_pii_config(
    datascrubbing_config: &DataScrubbingConfig,
) -> Result<Option<PiiConfig>, PiiConfigError> {
    let mut custom_rules = BTreeMap::new();
    let mut applied_rules = Vec::new();
    let mut applications = BTreeMap::new();

    if datascrubbing_config.scrub_data && datascrubbing_config.scrub_defaults {
        applied_rules.push("@common:filter".to_owned());
        applications.insert(
            SENSITIVE_COOKIES.clone(),
            vec!["@anything:filter".to_owned()],
        );
    }

    if datascrubbing_config.scrub_ip_addresses {
        // legacy(?) scrubs all fields that are known to have IPs regardless of actual content
        applications.insert(KNOWN_IP_FIELDS.clone(), vec!["@anything:remove".to_owned()]);

        // checks actual contents of all fields and scrubs where there is an IP address
        applied_rules.push("@ip:replace".to_owned());
    }

    if datascrubbing_config.scrub_data {
        let mut sensitive_fields = datascrubbing_config
            .sensitive_fields
            .iter()
            .map(|x| x.trim())
            .filter(|x| !x.is_empty())
            .peekable();

        let sensitive_fields_re = if sensitive_fields.peek().is_some() {
            let mut re = "".to_owned();

            for (idx, field) in sensitive_fields.enumerate() {
                if idx != 0 {
                    re.push('|');
                }

                // ugly: regex::escape returns owned string
                re.push_str(&regex::escape(field));
            }

            re.push_str("");
            Some(re)
        } else {
            None
        };

        if let Some(key_pattern) = sensitive_fields_re {
            custom_rules.insert(
                "strip-fields".to_owned(),
                RuleSpec {
                    ty: RuleType::RedactPair(RedactPairRule {
                        key_pattern: LazyPattern::new(key_pattern).case_insensitive(true),
                    }),
                    redaction: Redaction::Replace("[Filtered]".to_owned().into()),
                },
            );

            applied_rules.push("strip-fields".to_owned());
        }
    }

    if applied_rules.is_empty() && applications.is_empty() {
        return Ok(None);
    }

    let mut conjunctions = vec![
        SelectorSpec::Or(vec![
            ValueType::String.into(),
            ValueType::Number.into(),
            ValueType::Array.into(),
            ValueType::Object.into(),
        ]),
        SelectorSpec::Not(Box::new(DATASCRUBBER_IGNORE.clone())),
    ];

    for field in &datascrubbing_config.exclude_fields {
        let field = field.trim();

        if field.is_empty() {
            continue;
        }

        let spec = match SelectorSpec::parse_non_legacy(field) {
            Ok(spec) => spec,
            Err(_) => {
                // Ideally safe fields should be caught by sentry-side validation,
                // but there is still a considerable amount of "invalid" safe fields.
                // Fallback to stay compatible with the old behaviour.
                SelectorSpec::Path(vec![SelectorPathItem::Key(field.to_owned())])
            }
        };

        conjunctions.push(SelectorSpec::Not(Box::new(spec)));
    }

    let applied_selector = SelectorSpec::And(conjunctions);

    if !applied_rules.is_empty() {
        applications.insert(applied_selector, applied_rules);
    }

    Ok(Some(PiiConfig {
        rules: custom_rules,
        vars: Vars::default(),
        applications,
        ..Default::default()
    }))
}

#[cfg(test)]
mod tests {
    use relay_event_schema::processor::{ProcessingState, process_value};
    use relay_event_schema::protocol::Event;
    use relay_protocol::{FromValue, assert_annotated_snapshot};
    use similar_asserts::assert_eq;

    use crate::PiiProcessor;

    use super::to_pii_config as to_pii_config_impl;
    use super::*;

    // These tests are ported from Sentry's Python testsuite (test_data_scrubber). Each testcase
    // has an equivalent testcase in Python.

    fn to_pii_config(datascrubbing_config: &DataScrubbingConfig) -> Option<PiiConfig> {
        let rv = to_pii_config_impl(datascrubbing_config).unwrap();
        if let Some(ref config) = rv {
            let roundtrip: PiiConfig =
                serde_json::from_value(serde_json::to_value(config).unwrap()).unwrap();
            assert_eq!(&roundtrip, config);
        }
        rv
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

    fn sensitive_vars() -> serde_json::Value {
        serde_json::json!({
            "foo": "bar",
            "password": "hello",
            "the_secret": "hello",
            "a_password_here": "hello",
            "api_key": "secret_key",
            "apiKey": "secret_key",
        })
    }

    fn sensitive_cookies() -> serde_json::Value {
        serde_json::json!({
        // Common session cookie names for popular web frameworks
        "sentrysid": "my sentrysid", // Sentry default session cookie name
        "sudo": "my sudo",   // Sentry default sudo cookie name
        "su": "my su",    // Sentry superuser cookie name
        "session": "my session",
        "__session": "my __session",
        "sessionid": "my sessionid",
        "user_session": "my user_session",
        "symfony": "my symfony",
        "phpsessid": "my phpsessid",
        "fasthttpsessionid": "my fasthttpsessionid",
        "mysession": "my mysession",
        "irissessionid": "my irissessionid",
        // Common CSRF/XSRF cookie names for popular web frameworks
        "csrf": "my csrf",
        "xsrf": "my xsrf",
        "_xsrf": "my _xsrf",
        "_csrf": "my _csrf",
        "csrf-token": "my csrf-token",
        "csrf_token": "my csrf_token",
        "xsrf-token": "my  xsrf-token",
        "xsrf_token": "my  xsrf_token",
        "fastcsrf": "my fastcsrf",
        "_iris_csrf": "my _iris_csrf",
        "not_sensitive": "keep this",
        })
    }

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
        insta::assert_json_snapshot!(to_pii_config(&DataScrubbingConfig::default()), @"null");
    }

    #[test]
    fn test_convert_default_pii_config() {
        insta::assert_json_snapshot!(simple_enabled_pii_config(), @r###"
        {
          "applications": {
            "($string || $number || $array || $object) && !(debug_meta.** || $frame.filename || $frame.abs_path || $logentry.formatted || $error.value || $http.headers.user-agent)": [
              "@common:filter",
              "@ip:replace"
            ],
            "$http.env.REMOTE_ADDR || $user.ip_address || $sdk.client_ip || $span.sentry_tags.'user.ip'": [
              "@anything:remove"
            ],
            "*.cookies.sentrysid || *.cookies.sudo || *.cookies.su || *.cookies.session || *.cookies.__session || *.cookies.sessionid || *.cookies.user_session || *.cookies.symfony || *.cookies.phpsessid || *.cookies.fasthttpsessionid || *.cookies.mysession || *.cookies.irissessionid || *.cookies.csrf || *.cookies.xsrf || *.cookies._xsrf || *.cookies._csrf || *.cookies.csrf-token || *.cookies.csrf_token || *.cookies.xsrf-token || *.cookies.xsrf_token || *.cookies.fastcsrf || *.cookies._iris_csrf": [
              "@anything:filter"
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
          "applications": {
            "($string || $number || $array || $object) && !(debug_meta.** || $frame.filename || $frame.abs_path || $logentry.formatted || $error.value || $http.headers.user-agent)": [
              "@common:filter",
              "@ip:replace"
            ],
            "$http.env.REMOTE_ADDR || $user.ip_address || $sdk.client_ip || $span.sentry_tags.'user.ip'": [
              "@anything:remove"
            ],
            "*.cookies.sentrysid || *.cookies.sudo || *.cookies.su || *.cookies.session || *.cookies.__session || *.cookies.sessionid || *.cookies.user_session || *.cookies.symfony || *.cookies.phpsessid || *.cookies.fasthttpsessionid || *.cookies.mysession || *.cookies.irissessionid || *.cookies.csrf || *.cookies.xsrf || *.cookies._xsrf || *.cookies._csrf || *.cookies.csrf-token || *.cookies.csrf_token || *.cookies.xsrf-token || *.cookies.xsrf_token || *.cookies.fastcsrf || *.cookies._iris_csrf": [
              "@anything:filter"
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
              "keyPattern": "fieldy_field|moar_other_field",
              "redaction": {
                "method": "replace",
                "text": "[Filtered]"
              }
            }
          },
          "applications": {
            "($string || $number || $array || $object) && !(debug_meta.** || $frame.filename || $frame.abs_path || $logentry.formatted || $error.value || $http.headers.user-agent)": [
              "@common:filter",
              "@ip:replace",
              "strip-fields"
            ],
            "$http.env.REMOTE_ADDR || $user.ip_address || $sdk.client_ip || $span.sentry_tags.'user.ip'": [
              "@anything:remove"
            ],
            "*.cookies.sentrysid || *.cookies.sudo || *.cookies.su || *.cookies.session || *.cookies.__session || *.cookies.sessionid || *.cookies.user_session || *.cookies.symfony || *.cookies.phpsessid || *.cookies.fasthttpsessionid || *.cookies.mysession || *.cookies.irissessionid || *.cookies.csrf || *.cookies.xsrf || *.cookies._xsrf || *.cookies._csrf || *.cookies.csrf-token || *.cookies.csrf_token || *.cookies.xsrf-token || *.cookies.xsrf_token || *.cookies.fastcsrf || *.cookies._iris_csrf": [
              "@anything:filter"
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
          "applications": {
            "($string || $number || $array || $object) && !(debug_meta.** || $frame.filename || $frame.abs_path || $logentry.formatted || $error.value || $http.headers.user-agent) && !foobar": [
              "@common:filter",
              "@ip:replace"
            ],
            "$http.env.REMOTE_ADDR || $user.ip_address || $sdk.client_ip || $span.sentry_tags.'user.ip'": [
              "@anything:remove"
            ],
            "*.cookies.sentrysid || *.cookies.sudo || *.cookies.su || *.cookies.session || *.cookies.__session || *.cookies.sessionid || *.cookies.user_session || *.cookies.symfony || *.cookies.phpsessid || *.cookies.fasthttpsessionid || *.cookies.mysession || *.cookies.irissessionid || *.cookies.csrf || *.cookies.xsrf || *.cookies._xsrf || *.cookies._csrf || *.cookies.csrf-token || *.cookies.csrf_token || *.cookies.xsrf-token || *.cookies.xsrf_token || *.cookies.fastcsrf || *.cookies._iris_csrf": [
              "@anything:filter"
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
          "applications": {
            "($string || $number || $array || $object) && !(debug_meta.** || $frame.filename || $frame.abs_path || $logentry.formatted || $error.value || $http.headers.user-agent)": [
              "@ip:replace"
            ],
            "$http.env.REMOTE_ADDR || $user.ip_address || $sdk.client_ip || $span.sentry_tags.'user.ip'": [
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
                        "vars": sensitive_vars()
                    }
                    ]
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_http() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "data": sensitive_vars(),
                    "env": sensitive_vars(),
                    "headers": sensitive_vars(),
                    "cookies": sensitive_vars()
                },
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sensitive_cookies() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "cookies": sensitive_cookies()
                },
                "contexts": {
                    "response": {
                        "cookies": sensitive_cookies(),
                    }
                }
            })
            .into(),
        );
        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_user() {
        let mut data = Event::from_value(
            serde_json::json!({
                "user": {
                    "username": "secret",
                    "ip_address": "73.133.27.120",
                    "data": sensitive_vars()
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    /// Checks that any fields containing an IP-address is scrubbed.
    /// Even if it doesn't make sense for there to be an IP-address
    /// such as in the username field.
    #[test]
    fn test_ip_stripped() {
        let mut data = Event::from_value(
            serde_json::json!({
                "user": {
                    "username": "73.133.27.120", // should be stripped despite not being "known ip field"
                    "ip_address": "should be stripped despite lacking ip address",
                    "data": sensitive_vars()
                },
                "breadcrumbs": {
                    "values": [
                        {
                            "message": "73.133.27.120",
                            "data": {
                                "test_data": "73.133.27.120" // test deep wildcard stripping
                                }
                        },
                    ],
                },
                "sdk": {
                    "client_ip": "should also be stripped"
                },
                "spans": [
                    {
                        "sentry_tags": {
                            "user.ip": "73.133.27.120"
                        }
                    }
                ]
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_contexts() {
        let mut data = Event::from_value(
            serde_json::json!({
                "contexts": {
                    "secret": sensitive_vars(),
                    "biz": sensitive_vars(),

                    // all the structured contexts are not properly scrubbed. We introduced quite a
                    // few regressions here when porting old datascrubbers to rust, but that's fine
                    // because we saw in real data that nobody tried to scrub contexts in the first
                    // place. We're just snapshotting this so that we don't randomly change
                    // behavior further.
                    "device": sensitive_vars(),
                    "os": sensitive_vars(),
                    "runtime": sensitive_vars(),
                    "app": sensitive_vars(),
                    "browser": sensitive_vars(),
                    "gpu": sensitive_vars(),
                    "trace": sensitive_vars(),
                    "monitor": sensitive_vars(),
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();

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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_sanitize_additional_sensitive_fields() {
        let mut extra = sensitive_vars();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    macro_rules! sanitize_credit_card_within_value_test {
        ($cc:literal) => {{
            let mut data = Event::from_value(
                serde_json::json!({
                    "extra": {
                        "foo": $cc
                    }
                })
                .into(),
            );

            let pii_config = simple_enabled_pii_config();
                let mut pii_processor = PiiProcessor::new(pii_config.compiled());
            process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
            assert_annotated_snapshot!(data);
        }}
    }

    #[test]
    fn test_sanitize_credit_card_within_value_1() {
        sanitize_credit_card_within_value_test!("'4571234567890111'");
    }

    #[test]
    fn test_sanitize_credit_card_within_value_2() {
        sanitize_credit_card_within_value_test!("foo 4571234567890111");
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data, @r###"
        {
          "extra": {
            "foo": "1453843029218310"
          }
        }
        "###);
    }

    macro_rules! sanitize_url_test {
        ($url:literal) => {{
            let mut data = Event::from_value(
                serde_json::json!({
                    "extra": {
                        "foo": $url
                    }
                })
                .into(),
            );

            let pii_config = simple_enabled_pii_config();
                let mut pii_processor = PiiProcessor::new(pii_config.compiled());
            process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
            assert_annotated_snapshot!(data);
        }}
    }

    #[test]
    fn test_sanitize_url_1() {
        sanitize_url_test!("pg://matt:pass@localhost/1");
    }

    #[test]
    fn test_sanitize_url_2() {
        sanitize_url_test!("foo 'redis://redis:foo@localhost:6379/0' bar");
    }

    #[test]
    fn test_sanitize_url_3() {
        sanitize_url_test!("'redis://redis:foo@localhost:6379/0'");
    }

    #[test]
    fn test_sanitize_url_4() {
        sanitize_url_test!("foo redis://redis:foo@localhost:6379/0 bar");
    }

    #[test]
    fn test_sanitize_url_5() {
        sanitize_url_test!("foo redis://redis:foo@localhost:6379/0 bar pg://matt:foo@localhost/1");
    }

    #[test]
    fn test_sanitize_url_6() {
        // Make sure we don't mess up any other url.
        // This url specifically if passed through urlunsplit(urlsplit()),
        // it'll change the value.
        sanitize_url_test!("postgres:///path");
    }

    #[test]
    fn test_sanitize_url_7() {
        // Don't be too overly eager within JSON strings an catch the right field.
        sanitize_url_test!(
            r#"{"a":"https://localhost","b":"foo@localhost","c":"pg://matt:pass@localhost/1","d":"lol"}"#
        );
    }

    /// Ensure that valid JSON as request body is parsed as such, and that the PII stripping is
    /// then more granular/sophisticated because we now understand the structure.
    #[test]
    fn test_sanitize_http_body() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "data": {"email":"zzzz@gmail.com","password":"zzzzz"}
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();

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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();

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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();

        assert_annotated_snapshot!(data, @r###"
        {
          "extra": {
            "foobar": "xxx"
          }
        }
        "###);
    }

    macro_rules! should_have_mysql_pwd_as_a_default_test {
        ($key:literal) => {{
            let mut data = Event::from_value(
                serde_json::json!({
                    "extra": {
                        *$key: "the one",
                    }
                })
                .into(),
            );

            let pii_config = to_pii_config(&DataScrubbingConfig {
                sensitive_fields: vec!["".to_owned()],
                ..simple_enabled_config()
            });

            let pii_config = pii_config.unwrap();
                let mut pii_processor = PiiProcessor::new(pii_config.compiled());
            process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
            assert_annotated_snapshot!(data);
        }}
    }

    #[test]
    fn test_should_have_mysql_pwd_as_a_default_1() {
        should_have_mysql_pwd_as_a_default_test!("MYSQL_PWD");
    }

    #[test]
    fn test_should_have_mysql_pwd_as_a_default_2() {
        should_have_mysql_pwd_as_a_default_test!("mysql_pwd");
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
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
              "keyPattern": "session_key",
              "redaction": {
                "method": "replace",
                "text": "[Filtered]"
              }
            }
          },
          "applications": {
            "($string || $number || $array || $object) && !(debug_meta.** || $frame.filename || $frame.abs_path || $logentry.formatted || $error.value || $http.headers.user-agent)": [
              "@common:filter",
              "@ip:replace",
              "strip-fields"
            ],
            "$http.env.REMOTE_ADDR || $user.ip_address || $sdk.client_ip || $span.sentry_tags.'user.ip'": [
              "@anything:remove"
            ],
            "*.cookies.sentrysid || *.cookies.sudo || *.cookies.su || *.cookies.session || *.cookies.__session || *.cookies.sessionid || *.cookies.user_session || *.cookies.symfony || *.cookies.phpsessid || *.cookies.fasthttpsessionid || *.cookies.mysession || *.cookies.irissessionid || *.cookies.csrf || *.cookies.xsrf || *.cookies._xsrf || *.cookies._csrf || *.cookies.csrf-token || *.cookies.csrf_token || *.cookies.xsrf-token || *.cookies.xsrf_token || *.cookies.fastcsrf || *.cookies._iris_csrf": [
              "@anything:filter"
            ]
          }
        }
        "###);

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_event_message_not_strippable() {
        let mut data = Event::from_value(
            serde_json::json!({
                "logentry": {
                    "formatted": "hello world!"
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["formatted".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_debug_meta_files_not_strippable() {
        let mut data = Event::from_value(
            serde_json::json!({
                "debug_meta": {
                    "images": [
                        {
                            "type": "macho",
                            "code_file": "foo/bar.txt",
                            "debug_file": "foo/bar.txt",
                        }
                    ]
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["debug_file".to_owned(), "code_file".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_stacktrace_paths_not_strippable() {
        let mut data = Event::from_value(
            serde_json::json!({
                "stacktrace": {
                    "frames": [
                        {
                            "filename": "C:\\Windows\\BogusValue\\foo.txt",
                            "abs_path": "C:\\Windows\\BogusValue\\foo.txt"
                        }
                    ]
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["filename".to_owned(), "abs_path".to_owned()],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_odd_keys() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "do not ,./<>?!@#$%^&*())'ßtrip'": "foo",
                    "special ,./<>?!@#$%^&*())'gärbage'": "bar",
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec!["special ,./<>?!@#$%^&*())'gärbage'".to_owned()],
            exclude_fields: vec![
                "do not ,./<>?!@#$%^&*())'ßtrip'".to_owned(),
                "2abc".to_owned(),
            ],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_regression_more_odd_keys() {
        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec![],
            exclude_fields: vec![
                "url".to_owned(),
                "message".to_owned(),
                "http.request.url".to_owned(),
                "*url*".to_owned(),
                "*message*".to_owned(),
                "*http.request.url*".to_owned(),
            ],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        insta::assert_json_snapshot!(pii_config);
    }

    #[test]
    fn test_safe_fields_for_token() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "password": "foo",
                    "github_token": "bar",
                    "access_token": "quz",
                    "stripetoken": "baz",
                    "my-token": "secret",
                    "new_token": "hidden",
                    "secret-token-here": "ops"
                }
            })
            .into(),
        );
        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec![],
            exclude_fields: vec![
                "GITHUB_TOKEN".to_owned(),
                "access_token".to_owned(),
                "stripetoken".to_owned(),
            ],
            ..simple_enabled_config()
        });

        let pii_config = pii_config.unwrap();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_scrub_object() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "url": "https://www.example.com/fr/testing/authenticate",
                    "method": "POST",
                    "data": {
                      "password": "test",
                      "profile": {
                        "email": "testing@example.com",
                        "password": {
                          "$eq": "u{}{}{}]H[[[]]ww6KrA9F.x-F%%"
                        }
                      },
                      "submit": "Se connecter"
                    }
                }
            })
            .into(),
        );

        let config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_no_scrub_object_with_safe_fields() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "url": "https://www.example.com/fr/testing/authenticate",
                    "method": "POST",
                    "data": {
                      "password": "test",
                      "credentials": {
                        "email": "testing@example.com",
                        "password": "test",
                      },
                      "submit": "Se connecter"
                    }
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            sensitive_fields: vec![],
            exclude_fields: vec!["credentials".to_owned()],
            ..simple_enabled_config()
        })
        .unwrap();

        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_exclude_expression() {
        let mut data = Event::from_value(
            serde_json::json!({
                "extra": {
                    "do_not_scrub_1": "password",
                    "do_not_scrub_2": ["password"],
                    "do_not_scrub.dot": ["password"],
                },
                "user": {
                    "id": "5355849125500546",
                }
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            exclude_fields: vec![
                "do_not_scrub_1".to_owned(),
                "do_not_scrub_2.**".to_owned(),
                "extra.'do_not_scrub.dot'.**".to_owned(),
                "$user.id".to_owned(),
            ],
            ..simple_enabled_config()
        })
        .unwrap();

        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data, @r###"
        {
          "user": {
            "id": "5355849125500546"
          },
          "extra": {
            "do_not_scrub.dot": [
              "password"
            ],
            "do_not_scrub_1": "password",
            "do_not_scrub_2": [
              "password"
            ]
          }
        }
        "###);
    }

    #[test]
    fn test_safe_field_legacy_disables_all_string_parsing() {
        let mut data = Event::from_value(
            serde_json::json!({
                "request": {
                    "data": {
                        "email": "foo@bar.de",
                        "password": "kkee",
                        "recaptcha": null,
                    }
                },
            })
            .into(),
        );

        let pii_config = to_pii_config(&DataScrubbingConfig {
            // this triggered legacy behaviour and disabled scrubbing for all fields
            exclude_fields: vec!["email".to_owned()],
            scrub_data: true,
            scrub_ip_addresses: true,
            sensitive_fields: vec!["email".to_owned(), "password".to_owned()],
            scrub_defaults: true,
            ..Default::default()
        })
        .unwrap();

        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data, @r###"
        {
          "request": {
            "data": {
              "email": "foo@bar.de",
              "password": "[Filtered]",
              "recaptcha": null
            }
          },
          "_meta": {
            "request": {
              "data": {
                "password": {
                  "": {
                    "rem": [
                      [
                        "@password:filter",
                        "s",
                        0,
                        10
                      ]
                    ],
                    "len": 4
                  }
                }
              }
            }
          }
        }
        "###);
    }

    #[test]
    fn test_bearer_tokens_scrubbed() {
        let mut data = Event::from_value(
            serde_json::json!({
                "threads": {
                    "values": [
                        {
                            "stacktrace": {
                                "frames": [
                                    {
                                        "vars": {
                                            "request": {
                                                "headers": [
                                                    [
                                                        "AuthToken",
                                                        "b'Bearer A1BBC234QWERTY0987MNBV012765HJKL'"
                                                    ]
                                                ]
                                            }
                                        }
                                    }

                                ]
                            }
                        }
                    ]
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_pairlist_scrubbed_with_matching_keys() {
        let mut data = Event::from_value(
            serde_json::json!({
                "threads": {
                    "values": [
                        {
                            "stacktrace": {
                                "frames": [
                                    {
                                        "vars": {
                                            "request": {
                                                "headers": [
                                                    [
                                                        "secret",
                                                        // Should be filtered because of key `secret`.
                                                        "A1BBC234QWERTY0987MNBV012765HJKL"
                                                    ],
                                                    [
                                                        "passwd",
                                                        // Should be filtered because of key `passwd`.
                                                        "my_password"
                                                    ]
                                                ]
                                            }
                                        }
                                    }

                                ]
                            }
                        }
                    ]
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }

    #[test]
    fn test_pairlist_scrubbed_with_matching_values() {
        let mut data = Event::from_value(
            serde_json::json!({
                "threads": {
                    "values": [
                        {
                            "stacktrace": {
                                "frames": [
                                    {
                                        "vars": {
                                            "request": {
                                                "headers": [
                                                    [
                                                        "some_random_value",
                                                        // Should be filtered because it's treated
                                                        // as individual value.
                                                        "my_password"
                                                    ],
                                                    [
                                                        "some_random_value_2",
                                                        // Should not be filtered because the value
                                                        // is scraped by default.
                                                        "abc"
                                                    ]
                                                ]
                                            }
                                        }
                                    }

                                ]
                            }
                        }
                    ]
                }
            })
            .into(),
        );

        let pii_config = simple_enabled_pii_config();
        let mut pii_processor = PiiProcessor::new(pii_config.compiled());
        process_value(&mut data, &mut pii_processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(data);
    }
}
