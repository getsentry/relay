//! Event normalization and processing for attribute (EAP) based payloads.
//!
//! A central place for all modifications/normalizations for attributes.

use chrono::{DateTime, Utc};
use relay_common::time::UnixTimestamp;
use relay_conventions::{
    AttributeInfo, BROWSER_NAME, BROWSER_VERSION, OBSERVED_TIMESTAMP_NANOS,
    OBSERVED_TIMESTAMP_NANOS_INTERNAL, USER_AGENT_ORIGINAL, USER_GEO_CITY, USER_GEO_COUNTRY_CODE,
    USER_GEO_REGION, USER_GEO_SUBDIVISION, WriteBehavior,
};
use relay_event_schema::protocol::{AttributeType, Attributes, BrowserContext, Geo};
use relay_protocol::{Annotated, ErrorKind, Remark, RemarkType, Value};

use crate::{ClientHints, FromUserAgentInfo as _, RawUserAgentInfo};

/// Normalizes/validates all attribute types.
///
/// Removes and marks all attributes with an error for which the specified [`AttributeType`]
/// does not match the value.
pub fn normalize_attribute_types(attributes: &mut Annotated<Attributes>) {
    let Some(attributes) = attributes.value_mut() else {
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

/// Adds the `received` time to the attributes.
pub fn normalize_received(attributes: &mut Annotated<Attributes>, received: DateTime<Utc>) {
    let attributes = attributes.get_or_insert_with(Default::default);

    attributes.insert_if_missing(OBSERVED_TIMESTAMP_NANOS, || {
        received
            .timestamp_nanos_opt()
            .unwrap_or_else(|| UnixTimestamp::now().as_nanos() as i64)
            .to_string()
    });

    attributes.insert_if_missing(OBSERVED_TIMESTAMP_NANOS_INTERNAL, || {
        received
            .timestamp_nanos_opt()
            .unwrap_or_else(|| UnixTimestamp::now().as_nanos() as i64)
            .to_string()
    });
}

/// Normalizes the user agent/client information into [`Attributes`].
///
/// Does not modify the attributes if there is already browser information present,
/// to preserve original values.
pub fn normalize_user_agent(
    attributes: &mut Annotated<Attributes>,
    client_user_agent: Option<&str>,
    client_hints: ClientHints<&str>,
) {
    let attributes = attributes.get_or_insert_with(Default::default);

    if attributes.contains_key(BROWSER_NAME) || attributes.contains_key(BROWSER_VERSION) {
        return;
    }

    // Prefer the stored/explicitly sent user agent over the user agent from the client/transport.
    let user_agent = attributes
        .get_value(USER_AGENT_ORIGINAL)
        .and_then(|v| v.as_str())
        .or(client_user_agent);

    let Some(context) = BrowserContext::from_hints_or_ua(&RawUserAgentInfo {
        user_agent,
        client_hints,
    }) else {
        return;
    };

    attributes.insert_if_missing(BROWSER_NAME, || context.name);
    attributes.insert_if_missing(BROWSER_VERSION, || context.version);
}

/// Normalizes the user's geographical information into [`Attributes`].
///
/// Does not modify the attributes if there is already user geo information present,
/// to preserve original values.
pub fn normalize_user_geo(
    attributes: &mut Annotated<Attributes>,
    info: impl FnOnce() -> Option<Geo>,
) {
    let attributes = attributes.get_or_insert_with(Default::default);

    if [
        USER_GEO_COUNTRY_CODE,
        USER_GEO_CITY,
        USER_GEO_SUBDIVISION,
        USER_GEO_REGION,
    ]
    .into_iter()
    .any(|a| attributes.contains_key(a))
    {
        return;
    }

    let Some(geo) = info() else {
        return;
    };

    attributes.insert_if_missing(USER_GEO_COUNTRY_CODE, || geo.country_code);
    attributes.insert_if_missing(USER_GEO_CITY, || geo.city);
    attributes.insert_if_missing(USER_GEO_SUBDIVISION, || geo.subdivision);
    attributes.insert_if_missing(USER_GEO_REGION, || geo.region);
}

/// Normalizes deprecated attributes according to `sentry-conventions`.
///
/// Attributes with a status of `"normalize"` will be moved to their replacement name.
/// If there is already a value present under the replacement name, it will be left alone,
/// but the deprecated attribute is removed anyway.
///
/// Attributes with a status of `"backfill"` will be copied to their replacement name if the
/// replacement name is not present. In any case, the original name is left alone.
pub fn normalize_attribute_names(attributes: &mut Annotated<Attributes>) {
    normalize_attribute_names_inner(attributes, relay_conventions::attribute_info)
}

fn normalize_attribute_names_inner(
    attributes: &mut Annotated<Attributes>,
    attribute_info: fn(&str) -> Option<&'static AttributeInfo>,
) {
    let attributes = attributes.get_or_insert_with(Default::default);
    let attribute_names: Vec<_> = attributes.keys().cloned().collect();

    for name in attribute_names {
        let Some(attribute_info) = attribute_info(&name) else {
            continue;
        };

        match attribute_info.write_behavior {
            WriteBehavior::CurrentName => continue,
            WriteBehavior::NewName(new_name) => {
                let Some(old_attribute) = attributes.get_raw_mut(&name) else {
                    continue;
                };

                let new_attribute = old_attribute.clone();
                old_attribute.set_value(None);
                old_attribute
                    .meta_mut()
                    .add_remark(Remark::new(RemarkType::Removed, "attribute.deprecated"));

                if !attributes.contains_key(new_name) {
                    attributes.insert_raw(new_name.to_owned(), new_attribute);
                }
            }
            WriteBehavior::BothNames(new_name) => {
                if !attributes.contains_key(new_name)
                    && let Some(current_attribute) = attributes.get_raw(&name).cloned()
                {
                    attributes.insert_raw(new_name.to_owned(), current_attribute);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::SerializableAnnotated;

    use super::*;

    #[test]
    fn test_normalize_received_none() {
        let mut attributes = Default::default();

        normalize_received(
            &mut attributes,
            DateTime::from_timestamp_nanos(1_234_201_337),
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "sentry._internal.observed_timestamp_nanos": {
            "type": "string",
            "value": "1234201337"
          },
          "sentry.observed_timestamp_nanos": {
            "type": "string",
            "value": "1234201337"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_received_existing() {
        let mut attributes = Annotated::from_json(
            r#"{
          "sentry._internal.observed_timestamp_nanos": {
            "type": "string",
            "value": "111222333"
          },
          "sentry.observed_timestamp_nanos": {
            "type": "string",
            "value": "111222333"
          }
        }"#,
        )
        .unwrap();

        normalize_received(
            &mut attributes,
            DateTime::from_timestamp_nanos(1_234_201_337),
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r###"
        {
          "sentry._internal.observed_timestamp_nanos": {
            "type": "string",
            "value": "111222333"
          },
          "sentry.observed_timestamp_nanos": {
            "type": "string",
            "value": "111222333"
          }
        }
        "###);
    }

    #[test]
    fn test_process_attribute_types() {
        let json = r#"{
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
        }"#;

        let mut attributes = Annotated::<Attributes>::from_json(json).unwrap();
        normalize_attribute_types(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r###"
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
    fn test_normalize_user_agent_none() {
        let mut attributes = Default::default();
        normalize_user_agent(
            &mut attributes,
            Some(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ),
            ClientHints::default(),
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "sentry.browser.name": {
            "type": "string",
            "value": "Chrome"
          },
          "sentry.browser.version": {
            "type": "string",
            "value": "131.0.0"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_user_agent_existing() {
        let mut attributes = Annotated::from_json(
            r#"{
          "sentry.browser.name": {
            "type": "string",
            "value": "Very Special"
          },
          "sentry.browser.version": {
            "type": "string",
            "value": "13.3.7"
          }
        }"#,
        )
        .unwrap();

        normalize_user_agent(
            &mut attributes,
            Some(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ),
            ClientHints::default(),
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "sentry.browser.name": {
            "type": "string",
            "value": "Very Special"
          },
          "sentry.browser.version": {
            "type": "string",
            "value": "13.3.7"
          }
        }
        "#,
        );
    }

    #[test]
    fn test_normalize_user_geo_none() {
        let mut attributes = Default::default();

        normalize_user_geo(&mut attributes, || {
            Some(Geo {
                country_code: "XY".to_owned().into(),
                city: "Foo Hausen".to_owned().into(),
                subdivision: Annotated::empty(),
                region: "Illu".to_owned().into(),
                other: Default::default(),
            })
        });

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "user.geo.city": {
            "type": "string",
            "value": "Foo Hausen"
          },
          "user.geo.country_code": {
            "type": "string",
            "value": "XY"
          },
          "user.geo.region": {
            "type": "string",
            "value": "Illu"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_user_geo_existing() {
        let mut attributes = Annotated::from_json(
            r#"{
          "user.geo.city": {
            "type": "string",
            "value": "Foo Hausen"
          }
        }"#,
        )
        .unwrap();

        normalize_user_geo(&mut attributes, || unreachable!());

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "user.geo.city": {
            "type": "string",
            "value": "Foo Hausen"
          }
        }
        "#,
        );
    }

    #[test]
    fn test_normalize_attributes() {
        fn mock_attribute_info(name: &str) -> Option<&'static AttributeInfo> {
            use relay_conventions::Pii;

            match name {
                "replace.empty" => Some(&AttributeInfo {
                    write_behavior: WriteBehavior::NewName("replaced"),
                    pii: Pii::Maybe,
                    aliases: &["replaced"],
                }),
                "replace.existing" => Some(&AttributeInfo {
                    write_behavior: WriteBehavior::NewName("not.replaced"),
                    pii: Pii::Maybe,
                    aliases: &["not.replaced"],
                }),
                "backfill.empty" => Some(&AttributeInfo {
                    write_behavior: WriteBehavior::BothNames("backfilled"),
                    pii: Pii::Maybe,
                    aliases: &["backfilled"],
                }),
                "backfill.existing" => Some(&AttributeInfo {
                    write_behavior: WriteBehavior::BothNames("not.backfilled"),
                    pii: Pii::Maybe,
                    aliases: &["not.backfilled"],
                }),
                _ => None,
            }
        }

        let mut attributes = Annotated::new(Attributes::from([
            (
                "replace.empty".to_owned(),
                Annotated::new("Should be moved".to_owned().into()),
            ),
            (
                "replace.existing".to_owned(),
                Annotated::new("Should be removed".to_owned().into()),
            ),
            (
                "not.replaced".to_owned(),
                Annotated::new("Should be left alone".to_owned().into()),
            ),
            (
                "backfill.empty".to_owned(),
                Annotated::new("Should be copied".to_owned().into()),
            ),
            (
                "backfill.existing".to_owned(),
                Annotated::new("Should be left alone".to_owned().into()),
            ),
            (
                "not.backfilled".to_owned(),
                Annotated::new("Should be left alone".to_owned().into()),
            ),
        ]));

        normalize_attribute_names_inner(&mut attributes, mock_attribute_info);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r###"
        {
          "backfill.empty": {
            "type": "string",
            "value": "Should be copied"
          },
          "backfill.existing": {
            "type": "string",
            "value": "Should be left alone"
          },
          "backfilled": {
            "type": "string",
            "value": "Should be copied"
          },
          "not.backfilled": {
            "type": "string",
            "value": "Should be left alone"
          },
          "not.replaced": {
            "type": "string",
            "value": "Should be left alone"
          },
          "replace.empty": null,
          "replace.existing": null,
          "replaced": {
            "type": "string",
            "value": "Should be moved"
          },
          "_meta": {
            "replace.empty": {
              "": {
                "rem": [
                  [
                    "attribute.deprecated",
                    "x"
                  ]
                ]
              }
            },
            "replace.existing": {
              "": {
                "rem": [
                  [
                    "attribute.deprecated",
                    "x"
                  ]
                ]
              }
            }
          }
        }
        "###);
    }
}
