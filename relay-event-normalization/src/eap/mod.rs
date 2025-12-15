//! Event normalization and processing for attribute (EAP) based payloads.
//!
//! A central place for all modifications/normalizations for attributes.

use std::borrow::Cow;
use std::net::IpAddr;

use chrono::{DateTime, Utc};
use relay_common::time::UnixTimestamp;
use relay_conventions::consts::*;
use relay_conventions::{AttributeInfo, WriteBehavior};
use relay_event_schema::protocol::{AttributeType, Attributes, BrowserContext, Geo};
use relay_protocol::{Annotated, ErrorKind, Meta, Remark, RemarkType, Value};
use relay_sampling::DynamicSamplingContext;
use relay_spans::derive_op_for_v2_span;

use crate::span::TABLE_NAME_REGEX;
use crate::span::description::{scrub_db_query, scrub_http};
use crate::span::tag_extraction::{
    domain_from_scrubbed_http, domain_from_server_address, sql_action_from_query,
    sql_tables_from_query,
};
use crate::{ClientHints, FromUserAgentInfo as _, RawUserAgentInfo};

mod ai;
mod size;

pub use self::ai::normalize_ai;
pub use self::size::*;

/// Infers the sentry.op attribute and inserts it into [`Attributes`] if not already set.
pub fn normalize_sentry_op(attributes: &mut Annotated<Attributes>) {
    if attributes
        .value()
        .is_some_and(|attrs| attrs.contains_key(OP))
    {
        return;
    }
    let inferred_op = derive_op_for_v2_span(attributes);
    let attrs = attributes.get_or_insert_with(Default::default);
    attrs.insert_if_missing(OP, || inferred_op);
}

/// Normalizes/validates all attribute types.
///
/// Removes and marks all attributes with an error for which the specified [`AttributeType`]
/// does not match the value.
pub fn normalize_attribute_types(attributes: &mut Annotated<Attributes>) {
    let Some(attributes) = attributes.value_mut() else {
        return;
    };

    let attributes = attributes.0.values_mut();
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
    attributes
        .get_or_insert_with(Default::default)
        .insert_if_missing(OBSERVED_TIMESTAMP_NANOS, || {
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

/// Normalizes the client address into [`Attributes`].
///
/// Infers the client ip from the client information which was provided to Relay, if the SDK
/// indicates the client ip should be inferred by setting it to `{{auto}}`.
///
/// This requires cooperation from SDKs as inferring a client ip only works in non-server
/// environments, where the user/client device is also the device sending the item.
pub fn normalize_client_address(attributes: &mut Annotated<Attributes>, client_ip: Option<IpAddr>) {
    let Some(attributes) = attributes.value_mut() else {
        return;
    };
    let Some(client_ip) = client_ip else { return };

    let client_address = attributes
        .get_value(CLIENT_ADDRESS)
        .and_then(|v| v.as_str());

    if client_address == Some("{{auto}}") {
        attributes.insert(CLIENT_ADDRESS, client_ip.to_string());
    }
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

/// Normalizes the [DSC](DynamicSamplingContext) into [`Attributes`].
pub fn normalize_dsc(attributes: &mut Annotated<Attributes>, dsc: Option<&DynamicSamplingContext>) {
    let Some(dsc) = dsc else { return };

    let attributes = attributes.get_or_insert_with(Default::default);

    // Check if DSC attributes are already set, the trace id is always required and must always be set.
    if attributes.contains_key(DSC_TRACE_ID) {
        return;
    }

    attributes.insert(DSC_TRACE_ID, dsc.trace_id.to_string());
    attributes.insert(DSC_PUBLIC_KEY, dsc.public_key.to_string());
    if let Some(release) = &dsc.release {
        attributes.insert(DSC_RELEASE, release.clone());
    }
    if let Some(environment) = &dsc.environment {
        attributes.insert(DSC_ENVIRONMENT, environment.clone());
    }
    if let Some(transaction) = &dsc.transaction {
        attributes.insert(DSC_TRANSACTION, transaction.clone());
    }
    if let Some(sample_rate) = dsc.sample_rate {
        attributes.insert(DSC_SAMPLE_RATE, sample_rate);
    }
    if let Some(sampled) = dsc.sampled {
        attributes.insert(DSC_SAMPLED, sampled);
    }
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
    let Some(attributes) = attributes.value_mut() else {
        return;
    };

    let attribute_names: Vec<_> = attributes.0.keys().cloned().collect();

    for name in attribute_names {
        let Some(attribute_info) = attribute_info(&name) else {
            continue;
        };

        match attribute_info.write_behavior {
            WriteBehavior::CurrentName => continue,
            WriteBehavior::NewName(new_name) => {
                let Some(old_attribute) = attributes.0.get_mut(&name) else {
                    continue;
                };

                let mut meta = Meta::default();
                // TODO: Possibly add a new RemarkType for "renamed/moved"
                meta.add_remark(Remark::new(RemarkType::Removed, "attribute.deprecated"));
                let new_attribute = std::mem::replace(old_attribute, Annotated(None, meta));

                if !attributes.contains_key(new_name) {
                    attributes.0.insert(new_name.to_owned(), new_attribute);
                }
            }
            WriteBehavior::BothNames(new_name) => {
                if !attributes.contains_key(new_name)
                    && let Some(current_attribute) = attributes.0.get(&name).cloned()
                {
                    attributes.0.insert(new_name.to_owned(), current_attribute);
                }
            }
        }
    }
}

/// Normalizes the values of a set of attributes if present in the span.
///
/// Each span type has a set of important attributes containing the main relevant information displayed
/// in the product-end. For instance, for DB spans, these attributes are `db.query.text`, `db.operation.name`,
/// `db.collection.name`. Previously, V1 spans always held these important values in the `description` field,
/// however, V2 spans now store these values in their respective attributes based on sentry conventions.
/// This function ports over the SpanV1 normalization logic that was previously in `scrub_span_description`
/// by creating a set of functions to handle each group of attributes separately.
pub fn normalize_attribute_values(
    attributes: &mut Annotated<Attributes>,
    http_span_allowed_hosts: &[String],
) {
    normalize_db_attributes(attributes);
    normalize_http_attributes(attributes, http_span_allowed_hosts);
}

/// Normalizes the following db attributes: `db.query.text`, `db.operation.name`, `db.collection.name`
/// based on related attributes within DB spans.
///
/// This function reads the raw db query from `db.query.text`, scrubs it if possible, and writes
/// the normalized query to the `sentry.normalized_db_query` attribute. After normalizing the query,
/// the db operation and collection name are updated if needed.
///
/// Note: This function assumes that the sentry.op has already been inferred and set in the attributes.
fn normalize_db_attributes(annotated_attributes: &mut Annotated<Attributes>) {
    let Some(attributes) = annotated_attributes.value() else {
        return;
    };

    // Skip normalization if the normalized db query attribute is already set.
    if attributes.get_value(NORMALIZED_DB_QUERY).is_some() {
        return;
    }

    let (op, sub_op) = attributes
        .get_value(OP)
        .and_then(|v| v.as_str())
        .map(|op| op.split_once('.').unwrap_or((op, "")))
        .unwrap_or_default();

    let raw_query = attributes
        .get_value(DB_QUERY_TEXT)
        .or_else(|| {
            if op == "db" {
                attributes.get_value(DESCRIPTION)
            } else {
                None
            }
        })
        .and_then(|v| v.as_str());

    let db_system = attributes
        .get_value(DB_SYSTEM_NAME)
        .and_then(|v| v.as_str());

    let db_operation = attributes
        .get_value(DB_OPERATION_NAME)
        .and_then(|v| v.as_str());

    let collection_name = attributes
        .get_value(DB_COLLECTION_NAME)
        .and_then(|v| v.as_str());

    let span_origin = attributes.get_value(ORIGIN).and_then(|v| v.as_str());

    let (normalized_db_query, parsed_sql) = if let Some(raw_query) = raw_query {
        scrub_db_query(
            raw_query,
            sub_op,
            db_system,
            db_operation,
            collection_name,
            span_origin,
        )
    } else {
        (None, None)
    };

    let db_operation = if db_operation.is_none() {
        if sub_op == "redis" || db_system == Some("redis") {
            // This only works as long as redis span descriptions contain the command + " *"
            if let Some(query) = normalized_db_query.as_ref() {
                let command = query.replace(" *", "");
                if command.is_empty() {
                    None
                } else {
                    Some(command)
                }
            } else {
                None
            }
        } else if let Some(raw_query) = raw_query {
            // For other database operations, try to get the operation from data
            sql_action_from_query(raw_query).map(|a| a.to_uppercase())
        } else {
            None
        }
    } else {
        db_operation.map(|db_operation| db_operation.to_uppercase())
    };

    let db_collection_name: Option<String> = if let Some(name) = collection_name {
        if db_system == Some("mongodb") {
            match TABLE_NAME_REGEX.replace_all(name, "{%s}") {
                Cow::Owned(s) => Some(s),
                Cow::Borrowed(_) => Some(name.to_owned()),
            }
        } else {
            Some(name.to_owned())
        }
    } else if span_origin == Some("auto.db.supabase") {
        normalized_db_query
            .as_ref()
            .and_then(|query| query.strip_prefix("from("))
            .and_then(|s| s.strip_suffix(")"))
            .map(String::from)
    } else if let Some(raw_query) = raw_query {
        sql_tables_from_query(raw_query, &parsed_sql)
    } else {
        None
    };

    if let Some(attributes) = annotated_attributes.value_mut() {
        if let Some(normalized_db_query) = normalized_db_query {
            let mut normalized_db_query_hash = format!("{:x}", md5::compute(&normalized_db_query));
            normalized_db_query_hash.truncate(16);

            attributes.insert(NORMALIZED_DB_QUERY, normalized_db_query);
            attributes.insert(NORMALIZED_DB_QUERY_HASH, normalized_db_query_hash);
        }
        if let Some(db_operation_name) = db_operation {
            attributes.insert(DB_OPERATION_NAME, db_operation_name)
        }
        if let Some(db_collection_name) = db_collection_name {
            attributes.insert(DB_COLLECTION_NAME, db_collection_name);
        }
    }
}

/// Normalizes the following http attributes: `http.request.method` and `server.address`.
///
/// The normalization process first scrubs the url and extracts the server address from the url.
/// It also sets 'url.full' to the raw url if it is not already set and can be retrieved from the server address.
fn normalize_http_attributes(
    annotated_attributes: &mut Annotated<Attributes>,
    allowed_hosts: &[String],
) {
    let Some(attributes) = annotated_attributes.value() else {
        return;
    };

    // Skip normalization if not an http span.
    // This is equivalent to conditionally scrubbing by span category in the V1 pipeline.
    if !attributes.contains_key(HTTP_REQUEST_METHOD) {
        return;
    }

    let op = attributes.get_value(OP).and_then(|v| v.as_str());

    let method = attributes
        .get_value(HTTP_REQUEST_METHOD)
        .and_then(|v| v.as_str());

    let server_address = attributes
        .get_value(SERVER_ADDRESS)
        .and_then(|v| v.as_str());

    let url = attributes.get_value(URL_FULL).and_then(|v| v.as_str());

    let url_scheme = attributes.get_value(URL_SCHEME).and_then(|v| v.as_str());

    // If the span op is "http.client" and the method and url are present,
    // extract a normalized domain to be stored in the "server.address" attribute.
    let (normalized_server_address, raw_url) = if op == Some("http.client") {
        let domain_from_scrubbed_http = method
            .zip(url)
            .and_then(|(method, url)| scrub_http(method, url, allowed_hosts))
            .and_then(|scrubbed_http| domain_from_scrubbed_http(&scrubbed_http));

        if let Some(domain) = domain_from_scrubbed_http {
            (Some(domain), None)
        } else {
            domain_from_server_address(server_address, url_scheme)
        }
    } else {
        (None, None)
    };

    let method = method.map(|m| m.to_uppercase());

    if let Some(attributes) = annotated_attributes.value_mut() {
        if let Some(method) = method {
            attributes.insert(HTTP_REQUEST_METHOD, method);
        }

        if let Some(normalized_server_address) = normalized_server_address {
            attributes.insert(SERVER_ADDRESS, normalized_server_address);
        }

        if let Some(raw_url) = raw_url {
            attributes.insert_if_missing(URL_FULL, || raw_url);
        }
    }
}

/// Double writes sentry conventions attributes into legacy attributes.
///
/// This achieves backwards compatibility as it allows products to continue using legacy attributes
/// while we accumulate spans that conform to sentry conventions.
///
/// This function is called after attribute value normalization (`normalize_attribute_values`) as it
/// clones normalized attributes into legacy attributes.
pub fn write_legacy_attributes(attributes: &mut Annotated<Attributes>) {
    let Some(attributes) = attributes.value_mut() else {
        return;
    };

    // Map of new sentry conventions attributes to legacy SpanV1 attributes
    let current_to_legacy_attributes = [
        // DB attributes
        (NORMALIZED_DB_QUERY, "sentry.normalized_description"),
        (NORMALIZED_DB_QUERY_HASH, "sentry.group"),
        (DB_OPERATION_NAME, "sentry.action"),
        (DB_COLLECTION_NAME, "sentry.domain"),
        // HTTP attributes
        (SERVER_ADDRESS, "sentry.domain"),
        (HTTP_REQUEST_METHOD, "sentry.action"),
    ];

    for (current_attribute, legacy_attribute) in current_to_legacy_attributes {
        if attributes.contains_key(current_attribute) {
            let Some(attr) = attributes.get_attribute(current_attribute) else {
                continue;
            };
            attributes.insert(legacy_attribute, attr.value.clone());
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

    #[test]
    fn test_normalize_span_infers_op() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
          "db.system.name": {
                "type": "string",
                "value": "mysql"
            },
            "db.operation.name": {
                "type": "string",
                "value": "query"
            }
        }
        "#,
        )
        .unwrap();

        normalize_sentry_op(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "db.operation.name": {
            "type": "string",
            "value": "query"
          },
          "db.system.name": {
            "type": "string",
            "value": "mysql"
          },
          "sentry.op": {
            "type": "string",
            "value": "db"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_attribute_values_mysql_db_query_attributes() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "sentry.op": {
            "type": "string",
            "value": "db.query"
          },
          "sentry.origin": {
            "type": "string",
            "value": "auto.otlp.spans"
          },
          "db.system.name": {
            "type": "string",
            "value": "mysql"
          },
          "db.query.text": {
            "type": "string",
            "value": "SELECT \"not an identifier\""
          }
        }
        "#,
        )
        .unwrap();

        normalize_db_attributes(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "db.operation.name": {
            "type": "string",
            "value": "SELECT"
          },
          "db.query.text": {
            "type": "string",
            "value": "SELECT \"not an identifier\""
          },
          "db.system.name": {
            "type": "string",
            "value": "mysql"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "SELECT %s"
          },
          "sentry.normalized_db_query.hash": {
            "type": "string",
            "value": "3a377dcc490b1690"
          },
          "sentry.op": {
            "type": "string",
            "value": "db.query"
          },
          "sentry.origin": {
            "type": "string",
            "value": "auto.otlp.spans"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_mongodb_db_query_attributes() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "sentry.op": {
            "type": "string",
            "value": "db"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.operation.name": {
            "type": "string",
            "value": "find"
          },
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          }
        }
        "#,
        )
        .unwrap();

        normalize_db_attributes(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          },
          "db.operation.name": {
            "type": "string",
            "value": "FIND"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "{\"find\":\"documents\",\"foo\":\"?\"}"
          },
          "sentry.normalized_db_query.hash": {
            "type": "string",
            "value": "aedc5c7e8cec726b"
          },
          "sentry.op": {
            "type": "string",
            "value": "db"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_db_attributes_does_not_update_attributes_if_already_normalized() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          },
          "db.operation.name": {
            "type": "string",
            "value": "FIND"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "{\"find\":\"documents\",\"foo\":\"?\"}"
          },
          "sentry.op": {
            "type": "string",
            "value": "db"
          }
        }
        "#,
        )
        .unwrap();

        normalize_db_attributes(&mut attributes);

        insta::assert_json_snapshot!(
            SerializableAnnotated(&attributes), @r#"
        {
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          },
          "db.operation.name": {
            "type": "string",
            "value": "FIND"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "{\"find\":\"documents\",\"foo\":\"?\"}"
          },
          "sentry.op": {
            "type": "string",
            "value": "db"
          }
        }
        "#
        );
    }

    #[test]
    fn test_normalize_db_attributes_does_not_change_non_db_spans() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "sentry.origin": {
            "type": "string",
            "value": "auto.otlp.spans"
          },
          "http.request.method": {
            "type": "string",
            "value": "GET"
          }
        }
      "#,
        )
        .unwrap();

        normalize_db_attributes(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "http.request.method": {
            "type": "string",
            "value": "GET"
          },
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "sentry.origin": {
            "type": "string",
            "value": "auto.otlp.spans"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_http_attributes() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "http.request.method": {
            "type": "string",
            "value": "GET"
          },
          "url.full": {
            "type": "string",
            "value": "https://application.www.xn--85x722f.xn--55qx5d.cn"
          }
        }
      "#,
        )
        .unwrap();

        normalize_http_attributes(&mut attributes, &[]);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "http.request.method": {
            "type": "string",
            "value": "GET"
          },
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "server.address": {
            "type": "string",
            "value": "*.xn--85x722f.xn--55qx5d.cn"
          },
          "url.full": {
            "type": "string",
            "value": "https://application.www.xn--85x722f.xn--55qx5d.cn"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_http_attributes_server_address() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "url.scheme": {
            "type": "string",
            "value": "https"
          },
          "server.address": {
            "type": "string",
            "value": "subdomain.example.com:5688"
          },
          "http.request.method": {
            "type": "string",
            "value": "GET"
          }
        }
      "#,
        )
        .unwrap();

        normalize_http_attributes(&mut attributes, &[]);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "http.request.method": {
            "type": "string",
            "value": "GET"
          },
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "server.address": {
            "type": "string",
            "value": "*.example.com:5688"
          },
          "url.full": {
            "type": "string",
            "value": "https://subdomain.example.com:5688"
          },
          "url.scheme": {
            "type": "string",
            "value": "https"
          }
        }
        "#);
    }

    #[test]
    fn test_normalize_http_attributes_allowed_hosts() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "http.request.method": {
            "type": "string",
            "value": "GET"
          },
          "url.full": {
            "type": "string",
            "value": "https://application.www.xn--85x722f.xn--55qx5d.cn"
          }
        }
      "#,
        )
        .unwrap();

        normalize_http_attributes(
            &mut attributes,
            &["application.www.xn--85x722f.xn--55qx5d.cn".to_owned()],
        );

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "http.request.method": {
            "type": "string",
            "value": "GET"
          },
          "sentry.op": {
            "type": "string",
            "value": "http.client"
          },
          "server.address": {
            "type": "string",
            "value": "application.www.xn--85x722f.xn--55qx5d.cn"
          },
          "url.full": {
            "type": "string",
            "value": "https://application.www.xn--85x722f.xn--55qx5d.cn"
          }
        }
        "#);
    }

    #[test]
    fn test_write_legacy_attributes() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"
        {
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          },
          "db.operation.name": {
            "type": "string",
            "value": "FIND"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "{\"find\":\"documents\",\"foo\":\"?\"}"
          },
          "sentry.normalized_db_query.hash": {
            "type": "string",
            "value": "aedc5c7e8cec726b"
          },
          "sentry.op": {
            "type": "string",
            "value": "db"
          }
        }
        "#,
        )
        .unwrap();

        write_legacy_attributes(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r#"
        {
          "db.collection.name": {
            "type": "string",
            "value": "documents"
          },
          "db.operation.name": {
            "type": "string",
            "value": "FIND"
          },
          "db.query.text": {
            "type": "string",
            "value": "{\"find\": \"documents\", \"foo\": \"bar\"}"
          },
          "db.system.name": {
            "type": "string",
            "value": "mongodb"
          },
          "sentry.action": {
            "type": "string",
            "value": "FIND"
          },
          "sentry.domain": {
            "type": "string",
            "value": "documents"
          },
          "sentry.group": {
            "type": "string",
            "value": "aedc5c7e8cec726b"
          },
          "sentry.normalized_db_query": {
            "type": "string",
            "value": "{\"find\":\"documents\",\"foo\":\"?\"}"
          },
          "sentry.normalized_db_query.hash": {
            "type": "string",
            "value": "aedc5c7e8cec726b"
          },
          "sentry.normalized_description": {
            "type": "string",
            "value": "{\"find\":\"documents\",\"foo\":\"?\"}"
          },
          "sentry.op": {
            "type": "string",
            "value": "db"
          }
        }
        "#);
    }
}
