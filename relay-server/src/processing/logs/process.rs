use chrono::{DateTime, Utc};
use relay_event_normalization::{
    ClientHints, FromUserAgentInfo as _, RawUserAgentInfo, SchemaProcessor,
};
use relay_event_schema::processor::{ProcessingState, process_value};
use relay_event_schema::protocol::{Attribute, AttributeType, BrowserContext, OurLog};
use relay_ourlogs::OtelLog;
use relay_pii::PiiProcessor;
use relay_protocol::{Annotated, ErrorKind, Value};
use smallvec::SmallVec;

use crate::envelope::{ContainerItems, Item, ItemContainer};
use crate::extractors::RequestMeta;
use crate::processing::logs::{Error, ExpandedLogs, Result, SerializedLogs};
use crate::processing::{Context, Managed};
use crate::services::outcome::DiscardReason;

pub fn expand(logs: Managed<SerializedLogs>) -> Managed<ExpandedLogs> {
    let received_at = logs.received_at();
    logs.map(|logs, records| {
        let mut all_logs = SmallVec::with_capacity(logs.count());

        for logs in logs.logs {
            let expanded = expand_log_container(&logs, received_at);
            let expanded = records.or_default(expanded, logs);
            all_logs.extend(expanded);
        }

        all_logs.reserve_exact(logs.otel_logs.len());
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
            logs: all_logs,
        }
    })
}

fn expand_otel_log(item: &Item, received_at: DateTime<Utc>) -> Result<Annotated<OurLog>> {
    let log = serde_json::from_slice::<OtelLog>(&item.payload())
        .inspect_err(|err| {
            relay_log::debug!("failed to parse OTel Log: {err}");
        })
        .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?;

    let log = relay_ourlogs::otel_to_sentry_log(log, received_at)
        .inspect_err(|err| {
            relay_log::debug!("failed to convert OTel Log to Sentry Log: {:?}", err);
        })
        .map_err(|_| Error::Invalid(DiscardReason::InvalidLog))?;

    Ok(Annotated::new(log))
}

fn expand_log_container(item: &Item, received_at: DateTime<Utc>) -> Result<ContainerItems<OurLog>> {
    let mut logs = ItemContainer::parse(item)
        .inspect_err(|err| {
            relay_log::debug!("failed to parse logs container: {err}");
        })
        .map_err(|_| Error::Invalid(DiscardReason::InvalidJson))?
        .into_items();

    for log in &mut logs {
        relay_ourlogs::ourlog_merge_otel(log, received_at);
    }

    Ok(logs)
}

pub fn process(logs: &mut Managed<ExpandedLogs>, ctx: Context<'_>) {
    logs.modify(|logs, records| {
        let meta = logs.headers.meta();
        logs.logs
            .retain_mut(|log| records.or_default(process_log(log, meta, ctx).map(|_| true), &*log));
    });
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

fn process_attribute_types(log: &mut OurLog) {
    let Some(attributes) = log.attributes.value_mut() else {
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

fn populate_ua_fields(log: &mut OurLog, user_agent: Option<&str>, client_hints: ClientHints<&str>) {
    let attributes = log.attributes.get_or_insert_with(Default::default);
    let Some(context) = BrowserContext::from_hints_or_ua(&RawUserAgentInfo {
        user_agent,
        client_hints,
    }) else {
        return;
    };

    if !attributes.contains_key("sentry.browser.name") {
        if let Some(name) = context.name.value() {
            attributes.insert(
                "sentry.browser.name".to_owned(),
                Annotated::new(Attribute::new(
                    AttributeType::String,
                    Value::String(name.to_owned()),
                )),
            );
        }
    }

    if !attributes.contains_key("sentry.browser.version") {
        if let Some(version) = context.version.value() {
            attributes.insert(
                "sentry.browser.version".to_owned(),
                Annotated::new(Attribute::new(
                    AttributeType::String,
                    Value::String(version.to_owned()),
                )),
            );
        }
    }
}

#[cfg(test)]
mod tests {
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

        insta::assert_debug_snapshot!(data.value().unwrap().attributes, @r###"
        {
            "double_with_i64": Attribute {
                value: I64(
                    -42,
                ),
                type: Double,
                other: {},
            },
            "invalid_int_from_invalid_string": Meta {
                remarks: [],
                errors: [
                    Error {
                        kind: InvalidData,
                        data: {},
                    },
                ],
                original_length: None,
                original_value: Some(
                    Object(
                        {
                            "type": String(
                                "integer",
                            ),
                            "value": String(
                                "abc",
                            ),
                        },
                    ),
                ),
            },
            "missing_type": Meta {
                remarks: [],
                errors: [
                    Error {
                        kind: MissingAttribute,
                        data: {},
                    },
                ],
                original_length: None,
                original_value: Some(
                    Object(
                        {
                            "type": ~,
                            "value": String(
                                "value with missing type",
                            ),
                        },
                    ),
                ),
            },
            "missing_value": Meta {
                remarks: [],
                errors: [
                    Error {
                        kind: MissingAttribute,
                        data: {},
                    },
                ],
                original_length: None,
                original_value: Some(
                    Object(
                        {
                            "type": String(
                                "string",
                            ),
                            "value": ~,
                        },
                    ),
                ),
            },
            "unknown_type": Meta {
                remarks: [],
                errors: [
                    Error {
                        kind: InvalidData,
                        data: {},
                    },
                ],
                original_length: None,
                original_value: Some(
                    Object(
                        {
                            "type": String(
                                "custom",
                            ),
                            "value": String(
                                "test",
                            ),
                        },
                    ),
                ),
            },
            "valid_bool": Attribute {
                value: Bool(
                    true,
                ),
                type: Boolean,
                other: {},
            },
            "valid_double": Attribute {
                value: F64(
                    42.5,
                ),
                type: Double,
                other: {},
            },
            "valid_double_with_u64": Attribute {
                value: I64(
                    42,
                ),
                type: Double,
                other: {},
            },
            "valid_int_from_string": Meta {
                remarks: [],
                errors: [
                    Error {
                        kind: InvalidData,
                        data: {},
                    },
                ],
                original_length: None,
                original_value: Some(
                    Object(
                        {
                            "type": String(
                                "integer",
                            ),
                            "value": String(
                                "42",
                            ),
                        },
                    ),
                ),
            },
            "valid_int_i64": Attribute {
                value: I64(
                    -42,
                ),
                type: Integer,
                other: {},
            },
            "valid_int_u64": Attribute {
                value: I64(
                    42,
                ),
                type: Integer,
                other: {},
            },
            "valid_string": Attribute {
                value: String(
                    "test",
                ),
                type: String,
                other: {},
            },
            "valid_string_with_other": Attribute {
                value: String(
                    "test",
                ),
                type: String,
                other: {
                    "some_other_field": String(
                        "some_other_value",
                    ),
                },
            },
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
            attributes
                .get("sentry.browser.name")
                .unwrap()
                .value()
                .unwrap()
                .value
                .value,
            Value::String("Chrome".to_owned()).into(),
        );
        assert_eq!(
            attributes
                .get("sentry.browser.version")
                .unwrap()
                .value()
                .unwrap()
                .value
                .value,
            Value::String("131.0.0".to_owned()).into(),
        );
    }
}
