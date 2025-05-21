//! Log processing code.
use std::sync::Arc;

use crate::services::processor::LogGroup;
use relay_config::Config;
use relay_dynamic_config::{Feature, GlobalConfig};
use relay_event_normalization::{FromUserAgentInfo, RawUserAgentInfo};
use relay_event_schema::protocol::{Attribute, BrowserContext};

use crate::envelope::ItemType;
use crate::services::processor::should_filter;
use crate::services::projects::project::ProjectInfo;
use crate::utils::{ItemAction, TypedEnvelope};
use crate::utils::{ManagedEnvelope, sample};

#[cfg(feature = "processing")]
use {
    crate::envelope::{ContainerItems, Item, ItemContainer},
    crate::services::outcome::{DiscardReason, Outcome},
    crate::services::processor::ProcessingError,
    relay_dynamic_config::ProjectConfig,
    relay_event_normalization::{ClientHints, SchemaProcessor},
    relay_event_schema::processor::{ProcessingState, process_value},
    relay_event_schema::protocol::{AttributeType, OurLog},
    relay_ourlogs::OtelLog,
    relay_pii::PiiProcessor,
    relay_protocol::{Annotated, ErrorKind, Value},
};

/// Removes logs from the envelope if the feature is not enabled.
pub fn filter(
    managed_envelope: &mut TypedEnvelope<LogGroup>,
    config: Arc<Config>,
    project_info: Arc<ProjectInfo>,
    global_config: &GlobalConfig,
) {
    let logging_disabled = should_filter(&config, &project_info, Feature::OurLogsIngestion);
    let logs_sampled = global_config
        .options
        .ourlogs_ingestion_sample_rate
        .map(sample)
        .unwrap_or(true);

    let action = match logging_disabled || !logs_sampled {
        true => ItemAction::DropSilently,
        false => ItemAction::Keep,
    };

    managed_envelope.retain_items(move |item| match item.ty() {
        ItemType::OtelLog | ItemType::Log => action.clone(),
        _ => ItemAction::Keep,
    });
}

/// Processes logs.
#[cfg(feature = "processing")]
pub fn process(
    managed_envelope: &mut TypedEnvelope<LogGroup>,
    project_info: Arc<ProjectInfo>,
) -> Result<(), ProcessingError> {
    let log_items = managed_envelope
        .envelope()
        .items()
        .filter(|item| matches!(item.ty(), ItemType::Log))
        .count();

    // The `Log` item must always be sent as an `ItemContainer`, currently it is not allowed to
    // send multiple containers for logs.
    //
    // This restriction may be lifted in the future, this is why this validation only happens
    // when processing is enabled, allowing it to be changed easily in the future.
    //
    // This limit mostly exists to incentivise SDKs to batch multiple logs into a single container,
    // technically it can be removed without issues.
    if log_items > 1 {
        return Err(ProcessingError::DuplicateItem(ItemType::Log));
    }

    let normalize_config = NormalizeOurLogConfig::new(managed_envelope);

    managed_envelope.retain_items(|item| {
        let mut logs = match item.ty() {
            ItemType::OtelLog => match serde_json::from_slice::<OtelLog>(&item.payload()) {
                Ok(otel_log) => match relay_ourlogs::otel_to_sentry_log(otel_log) {
                    Ok(log) => ContainerItems::from_elem(Annotated::new(log), 1),
                    Err(err) => {
                        relay_log::debug!("failed to convert OTel Log to Sentry Log: {:?}", err);
                        return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidLog));
                    }
                },
                Err(err) => {
                    relay_log::debug!("failed to parse OTel Log: {err}");
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidLog));
                }
            },

            ItemType::Log => match ItemContainer::parse(item) {
                Ok(logs) => {
                    let mut logs = logs.into_items();
                    for log in logs.iter_mut() {
                        relay_ourlogs::ourlog_merge_otel(log);
                    }
                    logs
                }
                Err(err) => {
                    relay_log::debug!("failed to parse logs: {err}");
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidLog));
                }
            },

            _ => return ItemAction::Keep,
        };

        for log in logs.iter_mut() {
            if let Err(e) = scrub(log, &project_info.config) {
                relay_log::error!("failed to scrub pii from log: {}", e);
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            }

            if let Err(e) = normalize(log, normalize_config.clone()) {
                relay_log::debug!("failed to normalize log: {}", e);
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            };
        }

        *item = {
            let mut item = Item::new(ItemType::Log);
            let container = ItemContainer::from(logs);
            if let Err(err) = container.write_to(&mut item) {
                relay_log::debug!("failed to serialize log: {}", err);
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            }
            item
        };

        ItemAction::Keep
    });

    Ok(())
}

/// Config needed to normalize a standalone log.
#[derive(Clone, Debug)]
struct NormalizeOurLogConfig {
    /// The user agent parsed from the request.
    user_agent: Option<String>,
    /// Client hints parsed from the request.
    client_hints: ClientHints<String>,
}

impl NormalizeOurLogConfig {
    fn new(managed_envelope: &ManagedEnvelope) -> Self {
        Self {
            user_agent: managed_envelope
                .envelope()
                .meta()
                .user_agent()
                .map(String::from),
            client_hints: managed_envelope.meta().client_hints().clone(),
        }
    }
}

#[cfg(feature = "processing")]
fn normalize(
    annotated_log: &mut Annotated<OurLog>,
    config: NormalizeOurLogConfig,
) -> Result<(), ProcessingError> {
    process_value(annotated_log, &mut SchemaProcessor, ProcessingState::root())?;

    let Some(log) = annotated_log.value_mut() else {
        return Err(ProcessingError::NoEventPayload);
    };

    process_attribute_types(log);
    populate_ua_fields(
        log,
        config.user_agent.as_deref(),
        config.client_hints.as_deref(),
    );

    Ok(())
}

fn populate_ua_fields(log: &mut OurLog, user_agent: Option<&str>, client_hints: ClientHints<&str>) {
    let attributes = log.attributes.get_or_insert_with(Default::default);
    if let Some(context) = BrowserContext::from_hints_or_ua(&RawUserAgentInfo {
        user_agent,
        client_hints,
    }) {
        if !attributes.contains_key("browser.name") {
            if let Some(name) = context.name.value() {
                attributes.insert(
                    "browser.name".to_string(),
                    Annotated::new(Attribute::new(
                        AttributeType::String,
                        Value::String(name.to_string()),
                    )),
                );
            }
        }

        if !attributes.contains_key("browser.version") {
            if let Some(version) = context.version.value() {
                attributes.insert(
                    "browser.version".to_string(),
                    Annotated::new(Attribute::new(
                        AttributeType::String,
                        Value::String(version.to_string()),
                    )),
                );
            }
        }
    }
}

#[cfg(feature = "processing")]
fn scrub(
    annotated_log: &mut Annotated<OurLog>,
    project_config: &ProjectConfig,
) -> Result<(), ProcessingError> {
    if let Some(ref config) = project_config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(annotated_log, &mut processor, ProcessingState::root())?;
    }
    let pii_config = project_config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?;
    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(annotated_log, &mut processor, ProcessingState::root())?;
    }

    Ok(())
}

#[cfg(feature = "processing")]
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
    use super::*;
    use crate::envelope::{Envelope, ItemType};
    use crate::services::processor::ProcessingGroup;
    use crate::utils::ManagedEnvelope;
    use bytes::Bytes;

    use relay_dynamic_config::GlobalConfig;

    use relay_system::Addr;

    fn params() -> (TypedEnvelope<LogGroup>, Arc<ProjectInfo>) {
        let bytes = Bytes::from(
            r#"{"dsn":"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42"}
{"type":"otel_log"}
{}
"#,
        );

        let dummy_envelope = Envelope::parse_bytes(bytes).unwrap();
        let mut project_info = ProjectInfo::default();
        project_info
            .config
            .features
            .0
            .insert(Feature::OurLogsIngestion);
        let project_info = Arc::new(project_info);

        let managed_envelope = ManagedEnvelope::new(
            dummy_envelope,
            Addr::dummy(),
            Addr::dummy(),
            ProcessingGroup::Log,
        );

        let managed_envelope = managed_envelope.try_into().unwrap();

        (managed_envelope, project_info)
    }

    #[test]
    fn test_logs_sampled_default() {
        let global_config = GlobalConfig::default();
        let config = Arc::new(Config::default());
        assert!(
            global_config
                .options
                .ourlogs_ingestion_sample_rate
                .is_none()
        );
        let (mut managed_envelope, project_info) = params();
        filter(&mut managed_envelope, config, project_info, &global_config);
        assert!(
            managed_envelope
                .envelope()
                .items()
                .any(|item| item.ty() == &ItemType::OtelLog),
            "{:?}",
            managed_envelope.envelope()
        );
    }

    #[test]
    fn test_logs_sampled_explicit() {
        let mut global_config = GlobalConfig::default();
        global_config.options.ourlogs_ingestion_sample_rate = Some(1.0);
        let config = Arc::new(Config::default());
        let (mut managed_envelope, project_info) = params();
        filter(&mut managed_envelope, config, project_info, &global_config);
        assert!(
            managed_envelope
                .envelope()
                .items()
                .any(|item| item.ty() == &ItemType::OtelLog),
            "{:?}",
            managed_envelope.envelope()
        );
    }

    #[test]
    fn test_logs_sampled_dropped() {
        let mut global_config = GlobalConfig::default();
        global_config.options.ourlogs_ingestion_sample_rate = Some(0.0);
        let config = Arc::new(Config::default());
        let (mut managed_envelope, project_info) = params();
        filter(&mut managed_envelope, config, project_info, &global_config);
        assert!(
            !managed_envelope
                .envelope()
                .items()
                .any(|item| item.ty() == &ItemType::OtelLog),
            "{:?}",
            managed_envelope.envelope()
        );
    }

    #[test]
    #[cfg(feature = "processing")]
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
                .get("browser.name")
                .unwrap()
                .value()
                .unwrap()
                .value
                .value,
            Value::String("Chrome".to_string()).into(),
        );
        assert_eq!(
            attributes
                .get("browser.version")
                .unwrap()
                .value()
                .unwrap()
                .value
                .value,
            Value::String("131.0.0".to_string()).into(),
        );
    }
}
