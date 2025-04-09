//! Log processing code.
use std::sync::Arc;

use crate::services::processor::LogGroup;
use relay_config::Config;
use relay_dynamic_config::{Feature, GlobalConfig};

use crate::services::processor::should_filter;
use crate::services::projects::project::ProjectInfo;
use crate::utils::sample;
use crate::utils::{ItemAction, TypedEnvelope};

#[cfg(feature = "processing")]
use {
    crate::envelope::ContentType,
    crate::envelope::{Item, ItemType},
    crate::services::outcome::{DiscardReason, Outcome},
    crate::services::processor::ProcessingError,
    relay_dynamic_config::ProjectConfig,
    relay_event_schema::processor::{process_value, ProcessingState},
    relay_event_schema::protocol::{OurLog, OurLogAttributeType},
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

    managed_envelope.retain_items(|_| {
        if logging_disabled || !logs_sampled {
            ItemAction::DropSilently
        } else {
            ItemAction::Keep
        }
    });
}

/// Processes logs.
#[cfg(feature = "processing")]
pub fn process(managed_envelope: &mut TypedEnvelope<LogGroup>, project_info: Arc<ProjectInfo>) {
    managed_envelope.retain_items(|item| {
        let mut annotated_log = match item.ty() {
            ItemType::OtelLog => match serde_json::from_slice::<OtelLog>(&item.payload()) {
                Ok(otel_log) => Annotated::new(relay_ourlogs::otel_to_sentry_log(otel_log)),
                Err(err) => {
                    relay_log::debug!("failed to parse OTel Log: {}", err);
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidLog));
                }
            },
            ItemType::Log => match Annotated::<OurLog>::from_json_bytes(&item.payload()) {
                Ok(our_log) => relay_ourlogs::ourlog_merge_otel(our_log),
                Err(err) => {
                    relay_log::debug!("failed to parse Sentry Log: {}", err);
                    return ItemAction::Drop(Outcome::Invalid(DiscardReason::InvalidLog));
                }
            },

            _ => return ItemAction::Keep,
        };

        if let Err(e) = scrub(&mut annotated_log, &project_info.config) {
            relay_log::error!("failed to scrub pii from log: {}", e);
            return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
        }

        if let Err(e) = normalize(&mut annotated_log) {
            relay_log::debug!("failed to normalize log: {}", e);
            return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
        };

        let mut new_item = Item::new(ItemType::Log);
        let payload = match annotated_log.to_json() {
            Ok(payload) => payload,
            Err(err) => {
                relay_log::debug!("failed to serialize log: {}", err);
                return ItemAction::Drop(Outcome::Invalid(DiscardReason::Internal));
            }
        };
        new_item.set_payload(ContentType::Json, payload);

        *item = new_item;

        ItemAction::Keep
    });
}

#[cfg(feature = "processing")]
fn normalize(annotated_log: &mut Annotated<OurLog>) -> Result<(), ProcessingError> {
    let Some(log) = annotated_log.value_mut() else {
        return Err(ProcessingError::NoEventPayload);
    };

    process_attribute_types(log);

    Ok(())
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
    if let Some(data) = ourlog.attributes.value_mut() {
        for (_, attr) in data.iter_mut() {
            if let Some(attr) = attr.value_mut() {
                match (&mut attr.value.ty, &mut attr.value.value) {
                    (
                        Annotated(Some(ty), _),
                        Annotated(Some(ref mut value), ref mut value_meta),
                    ) => {
                        let attr_type = OurLogAttributeType::from(ty.clone());
                        let should_keep = match (attr_type, &*value) {
                            (OurLogAttributeType::Bool, Value::Bool(_)) => true,
                            (OurLogAttributeType::Int, Value::I64(_)) => true,
                            (OurLogAttributeType::Int, Value::U64(_)) => true,
                            (OurLogAttributeType::Int, Value::String(v)) => {
                                v.parse::<i64>().is_ok() || v.parse::<u64>().is_ok()
                            }
                            (OurLogAttributeType::Double, Value::F64(_)) => true,
                            (OurLogAttributeType::Double, Value::String(f)) => {
                                f.parse::<f64>().is_ok()
                            }
                            (OurLogAttributeType::String, Value::String(_)) => true,
                            _ => false,
                        };
                        if !should_keep {
                            value_meta.add_error(ErrorKind::InvalidData);
                            value_meta.set_original_value(Some(std::mem::replace(
                                value,
                                Value::String("".to_string()),
                            )));

                            attr.value.ty = Annotated::new(
                                OurLogAttributeType::Unknown("unknown".to_string())
                                    .as_str()
                                    .to_string(),
                            );
                        }
                    }
                    (Annotated(ty_opt, ty_meta), Annotated(value_opt, value_meta)) => {
                        if ty_opt.is_none() {
                            ty_meta.add_error(ErrorKind::MissingAttribute);
                        }
                        if value_opt.is_none() {
                            value_meta.add_error(ErrorKind::MissingAttribute);
                        }
                        attr.value.ty = Annotated::new(
                            OurLogAttributeType::Unknown("unknown".to_string())
                                .as_str()
                                .to_string(),
                        );
                        *value_opt = Some(Value::String("".to_string()));
                    }
                }
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
        assert!(global_config
            .options
            .ourlogs_ingestion_sample_rate
            .is_none());
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

    #[cfg(feature = "processing")]
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
                "valid_double_with_i64": {
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
            "invalid_int_from_invalid_string": OurLogAttribute {
                value: Annotated(
                    String(
                        "",
                    ),
                    Meta {
                        remarks: [],
                        errors: [
                            Error {
                                kind: InvalidData,
                                data: {},
                            },
                        ],
                        original_length: None,
                        original_value: Some(
                            String(
                                "abc",
                            ),
                        ),
                    },
                ),
                type: "unknown",
                other: {},
            },
            "missing_type": OurLogAttribute {
                value: Annotated(
                    String(
                        "value with missing type",
                    ),
                    Meta {
                        remarks: [],
                        errors: [
                            Error {
                                kind: MissingAttribute,
                                data: {},
                            },
                        ],
                        original_length: None,
                        original_value: None,
                    },
                ),
                type: Meta {
                    remarks: [],
                    errors: [
                        Error {
                            kind: MissingAttribute,
                            data: {},
                        },
                    ],
                    original_length: None,
                    original_value: None,
                },
                other: {},
            },
            "missing_value": OurLogAttribute {
                value: Meta {
                    remarks: [],
                    errors: [
                        Error {
                            kind: MissingAttribute,
                            data: {},
                        },
                    ],
                    original_length: None,
                    original_value: None,
                },
                type: Annotated(
                    "string",
                    Meta {
                        remarks: [],
                        errors: [
                            Error {
                                kind: MissingAttribute,
                                data: {},
                            },
                        ],
                        original_length: None,
                        original_value: None,
                    },
                ),
                other: {},
            },
            "unknown_type": OurLogAttribute {
                value: Annotated(
                    String(
                        "",
                    ),
                    Meta {
                        remarks: [],
                        errors: [
                            Error {
                                kind: InvalidData,
                                data: {},
                            },
                        ],
                        original_length: None,
                        original_value: Some(
                            String(
                                "test",
                            ),
                        ),
                    },
                ),
                type: "unknown",
                other: {},
            },
            "valid_bool": OurLogAttribute {
                value: Bool(
                    true,
                ),
                type: "boolean",
                other: {},
            },
            "valid_double": OurLogAttribute {
                value: F64(
                    42.5,
                ),
                type: "double",
                other: {},
            },
            "valid_double_with_i64": OurLogAttribute {
                value: I64(
                    -42,
                ),
                type: "double",
                other: {},
            },
            "valid_double_with_u64": OurLogAttribute {
                value: I64(
                    42,
                ),
                type: "double",
                other: {},
            },
            "valid_int_from_string": OurLogAttribute {
                value: String(
                    "42",
                ),
                type: "integer",
                other: {},
            },
            "valid_int_i64": OurLogAttribute {
                value: I64(
                    -42,
                ),
                type: "integer",
                other: {},
            },
            "valid_int_u64": OurLogAttribute {
                value: I64(
                    42,
                ),
                type: "integer",
                other: {},
            },
            "valid_string": OurLogAttribute {
                value: String(
                    "test",
                ),
                type: "string",
                other: {},
            },
            "valid_string_with_other": OurLogAttribute {
                value: String(
                    "test",
                ),
                type: "string",
                other: {
                    "some_other_field": String(
                        "some_other_value",
                    ),
                },
            },
        }
        "###);
    }
}
