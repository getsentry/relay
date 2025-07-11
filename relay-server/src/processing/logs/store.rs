use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use relay_event_schema::processor::{ProcessValue, ProcessingState, process_value};
use relay_event_schema::protocol::{Attributes, OurLog};
use relay_protocol::{Annotated, Value};
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{AnyValue, TraceItem, TraceItemType, any_value};
use serde::Serialize;
use uuid::Uuid;

use crate::constants::DEFAULT_EVENT_RETENTION;
use crate::envelope::WithHeader;
use crate::processing::Counted;
use crate::processing::logs::{Error, Result};
use crate::services::outcome::DiscardReason;
use crate::services::store::StoreLog;

macro_rules! required {
    ($value:expr) => {{
        match $value {
            Annotated(Some(value), _) => value,
            Annotated(None, meta) => {
                relay_log::debug!(
                    "dropping log because of missing required field {} with meta {meta:?}",
                    stringify!($value),
                );
                return Err(Error::Invalid(DiscardReason::InvalidLog));
            }
        }
    }};
}

/// Context parameters for [`convert`].
#[derive(Debug, Clone, Copy)]
pub struct Context {
    /// Received time.
    pub received_at: DateTime<Utc>,
    /// Item scoping.
    pub scoping: Scoping,
    /// Storage retention in days.
    pub retention: Option<u16>,
}

pub fn convert(mut log: WithHeader<OurLog>, ctx: &Context) -> Result<StoreLog> {
    let quantities = log.quantities();

    let meta = collect_meta(&mut log.value);

    let log = required!(log.value);
    let timestamp = required!(log.timestamp);
    let attrs = log.attributes.0.unwrap_or_default();

    let trace_item = TraceItem {
        item_type: TraceItemType::Log.into(),
        organization_id: ctx.scoping.organization_id.value(),
        project_id: ctx.scoping.project_id.value(),
        received: Some(ts(ctx.received_at)),
        retention_days: ctx.retention.unwrap_or(DEFAULT_EVENT_RETENTION).into(),
        timestamp: Some(ts(timestamp.0)),
        trace_id: required!(log.trace_id).to_string(),
        item_id: Uuid::new_v7(timestamp.into()).as_bytes().to_vec(),
        attributes: attributes(meta, attrs),
        client_sample_rate: 1.0,
        server_sample_rate: 1.0,
    };

    Ok(StoreLog {
        trace_item,
        quantities,
    })
}

fn ts(dt: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: dt.timestamp(),
        nanos: i32::try_from(dt.timestamp_subsec_nanos()).unwrap_or(0),
    }
}

/// The schema of a 'metadata attribute' stored in EAP.
///
/// A metadata attribute is a regular attribute stored in EAP, but it carries metadata about fields
/// of the original payload processed by Relay and other components.
///
/// It is also a place to store other, non-processing related, metadata on attributes, for example
/// the `unit` of an attribute.
///
/// The attribute metadata itself is serialized as a JSON string.
#[derive(Debug, Serialize)]
struct AttributeMeta {
    /// Meta as it was extracted from Relay's annotated model.
    meta: relay_protocol::Meta,
}

/// Extracts and converts all metadata attached to individual fields into individual attributes
/// which can be attached to a [`TraceItem`].
fn collect_meta(log: &mut Annotated<OurLog>) -> HashMap<String, AnyValue> {
    let mut meta = relay_event_normalization::MetaCollectProcessor::new();

    if let Err(_) = process_value(log, &mut meta, ProcessingState::root()) {
        debug_assert!(false, "meta collect processor should never fail");
        return HashMap::new();
    }

    meta.into_inner()
        .into_iter()
        .filter_map(|(key, meta)| {
            let value = serde_json::to_string(&AttributeMeta { meta })
                .inspect_err(|err| {
                    debug_assert!(
                        false,
                        "attribute meta serialization should never fail: {err:?}"
                    )
                })
                .ok()?;

            let key = format!("sentry._meta.fields.{key}");
            let value = AnyValue {
                value: Some(any_value::Value::StringValue(value)),
            };
            Some((key, value))
        })
        .collect()
}

fn attributes(
    meta: HashMap<String, AnyValue>,
    attributes: Attributes,
) -> HashMap<String, AnyValue> {
    let mut result = meta;
    result.reserve(attributes.0.len());

    for (name, attribute) in attributes {
        let value = attribute
            .into_value()
            .and_then(|v| v.value.value.into_value());

        let Some(value) = value else {
            // Emit `_meta` attributes here with #4804.
            continue;
        };

        let Some(value) = (match value {
            Value::Bool(v) => Some(any_value::Value::BoolValue(v)),
            Value::I64(v) => Some(any_value::Value::IntValue(v)),
            Value::U64(v) => i64::try_from(v).ok().map(any_value::Value::IntValue),
            Value::F64(v) => Some(any_value::Value::DoubleValue(v)),
            Value::String(v) => Some(any_value::Value::StringValue(v)),
            // These cases do not happen, as they are not valid attributes
            // and they should have been filtered out before already.
            Value::Array(_) | Value::Object(_) => {
                debug_assert!(false, "unsupported log value");
                None
            }
        }) else {
            continue;
        };

        result.insert(name, AnyValue { value: Some(value) });
    }

    result
}

#[cfg(test)]
mod tests {
    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_event_schema::protocol::OurLogHeader;
    use relay_protocol::{Error as MetaError, FromValue, Meta};

    use super::*;

    macro_rules! ourlog {
        ($($tt:tt)*) => {{
           WithHeader {
               header: Some(OurLogHeader {
                   byte_size: Some(420),
                   other: Default::default(),
               }),
               value: OurLog::from_value(serde_json::json!($($tt)*).into())
           }
        }};
    }

    macro_rules! get_mut {
        ($e:expr) => {{ $e.value_mut().as_mut().unwrap() }};
    }

    fn test_context() -> Context {
        Context {
            received_at: DateTime::from_timestamp(1, 0).unwrap(),
            scoping: Scoping {
                organization_id: OrganizationId::new(1),
                project_id: ProjectId::new(42),
                project_key: "12333333333333333333333333333333".parse().unwrap(),
                key_id: Some(3),
            },
            retention: Some(42),
        }
    }

    #[test]
    fn test_log_meta() {
        let mut log = ourlog!({
            "timestamp": 946684800.0,
            "level": "info",
            "trace_id": "5B8EFFF798038103D269B633813FC60C",
            "span_id": "EEE19B7EC3C1B174",
            "body": "Example log record",
            "attributes": {
                "foo": {
                    "value": "9",
                    "type": "string"
                }
            }
        });

        {
            let log = get_mut!(log.value);
            log.body
                .meta_mut()
                .add_error(MetaError::expected("something in the body"));

            let attributes = get_mut!(log.attributes);
            attributes.insert_raw(
                "bar".to_owned(),
                Annotated(None, Meta::from_error(MetaError::expected("invalid type"))),
            );
        }

        let log = convert(log, &test_context()).unwrap();
        insta::assert_debug_snapshot!(log.trace_item.attributes, @r###"
        {
            "asd.attributes.bar": AnyValue {
                value: Some(
                    StringValue(
                        "{\"meta\":{\"err\":[[\"invalid_data\",{\"reason\":\"expected invalid type\"}]]}}",
                    ),
                ),
            },
            "foo": AnyValue {
                value: Some(
                    StringValue(
                        "9",
                    ),
                ),
            },
            "asd.body": AnyValue {
                value: Some(
                    StringValue(
                        "{\"meta\":{\"err\":[[\"invalid_data\",{\"reason\":\"expected something in the body\"}]]}}",
                    ),
                ),
            },
        }
        "###);
    }
}
