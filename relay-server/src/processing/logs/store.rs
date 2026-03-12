use std::collections::HashMap;

use chrono::{DateTime, Utc};
use relay_event_schema::protocol::{Attributes, OurLog, OurLogLevel, SpanId};
use relay_protocol::Annotated;
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{AnyValue, TraceItem, TraceItemType, any_value};
use uuid::Uuid;

use crate::envelope::WithHeader;
use crate::processing::logs::{Error, Result};
use crate::processing::utils::store::{
    extract_meta_attributes, proto_timestamp, quantities_to_trace_item_outcomes, uuid_to_item_id,
};
use crate::processing::{self, Counted, Retention};
use crate::services::outcome::DiscardReason;
use crate::services::store::StoreTraceItem;

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
    /// Item retention.
    pub retention: Retention,
}

pub fn convert(log: WithHeader<OurLog>, ctx: &Context) -> Result<StoreTraceItem> {
    let quantities = log.quantities();
    let payload_size_bytes = log
        .header
        .as_ref()
        .and_then(|h| h.byte_size)
        .unwrap_or_default();

    let log = required!(log.value);
    let timestamp = required!(log.timestamp);

    let meta = extract_meta_attributes(&log, &log.attributes);
    let attrs = log.attributes.0.unwrap_or_default();
    let fields = FieldAttributes {
        level: required!(log.level),
        timestamp,
        body: required!(log.body),
        span_id: log.span_id.into_value(),
        payload_size_bytes,
    };

    let trace_item = TraceItem {
        item_type: TraceItemType::Log.into(),
        organization_id: ctx.scoping.organization_id.value(),
        project_id: ctx.scoping.project_id.value(),
        received: Some(proto_timestamp(ctx.received_at)),
        retention_days: ctx.retention.standard.into(),
        downsampled_retention_days: ctx.retention.downsampled.into(),
        timestamp: Some(proto_timestamp(timestamp.0)),
        trace_id: required!(log.trace_id).to_string(),
        item_id: uuid_to_item_id(Uuid::new_v7(timestamp.into())),
        attributes: attributes(meta, attrs, fields),
        client_sample_rate: 1.0,
        server_sample_rate: 1.0,
        outcomes: Some(quantities_to_trace_item_outcomes(quantities, ctx.scoping)),
    };

    Ok(StoreTraceItem { trace_item })
}

/// Fields on the log message which are stored as fields.
struct FieldAttributes {
    /// The log level.
    ///
    /// See: [`OurLog::level`].
    level: OurLogLevel,
    /// The original timestamp when the log was created.
    ///
    /// See: [`OurLog::timestamp`].
    timestamp: relay_event_schema::protocol::Timestamp,
    /// The log body.
    ///
    /// See: [`OurLog::body`].
    body: String,
    /// The optionally associated span id.
    ///
    /// See: [`OurLog::span_id`].
    span_id: Option<SpanId>,
    /// Payload size as it is ingested.
    payload_size_bytes: u64,
}

/// Extracts all attributes of a log, combines it with extracted meta attributes.
fn attributes(
    meta: HashMap<String, AnyValue>,
    attributes: Attributes,
    fields: FieldAttributes,
) -> HashMap<String, AnyValue> {
    let mut result = meta;
    // +N, one for each field attribute added and some extra for potential meta.
    result.reserve(attributes.0.len() + 5 + 3);

    processing::utils::store::convert_attributes_into(&mut result, attributes);

    let FieldAttributes {
        level,
        timestamp,
        body,
        span_id,
        payload_size_bytes,
    } = fields;

    // Unconditionally override any prior set attributes with the same key, as they should always
    // come from the log itself.
    //
    // Ideally these attributes are marked as private in sentry-conventions and potentially
    // validated against.
    result.insert(
        "sentry.severity_text".to_owned(),
        AnyValue {
            value: Some(any_value::Value::StringValue(level.to_string())),
        },
    );

    let timestamp_nanos = timestamp
        .into_inner()
        .timestamp_nanos_opt()
        // We can expect valid timestamps at this point, clock drift correction / normalization
        // should've taken care of this already.
        .unwrap_or_default();

    result.insert(
        "sentry.timestamp_precise".to_owned(),
        AnyValue {
            value: Some(any_value::Value::IntValue(timestamp_nanos)),
        },
    );
    result.insert(
        "sentry.body".to_owned(),
        AnyValue {
            value: Some(any_value::Value::StringValue(body)),
        },
    );

    if let Some(span_id) = span_id {
        result.insert(
            "sentry.span_id".to_owned(),
            AnyValue {
                value: Some(any_value::Value::StringValue(span_id.to_string())),
            },
        );
    }

    result.insert(
        "sentry.payload_size_bytes".to_owned(),
        AnyValue {
            value: Some(any_value::Value::IntValue(payload_size_bytes as i64)),
        },
    );

    result
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_event_schema::protocol::{Attribute, AttributeType, AttributeValue, OurLogHeader};
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
            retention: Retention {
                standard: 42,
                downsampled: 43,
            },
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
            attributes.0.insert(
                "attr_meta".to_owned(),
                Annotated(None, Meta::from_error(MetaError::expected("meow"))),
            );
            attributes.0.insert(
                "value_meta".to_owned(),
                Annotated::new(Attribute {
                    value: AttributeValue {
                        ty: Annotated::new(AttributeType::String),
                        value: Annotated(
                            None,
                            Meta::from_error(MetaError::expected("something else")),
                        ),
                    },
                    other: Default::default(),
                }),
            );
        }

        let log = convert(log, &test_context()).unwrap();
        let attributes = log
            .trace_item
            .attributes
            .into_iter()
            .collect::<BTreeMap<_, _>>();

        insta::assert_debug_snapshot!(attributes, @r#"
        {
            "foo": AnyValue {
                value: Some(
                    StringValue(
                        "9",
                    ),
                ),
            },
            "sentry._meta.fields.attributes.attr_meta": AnyValue {
                value: Some(
                    StringValue(
                        "{\"meta\":{\"\":{\"err\":[[\"invalid_data\",{\"reason\":\"expected meow\"}]]}}}",
                    ),
                ),
            },
            "sentry._meta.fields.attributes.value_meta": AnyValue {
                value: Some(
                    StringValue(
                        "{\"meta\":{\"value\":{\"\":{\"err\":[[\"invalid_data\",{\"reason\":\"expected something else\"}]]}}}}",
                    ),
                ),
            },
            "sentry._meta.fields.body": AnyValue {
                value: Some(
                    StringValue(
                        "{\"meta\":{\"\":{\"err\":[[\"invalid_data\",{\"reason\":\"expected something in the body\"}]]}}}",
                    ),
                ),
            },
            "sentry.body": AnyValue {
                value: Some(
                    StringValue(
                        "Example log record",
                    ),
                ),
            },
            "sentry.payload_size_bytes": AnyValue {
                value: Some(
                    IntValue(
                        420,
                    ),
                ),
            },
            "sentry.severity_text": AnyValue {
                value: Some(
                    StringValue(
                        "info",
                    ),
                ),
            },
            "sentry.span_id": AnyValue {
                value: Some(
                    StringValue(
                        "eee19b7ec3c1b174",
                    ),
                ),
            },
            "sentry.timestamp_precise": AnyValue {
                value: Some(
                    IntValue(
                        946684800000000000,
                    ),
                ),
            },
        }
        "#);
    }
}
