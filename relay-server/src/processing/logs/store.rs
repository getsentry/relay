use std::collections::HashMap;

use chrono::{DateTime, Utc};
use prost_types::Timestamp;
use relay_event_schema::protocol::{Attributes, OurLog};
use relay_protocol::{Annotated, Value};
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{AnyValue, TraceItem, TraceItemType, any_value};
use uuid::Uuid;

use crate::constants::DEFAULT_EVENT_RETENTION;
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

pub fn convert(log: Annotated<OurLog>, ctx: &Context) -> Result<StoreLog> {
    let quantities = log.quantities();

    let log = required!(log);
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
        attributes: attributes(attrs),
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

fn attributes(attributes: Attributes) -> HashMap<String, AnyValue> {
    let mut result = HashMap::with_capacity(attributes.0.len());

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
