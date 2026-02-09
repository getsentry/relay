use std::collections::{BTreeSet, HashMap};

use relay_metrics::{Bucket, BucketValue, MetricNamespace};
use relay_quotas::Scoping;
use sentry_protos::snuba::v1::{AnyValue, ArrayValue, TraceItem, TraceItemType, any_value};

use crate::processing::utils::store::uuid_to_item_id;

/// Converts a session [`Bucket`] into an EAP [`TraceItem`].
pub fn to_trace_item(scoping: Scoping, bucket: Bucket, retention: u16) -> Option<TraceItem> {
    if bucket.name.namespace() != MetricNamespace::Sessions {
        return None;
    }

    // Currently max 5 tags + 1 metric value.
    let mut attributes = HashMap::with_capacity(6);

    match bucket.name.try_name()? {
        "session" => {
            let BucketValue::Counter(v) = bucket.value else {
                return None;
            };
            // The metric counter uses floats to represent a counter, but this value represent an
            // integer, the total amount of sessions. Restore the original type and store it as
            // such (an integer).
            let count = v.to_f64() as i64;
            attributes.insert(
                "session_count".to_owned(),
                AnyValue {
                    value: Some(any_value::Value::IntValue(count)),
                },
            );
        }
        "user" => {
            let BucketValue::Set(set) = &bucket.value else {
                return None;
            };
            attributes.insert("user_id_hash".to_owned(), set_to_attribute_value(set));
        }
        "error" => {
            let BucketValue::Set(set) = &bucket.value else {
                return None;
            };
            attributes.insert(
                "errored_session_id_hash".to_owned(),
                set_to_attribute_value(set),
            );
        }
        _ => return None,
    }

    for (name, value) in bucket.tags.into_iter().filter_map(tag_to_attribute) {
        attributes.insert(name, value);
    }

    let uuid = uuid::Uuid::new_v4();
    Some(TraceItem {
        organization_id: scoping.organization_id.value(),
        project_id: scoping.project_id.value(),
        trace_id: uuid.to_string(),
        item_id: uuid_to_item_id(uuid),
        item_type: TraceItemType::UserSession.into(),
        timestamp: Some(prost_types::Timestamp {
            seconds: bucket.timestamp.as_secs() as i64,
            nanos: 0,
        }),
        received: bucket
            .metadata
            .received_at
            .map(|ts| prost_types::Timestamp {
                seconds: ts.as_secs() as i64,
                nanos: 0,
            }),
        retention_days: retention.into(),
        downsampled_retention_days: retention.into(),
        attributes,
        client_sample_rate: 1.0,
        server_sample_rate: 1.0,
    })
}

fn set_to_attribute_value(set: &BTreeSet<u32>) -> AnyValue {
    let values = set
        .iter()
        .map(|v| i64::from(*v))
        .map(any_value::Value::IntValue)
        .map(|value| AnyValue { value: Some(value) })
        .collect();

    AnyValue {
        value: Some(any_value::Value::ArrayValue(ArrayValue { values })),
    }
}

fn tag_to_attribute((name, value): (String, String)) -> Option<(String, AnyValue)> {
    let name = match name.as_str() {
        "session.status" => "status".to_owned(),
        "release" | "environment" | "sdk" | "abnormal_mechanism" => name,
        _ => return None,
    };

    let value = AnyValue {
        value: Some(any_value::Value::StringValue(value)),
    };

    Some((name, value))
}
