//! Converts session data to EAP TraceItem format for the snuba-items topic.
//!
//! This module implements the double-write path for user sessions, sending session data
//! directly to the snuba-items Kafka topic as `i32::from(TraceItemType::UserSession)` TraceItems,
//! in addition to the legacy metrics extraction path.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use relay_event_schema::protocol::{
    AbnormalMechanism, SessionAggregateItem, SessionAttributes, SessionStatus, SessionUpdate,
};
use relay_quotas::{DataCategory, Scoping};
use sentry_protos::snuba::v1::{AnyValue, TraceItem, TraceItemType, any_value};
use uuid::Uuid;

use crate::processing::Retention;
use crate::processing::utils::store::{proto_timestamp, uuid_to_item_id};
use crate::services::store::StoreTraceItem;

/// UUID namespace for user sessions.
///
/// This must match the `USER_SESSION_NAMESPACE` in sentry's
/// `sentry/user_sessions/eap/constants.py` to ensure consistent trace_id and item_id
/// generation between relay and sentry.
///
/// Using a fixed namespace ensures:
/// - No collision with other trace types in EAP
/// - Deterministic IDs for ReplacingMergeTree deduplication
const USER_SESSION_NAMESPACE: Uuid = Uuid::from_bytes([
    0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x78, 0x90, 0xab, 0xcd, 0xef, 0x12, 0x34, 0x56, 0x78, 0x90,
]);

/// Context parameters for converting sessions to TraceItems.
#[derive(Debug, Clone, Copy)]
pub struct Context {
    /// Received time.
    pub received_at: DateTime<Utc>,
    /// Item scoping.
    pub scoping: Scoping,
    /// Item retention.
    pub retention: Retention,
}

/// Converts a `SessionUpdate` to a `StoreTraceItem` for the snuba-items topic.
///
/// The resulting TraceItem uses `i32::from(TraceItemType::UserSession)` and includes all
/// session attributes that are supported by the EAP user sessions schema.
pub fn convert_session_update(session: &SessionUpdate, ctx: &Context) -> StoreTraceItem {
    let session_id = session.session_id.to_string();

    // Generate deterministic trace_id from session_id using UUID5 with namespace.
    // This ensures the same session always gets the same trace_id.
    let trace_id = Uuid::new_v5(&USER_SESSION_NAMESPACE, session_id.as_bytes()).to_string();

    // Generate deterministic item_id for ReplacingMergeTree deduplication.
    // Include sequence to differentiate multiple updates for the same session.
    let item_id_input = format!("user_session_{}_{}", session_id, session.sequence);
    let item_id = uuid_to_item_id(Uuid::new_v5(
        &USER_SESSION_NAMESPACE,
        item_id_input.as_bytes(),
    ));

    // Determine if the session crashed based on status.
    let crashed = matches!(session.status, SessionStatus::Crashed);

    // Build attributes map matching sentry's UserSessionData fields.
    let mut attributes = HashMap::new();

    // Required attributes
    insert_string(&mut attributes, "session_id", &session_id);
    insert_bool(&mut attributes, "crashed", crashed);
    insert_int(&mut attributes, "error_count", session.errors as i64);

    // Optional attributes from session
    if let Some(ref distinct_id) = session.distinct_id {
        insert_string(&mut attributes, "user_id", distinct_id);
    }

    // Attributes from session attributes
    insert_string(&mut attributes, "release", &session.attributes.release);

    if let Some(ref environment) = session.attributes.environment {
        insert_string(&mut attributes, "environment", environment);
    }

    if let Some(ref user_agent) = session.attributes.user_agent {
        insert_string(&mut attributes, "user_agent", user_agent);
    }

    // Session status as string for debugging/filtering
    insert_string(&mut attributes, "status", &session.status.to_string());

    // Duration if available
    if let Some(duration) = session.duration {
        insert_double(&mut attributes, "duration", duration);
    }

    // Whether this is an init session
    insert_bool(&mut attributes, "init", session.init);

    // Abnormal mechanism (ANR info) if present
    if !matches!(session.abnormal_mechanism, AbnormalMechanism::None) {
        insert_string(
            &mut attributes,
            "abnormal_mechanism",
            &session.abnormal_mechanism.to_string(),
        );
    }

    let trace_item = TraceItem {
        organization_id: ctx.scoping.organization_id.value(),
        project_id: ctx.scoping.project_id.value(),
        trace_id,
        item_id,
        item_type: i32::from(TraceItemType::UserSession),
        timestamp: Some(proto_timestamp(session.timestamp)),
        received: Some(proto_timestamp(ctx.received_at)),
        retention_days: ctx.retention.standard.into(),
        downsampled_retention_days: ctx.retention.downsampled.into(),
        attributes,
        client_sample_rate: 1.0,
        server_sample_rate: 1.0,
    };

    StoreTraceItem {
        trace_item,
        quantities: smallvec::smallvec![(DataCategory::Session, 1)],
    }
}

/// Converts a `SessionAggregateItem` to multiple `StoreTraceItem`s for the snuba-items topic.
///
/// Session aggregates represent pre-aggregated session data. Each aggregate is expanded
/// into individual session rows to unify the format with regular session updates.
/// For example, an aggregate with `exited: 3, crashed: 2` produces 5 individual TraceItems.
pub fn convert_session_aggregate(
    aggregate: &SessionAggregateItem,
    release: &str,
    environment: Option<&str>,
    ctx: &Context,
) -> Vec<StoreTraceItem> {
    // Expand the aggregate into synthetic SessionUpdate objects, then convert each
    // using the same code path as regular session updates.
    expand_aggregate_to_sessions(
        aggregate,
        release,
        environment,
        ctx.scoping.project_id.value(),
    )
    .into_iter()
    .map(|session| convert_session_update(&session, ctx))
    .collect()
}

/// Expands a `SessionAggregateItem` into synthetic `SessionUpdate` objects.
///
/// Each status count in the aggregate becomes individual session updates.
/// This allows the same TraceItem conversion logic to be used for both
/// regular session updates and aggregates.
fn expand_aggregate_to_sessions(
    aggregate: &SessionAggregateItem,
    release: &str,
    environment: Option<&str>,
    project_id: u64,
) -> Vec<SessionUpdate> {
    let mut sessions = Vec::new();

    // Map each status count to (count, SessionStatus)
    let status_counts = [
        (aggregate.exited, SessionStatus::Exited),
        (aggregate.errored, SessionStatus::Errored),
        (aggregate.abnormal, SessionStatus::Abnormal),
        (aggregate.unhandled, SessionStatus::Unhandled),
        (aggregate.crashed, SessionStatus::Crashed),
    ];

    // Pre-compute values that are shared across all expanded sessions
    let distinct_id = aggregate.distinct_id.clone();
    let distinct_id_str = distinct_id.as_deref().unwrap_or("unknown");
    let timestamp_millis = aggregate.started.timestamp_millis();

    let mut index: u32 = 0;
    for (count, status) in status_counts {
        for _ in 0..count {
            // Generate a unique, deterministic session ID for this expanded row.
            let session_id_str = format!(
                "aggregate_{}_{}_{}_{}",
                timestamp_millis, distinct_id_str, project_id, index
            );
            let session_id = Uuid::new_v5(&USER_SESSION_NAMESPACE, session_id_str.as_bytes());

            let session = SessionUpdate {
                session_id,
                distinct_id: distinct_id.clone(),
                sequence: 0,
                init: true, // Aggregates represent completed sessions
                timestamp: aggregate.started,
                started: aggregate.started,
                duration: None,
                status: status.clone(),
                errors: 0, // Aggregates don't track per-session error counts
                attributes: SessionAttributes {
                    release: release.to_owned(),
                    environment: environment.map(str::to_owned),
                    ip_address: None,
                    user_agent: None,
                },
                abnormal_mechanism: AbnormalMechanism::None,
            };

            sessions.push(session);
            index += 1;
        }
    }

    sessions
}

fn insert_string(attrs: &mut HashMap<String, AnyValue>, key: &str, value: &str) {
    let value = AnyValue {
        value: Some(any_value::Value::StringValue(value.to_owned())),
    };
    attrs.insert(key.to_owned(), value);
}

fn insert_bool(attrs: &mut HashMap<String, AnyValue>, key: &str, value: bool) {
    let value = AnyValue {
        value: Some(any_value::Value::BoolValue(value)),
    };
    attrs.insert(key.to_owned(), value);
}

fn insert_int(attrs: &mut HashMap<String, AnyValue>, key: &str, value: i64) {
    let value = AnyValue {
        value: Some(any_value::Value::IntValue(value)),
    };
    attrs.insert(key.to_owned(), value);
}

fn insert_double(attrs: &mut HashMap<String, AnyValue>, key: &str, value: f64) {
    let value = AnyValue {
        value: Some(any_value::Value::DoubleValue(value)),
    };
    attrs.insert(key.to_owned(), value);
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use relay_base_schema::organization::OrganizationId;
    use relay_base_schema::project::ProjectId;
    use relay_event_schema::protocol::{AbnormalMechanism, SessionAttributes};

    use super::*;

    fn test_context() -> Context {
        Context {
            received_at: Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap(),
            scoping: Scoping {
                organization_id: OrganizationId::new(1),
                project_id: ProjectId::new(42),
                project_key: "12333333333333333333333333333333".parse().unwrap(),
                key_id: Some(3),
            },
            retention: Retention {
                standard: 90,
                downsampled: 90,
            },
        }
    }

    fn get_str_attr<'a>(item: &'a StoreTraceItem, key: &str) -> Option<&'a str> {
        item.trace_item
            .attributes
            .get(key)
            .and_then(|v| match v.value.as_ref()? {
                any_value::Value::StringValue(s) => Some(s.as_str()),
                _ => None,
            })
    }

    fn get_bool_attr(item: &StoreTraceItem, key: &str) -> Option<bool> {
        item.trace_item
            .attributes
            .get(key)
            .and_then(|v| match v.value.as_ref()? {
                any_value::Value::BoolValue(b) => Some(*b),
                _ => None,
            })
    }

    #[test]
    fn test_convert_session_update() {
        let session = SessionUpdate {
            session_id: Uuid::new_v4(),
            distinct_id: Some("user-123".to_owned()),
            sequence: 1,
            init: true,
            timestamp: Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap(),
            started: Utc.with_ymd_and_hms(2024, 1, 1, 9, 0, 0).unwrap(),
            duration: Some(3600.0),
            status: SessionStatus::Ok,
            errors: 2,
            attributes: SessionAttributes {
                release: "1.0.0".to_owned(),
                environment: Some("production".to_owned()),
                ip_address: None,
                user_agent: Some("TestAgent/1.0".to_owned()),
            },
            abnormal_mechanism: AbnormalMechanism::None,
        };

        let ctx = test_context();
        let result = convert_session_update(&session, &ctx);

        // Check item_type matches i32::from(TraceItemType::UserSession) (12)
        assert_eq!(
            result.trace_item.item_type,
            i32::from(TraceItemType::UserSession)
        );
        assert_eq!(result.trace_item.organization_id, 1);
        assert_eq!(result.trace_item.project_id, 42);
        assert_eq!(result.trace_item.retention_days, 90);

        // Check attributes
        let attrs = &result.trace_item.attributes;
        assert!(attrs.contains_key("session_id"));
        assert!(attrs.contains_key("crashed"));
        assert!(attrs.contains_key("error_count"));
        assert!(attrs.contains_key("user_id"));
        assert!(attrs.contains_key("release"));
        assert!(attrs.contains_key("environment"));
    }

    #[test]
    fn test_convert_crashed_session() {
        let session = SessionUpdate {
            session_id: Uuid::new_v4(),
            distinct_id: None,
            sequence: 0,
            init: false,
            timestamp: Utc::now(),
            started: Utc::now(),
            duration: None,
            status: SessionStatus::Crashed,
            errors: 1,
            attributes: SessionAttributes {
                release: "1.0.0".to_owned(),
                environment: None,
                ip_address: None,
                user_agent: None,
            },
            abnormal_mechanism: AbnormalMechanism::None,
        };

        let ctx = test_context();
        let result = convert_session_update(&session, &ctx);

        assert_eq!(get_bool_attr(&result, "crashed"), Some(true));
    }

    #[test]
    fn test_deterministic_ids() {
        let session = SessionUpdate {
            session_id: Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
            distinct_id: None,
            sequence: 0,
            init: true,
            timestamp: Utc::now(),
            started: Utc::now(),
            duration: None,
            status: SessionStatus::Ok,
            errors: 0,
            attributes: SessionAttributes {
                release: "1.0.0".to_owned(),
                environment: None,
                ip_address: None,
                user_agent: None,
            },
            abnormal_mechanism: AbnormalMechanism::None,
        };

        let ctx = test_context();

        // Convert the same session twice
        let result1 = convert_session_update(&session, &ctx);
        let result2 = convert_session_update(&session, &ctx);

        // IDs should be deterministic (same input = same output)
        assert_eq!(result1.trace_item.trace_id, result2.trace_item.trace_id);
        assert_eq!(result1.trace_item.item_id, result2.trace_item.item_id);
    }

    #[test]
    fn test_convert_session_aggregate_expands_to_multiple_rows() {
        let aggregate = SessionAggregateItem {
            started: Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap(),
            distinct_id: Some("user-456".to_owned()),
            exited: 3,
            errored: 1,
            abnormal: 0,
            unhandled: 0,
            crashed: 2,
        };

        let ctx = test_context();
        let results = convert_session_aggregate(&aggregate, "1.0.0", Some("production"), &ctx);

        // Should produce 6 rows: 3 exited + 1 errored + 2 crashed
        assert_eq!(results.len(), 6);

        // Check status distribution
        let statuses: Vec<_> = results
            .iter()
            .filter_map(|r| get_str_attr(r, "status"))
            .collect();
        assert_eq!(statuses.iter().filter(|&&s| s == "exited").count(), 3);
        assert_eq!(statuses.iter().filter(|&&s| s == "errored").count(), 1);
        assert_eq!(statuses.iter().filter(|&&s| s == "crashed").count(), 2);

        // Check that crashed sessions have crashed=true
        for result in &results {
            let is_crashed_status = get_str_attr(result, "status") == Some("crashed");
            assert_eq!(get_bool_attr(result, "crashed"), Some(is_crashed_status));
        }

        // All items should have common attributes
        for result in &results {
            assert_eq!(
                result.trace_item.item_type,
                i32::from(TraceItemType::UserSession)
            );
            assert_eq!(result.trace_item.organization_id, 1);
            assert_eq!(result.trace_item.project_id, 42);
            assert!(get_str_attr(result, "session_id").is_some());
            assert!(get_str_attr(result, "release").is_some());
            assert!(get_str_attr(result, "environment").is_some());
            assert!(get_str_attr(result, "user_id").is_some());
            assert!(get_bool_attr(result, "init").is_some());
        }
    }

    #[test]
    fn test_convert_empty_aggregate_produces_no_rows() {
        let aggregate = SessionAggregateItem {
            started: Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap(),
            distinct_id: None,
            exited: 0,
            errored: 0,
            abnormal: 0,
            unhandled: 0,
            crashed: 0,
        };

        let ctx = test_context();
        let results = convert_session_aggregate(&aggregate, "1.0.0", None, &ctx);

        assert!(results.is_empty());
    }

    #[test]
    fn test_aggregate_rows_have_unique_deterministic_ids() {
        let aggregate = SessionAggregateItem {
            started: Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap(),
            distinct_id: Some("user-789".to_owned()),
            exited: 2,
            errored: 0,
            abnormal: 0,
            unhandled: 0,
            crashed: 1,
        };

        let ctx = test_context();

        // Convert twice to verify determinism
        let results1 = convert_session_aggregate(&aggregate, "1.0.0", None, &ctx);
        let results2 = convert_session_aggregate(&aggregate, "1.0.0", None, &ctx);

        assert_eq!(results1.len(), results2.len());

        // Each row should have a unique item_id
        let ids1: Vec<_> = results1
            .iter()
            .map(|r| r.trace_item.item_id.clone())
            .collect();
        let ids2: Vec<_> = results2
            .iter()
            .map(|r| r.trace_item.item_id.clone())
            .collect();

        // All IDs should be unique within a result set
        let unique_ids1: std::collections::HashSet<_> = ids1.iter().collect();
        assert_eq!(unique_ids1.len(), ids1.len());

        // IDs should be deterministic across conversions
        assert_eq!(ids1, ids2);
    }
}
