use relay_common::time::UnixTimestamp;
use relay_event_schema::protocol::{
    AbnormalMechanism, SessionAttributes, SessionErrored, SessionLike, SessionStatus,
};
use relay_metrics::Bucket;
use uuid::Uuid;

use crate::metrics_extraction::sessions::types::{
    CommonTags, SessionMetric, SessionSessionTags, SessionUserTags,
};
use crate::metrics_extraction::IntoMetric;

pub mod types;

/// Convert contained nil UUIDs to None
fn nil_to_none(distinct_id: Option<&String>) -> Option<&String> {
    let distinct_id = distinct_id?;
    if let Ok(uuid) = distinct_id.parse::<Uuid>() {
        if uuid.is_nil() {
            return None;
        }
    }

    Some(distinct_id)
}

pub fn extract_session_metrics<T: SessionLike>(
    attributes: &SessionAttributes,
    session: &T,
    client: Option<&str>,
    target: &mut Vec<Bucket>,
    extract_abnormal_mechanism: bool,
) {
    let timestamp = match UnixTimestamp::from_datetime(session.started()) {
        Some(ts) => ts,
        None => {
            relay_log::error!(
                timestamp = %session.started(),
                "invalid session started timestamp"
            );
            return;
        }
    };

    let common_tags = CommonTags {
        release: attributes.release.clone(),
        environment: attributes.environment.clone(),
        sdk: client.map(|s| s.to_string()),
    };

    // Always capture with "init" tag for the first session update of a session. This is used
    // for adoption and as baseline for crash rates.
    if session.total_count() > 0 {
        target.push(
            SessionMetric::Session {
                counter: session.total_count() as f64,
                tags: SessionSessionTags {
                    status: "init".to_string(),
                    common_tags: common_tags.clone(),
                },
            }
            .into_metric(timestamp),
        );
    }

    // Mark the session as errored, which includes fatal sessions.
    if let Some(errors) = session.all_errors() {
        target.push(match errors {
            SessionErrored::Individual(session_id) => SessionMetric::Error {
                session_id,
                tags: common_tags.clone(),
            }
            .into_metric(timestamp),

            SessionErrored::Aggregated(count) => SessionMetric::Session {
                counter: count as f64,
                tags: SessionSessionTags {
                    status: "errored_preaggr".to_string(),
                    common_tags: common_tags.clone(),
                },
            }
            .into_metric(timestamp),
        });

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            target.push(
                SessionMetric::User {
                    distinct_id: distinct_id.clone(),
                    tags: SessionUserTags {
                        status: Some(SessionStatus::Errored),
                        abnormal_mechanism: None,
                        common_tags: common_tags.clone(),
                    },
                }
                .into_metric(timestamp),
            );
        }
    } else if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
        // For session updates without errors, we collect the user without a session.status tag.
        // To get the number of healthy users (i.e. users without a single errored session), query
        // |users| - |users{session.status:errored}|
        target.push(
            SessionMetric::User {
                distinct_id: distinct_id.clone(),
                tags: SessionUserTags {
                    status: None,
                    abnormal_mechanism: None,
                    common_tags: common_tags.clone(),
                },
            }
            .into_metric(timestamp),
        )
    }

    // Record fatal sessions for crash rate computation. This is a strict subset of errored
    // sessions above.
    if session.abnormal_count() > 0 {
        target.push(
            SessionMetric::Session {
                counter: session.abnormal_count() as f64,
                tags: SessionSessionTags {
                    status: "abnormal".to_string(),
                    common_tags: common_tags.clone(),
                },
            }
            .into_metric(timestamp),
        );

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            let abnormal_mechanism = if extract_abnormal_mechanism
                && session.abnormal_mechanism() != AbnormalMechanism::None
            {
                Some(session.abnormal_mechanism().to_string())
            } else {
                None
            };
            target.push(
                SessionMetric::User {
                    distinct_id: distinct_id.clone(),
                    tags: SessionUserTags {
                        status: Some(SessionStatus::Abnormal),
                        abnormal_mechanism,
                        common_tags: common_tags.clone(),
                    },
                }
                .into_metric(timestamp),
            )
        }
    }

    if session.crashed_count() > 0 {
        target.push(
            SessionMetric::Session {
                counter: session.crashed_count() as f64,
                tags: SessionSessionTags {
                    status: "crashed".to_string(),
                    common_tags: common_tags.clone(),
                },
            }
            .into_metric(timestamp),
        );

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            target.push(
                SessionMetric::User {
                    distinct_id: distinct_id.clone(),
                    tags: SessionUserTags {
                        status: Some(SessionStatus::Crashed),
                        abnormal_mechanism: None,
                        common_tags,
                    },
                }
                .into_metric(timestamp),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{AbnormalMechanism, SessionAggregates, SessionUpdate};
    use relay_metrics::BucketValue;

    use super::*;

    fn started() -> UnixTimestamp {
        UnixTimestamp::from_secs(1619420400)
    }

    #[test]
    fn test_nil_to_none() {
        assert!(nil_to_none(None).is_none());

        let asdf = Some("asdf".to_owned());
        assert_eq!(nil_to_none(asdf.as_ref()).unwrap(), "asdf");

        let nil = Some("00000000-0000-0000-0000-000000000000".to_owned());
        assert!(nil_to_none(nil.as_ref()).is_none());

        let nil2 = Some("00000000000000000000000000000000".to_owned());
        assert!(nil_to_none(nil2.as_ref()).is_none());

        let not_nil = Some("00000000-0000-0000-0000-000000000123".to_owned());
        assert_eq!(
            nil_to_none(not_nil.as_ref()).unwrap(),
            not_nil.as_ref().unwrap()
        );
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_extract_session_metrics() {
        let mut metrics = vec![];

        let client = "sentry-test/1.0";
        let session = SessionUpdate::parse(
            r#"{
            "init": true,
            "started": "2021-04-26T08:00:00+0100",
            "attrs": {
                "release": "1.0.0"
            },
            "did": "user123"
        }"#
            .as_bytes(),
        )
        .unwrap();

        extract_session_metrics(
            &session.attributes,
            &session,
            Some(client),
            &mut metrics,
            true,
        );

        assert_eq!(metrics.len(), 2);

        let session_metric = &metrics[0];
        assert_eq!(session_metric.timestamp, started());
        assert_eq!(session_metric.name, "c:sessions/session@none");
        assert!(matches!(session_metric.value, BucketValue::Counter(_)));
        assert_eq!(session_metric.tags["session.status"], "init");
        assert_eq!(session_metric.tags["release"], "1.0.0");
        assert_eq!(session_metric.tags["sdk"], client);

        let user_metric = &metrics[1];
        assert_eq!(user_metric.timestamp, started());
        assert_eq!(user_metric.name, "s:sessions/user@none");
        assert!(matches!(user_metric.value, BucketValue::Set(_)));
        assert!(!user_metric.tags.contains_key("session.status"));
        assert_eq!(user_metric.tags["release"], "1.0.0");
        assert_eq!(user_metric.tags["sdk"], client);
    }

    #[test]
    fn test_extract_session_metrics_ok() {
        let mut metrics = vec![];

        let session = SessionUpdate::parse(
            r#"{
                "init": false,
                "started": "2021-04-26T08:00:00+0100",
                "attrs": {
                    "release": "1.0.0"
                },
                "did": "user123"
            }"#
            .as_bytes(),
        )
        .unwrap();

        extract_session_metrics(&session.attributes, &session, None, &mut metrics, true);

        // A none-initial update which is not errored/crashed/abnormal will only emit a user metric.
        assert_eq!(metrics.len(), 1);
        let user_metric = &metrics[0];
        assert_eq!(user_metric.name, "s:sessions/user@none");
        assert!(matches!(user_metric.value, BucketValue::Set(_)));
        assert!(!user_metric.tags.contains_key("session.status"));
    }

    #[test]
    fn test_extract_session_metrics_errored() {
        let update1 = SessionUpdate::parse(
            r#"{
                "init": true,
                "started": "2021-04-26T08:00:00+0100",
                "attrs": {
                    "release": "1.0.0"
                },
                "did": "user123",
                "status": "errored"
            }"#
            .as_bytes(),
        )
        .unwrap();

        let mut update2 = update1.clone();
        update2.init = false;

        let mut update3 = update2.clone();
        update3.status = SessionStatus::Ok;
        update3.errors = 123;

        for (update, expected_metrics) in vec![
            (update1, 3), // init == true, so expect 3 metrics
            (update2, 2),
            (update3, 2),
        ] {
            let mut metrics = vec![];
            extract_session_metrics(&update.attributes, &update, None, &mut metrics, true);

            assert_eq!(metrics.len(), expected_metrics);

            let session_metric = &metrics[expected_metrics - 2];
            assert_eq!(session_metric.timestamp, started());
            assert_eq!(session_metric.name, "s:sessions/error@none");
            assert!(matches!(session_metric.value, BucketValue::Set(_)));
            assert_eq!(session_metric.tags.len(), 1); // Only the release tag

            let user_metric = &metrics[expected_metrics - 1];
            assert_eq!(user_metric.timestamp, started());
            assert_eq!(user_metric.name, "s:sessions/user@none");
            assert!(matches!(user_metric.value, BucketValue::Set(_)));
            assert_eq!(user_metric.tags["session.status"], "errored");
            assert_eq!(user_metric.tags["release"], "1.0.0");
        }
    }

    #[test]
    fn test_extract_session_metrics_fatal() {
        let session = SessionUpdate::parse(
            r#"{
                "init": false,
                "started": "2021-04-26T08:00:00+0100",
                "attrs": {
                    "release": "1.0.0"
                },
                "did": "user123",
                "status": "crashed"
            }"#
            .as_bytes(),
        )
        .unwrap();

        let mut metrics = vec![];

        extract_session_metrics(&session.attributes, &session, None, &mut metrics, true);

        assert_eq!(metrics.len(), 4);

        assert_eq!(metrics[0].name, "s:sessions/error@none");
        assert_eq!(metrics[1].name, "s:sessions/user@none");
        assert_eq!(metrics[1].tags["session.status"], "errored");

        let session_metric = &metrics[2];
        assert_eq!(session_metric.timestamp, started());
        assert_eq!(session_metric.name, "c:sessions/session@none");
        assert!(matches!(session_metric.value, BucketValue::Counter(_)));
        assert_eq!(session_metric.tags["session.status"], "crashed");

        let user_metric = &metrics[3];
        assert_eq!(user_metric.timestamp, started());
        assert_eq!(user_metric.name, "s:sessions/user@none");
        assert!(matches!(user_metric.value, BucketValue::Set(_)));
        assert_eq!(user_metric.tags["session.status"], "crashed");
    }

    #[test]
    fn test_extract_session_metrics_abnormal() {
        for (abnormal_mechanism, expected_tag_value) in [
            (None, None),
            (Some(AbnormalMechanism::None), None),
            (
                Some(AbnormalMechanism::AnrForeground),
                Some("anr_foreground"),
            ),
        ] {
            let mut session = SessionUpdate::parse(
                r#"{
                    "init": false,
                    "started": "2021-04-26T08:00:00+0100",
                    "attrs": {
                        "release": "1.0.0"
                    },
                    "did": "user123",
                    "status": "abnormal"
                }"#
                .as_bytes(),
            )
            .unwrap();

            if let Some(mechanism) = abnormal_mechanism {
                session.abnormal_mechanism = mechanism;
            }

            let mut metrics = vec![];

            extract_session_metrics(&session.attributes, &session, None, &mut metrics, true);

            assert_eq!(metrics.len(), 4);

            assert_eq!(metrics[0].name, "s:sessions/error@none");
            assert_eq!(metrics[1].name, "s:sessions/user@none");
            assert_eq!(metrics[1].tags["session.status"], "errored");

            let session_metric = &metrics[2];
            assert_eq!(session_metric.timestamp, started());
            assert_eq!(session_metric.name, "c:sessions/session@none");
            assert!(matches!(session_metric.value, BucketValue::Counter(_)));
            assert_eq!(session_metric.tags["session.status"], "abnormal");

            let session_metric_tag_keys: Vec<String> =
                session_metric.tags.keys().cloned().collect();
            assert_eq!(session_metric_tag_keys, ["release", "session.status"]);

            let user_metric = &metrics[3];
            assert_eq!(user_metric.timestamp, started());
            assert_eq!(user_metric.name, "s:sessions/user@none");
            assert!(matches!(user_metric.value, BucketValue::Set(_)));
            assert_eq!(user_metric.tags["session.status"], "abnormal");

            let user_metric_tag_keys: Vec<String> = user_metric.tags.keys().cloned().collect();
            if let Some(value) = expected_tag_value {
                assert_eq!(
                    user_metric_tag_keys,
                    ["abnormal_mechanism", "release", "session.status"]
                );
                assert_eq!(user_metric.tags["abnormal_mechanism"], value);
            } else {
                assert_eq!(user_metric_tag_keys, ["release", "session.status"]);
            }
        }
    }

    #[test]
    fn test_extract_session_metrics_aggregate() {
        let mut metrics = vec![];

        let client = "sentry-test/1.0";
        let session = SessionAggregates::parse(
            r#"{
                "aggregates": [
                    {
                    "started": "2020-02-07T14:16:00Z",
                    "exited": 123,
                    "abnormal": 5,
                    "crashed": 7
                    },
                    {
                    "started": "2020-02-07T14:16:01Z",
                    "did": "optional distinct user id",
                    "exited": 12,
                    "errored": 3
                    }
                ],
                "attrs": {
                    "release": "my-project-name@1.0.0",
                    "environment": "development"
                }
            }"#
            .as_bytes(),
        )
        .unwrap();

        for aggregate in &session.aggregates {
            extract_session_metrics(
                &session.attributes,
                aggregate,
                Some(client),
                &mut metrics,
                true,
            );
        }

        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Bucket {
                timestamp: UnixTimestamp(1581084960),
                width: 0,
                name: "c:sessions/session@none",
                value: Counter(
                    135.0,
                ),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "init",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1581084960),
                width: 0,
                name: "c:sessions/session@none",
                value: Counter(
                    12.0,
                ),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "errored_preaggr",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1581084960),
                width: 0,
                name: "c:sessions/session@none",
                value: Counter(
                    5.0,
                ),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "abnormal",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1581084960),
                width: 0,
                name: "c:sessions/session@none",
                value: Counter(
                    7.0,
                ),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "crashed",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1581084961),
                width: 0,
                name: "c:sessions/session@none",
                value: Counter(
                    15.0,
                ),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "init",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1581084961),
                width: 0,
                name: "c:sessions/session@none",
                value: Counter(
                    3.0,
                ),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "errored_preaggr",
                },
            },
            Bucket {
                timestamp: UnixTimestamp(1581084961),
                width: 0,
                name: "s:sessions/user@none",
                value: Set(
                    {
                        3097475539,
                    },
                ),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "errored",
                },
            },
        ]
        "###);
    }
}
