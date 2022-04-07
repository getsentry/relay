use std::collections::BTreeMap;

use relay_common::{UnixTimestamp, Uuid};
use relay_general::protocol::{SessionAttributes, SessionErrored, SessionLike, SessionStatus};
use relay_metrics::{DurationUnit, Metric, MetricUnit, MetricValue};

use super::utils::with_tag;

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

const METRIC_NAMESPACE: &str = "sessions";

pub fn extract_session_metrics<T: SessionLike>(
    attributes: &SessionAttributes,
    session: &T,
    target: &mut Vec<Metric>,
) {
    let timestamp = match UnixTimestamp::from_datetime(session.started()) {
        Some(ts) => ts,
        None => {
            relay_log::error!("invalid session started timestamp: {}", session.started());
            return;
        }
    };

    let mut tags = BTreeMap::new();
    tags.insert("release".to_owned(), attributes.release.clone());
    if let Some(ref environment) = attributes.environment {
        tags.insert("environment".to_owned(), environment.clone());
    }

    // Always capture with "init" tag for the first session update of a session. This is used
    // for adoption and as baseline for crash rates.
    if session.total_count() > 0 {
        target.push(Metric::new_mri(
            METRIC_NAMESPACE,
            "session",
            MetricUnit::None,
            MetricValue::Counter(session.total_count() as f64),
            timestamp,
            with_tag(&tags, "session.status", "init"),
        ));

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            target.push(Metric::new_mri(
                METRIC_NAMESPACE,
                "user",
                MetricUnit::None,
                MetricValue::set_from_str(distinct_id),
                timestamp,
                with_tag(&tags, "session.status", "init"),
            ));
        }
    }

    // Mark the session as errored, which includes fatal sessions.
    if let Some(errors) = session.errors() {
        target.push(match errors {
            SessionErrored::Individual(session_id) => Metric::new_mri(
                METRIC_NAMESPACE,
                "error",
                MetricUnit::None,
                MetricValue::set_from_display(session_id),
                timestamp,
                tags.clone(),
            ),
            SessionErrored::Aggregated(count) => Metric::new_mri(
                METRIC_NAMESPACE,
                "session",
                MetricUnit::None,
                MetricValue::Counter(count as f64),
                timestamp,
                with_tag(&tags, "session.status", "errored_preaggr"),
            ),
        });

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            target.push(Metric::new_mri(
                METRIC_NAMESPACE,
                "user",
                MetricUnit::None,
                MetricValue::set_from_str(distinct_id),
                timestamp,
                with_tag(&tags, "session.status", "errored"),
            ));
        }
    }

    // Record fatal sessions for crash rate computation. This is a strict subset of errored
    // sessions above.
    if session.abnormal_count() > 0 {
        target.push(Metric::new_mri(
            METRIC_NAMESPACE,
            "session",
            MetricUnit::None,
            MetricValue::Counter(session.abnormal_count() as f64),
            timestamp,
            with_tag(&tags, "session.status", SessionStatus::Abnormal),
        ));

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            target.push(Metric::new_mri(
                METRIC_NAMESPACE,
                "user",
                MetricUnit::None,
                MetricValue::set_from_str(distinct_id),
                timestamp,
                with_tag(&tags, "session.status", SessionStatus::Abnormal),
            ));
        }
    }

    if session.crashed_count() > 0 {
        target.push(Metric::new_mri(
            METRIC_NAMESPACE,
            "session",
            MetricUnit::None,
            MetricValue::Counter(session.crashed_count() as f64),
            timestamp,
            with_tag(&tags, "session.status", SessionStatus::Crashed),
        ));

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            target.push(Metric::new_mri(
                METRIC_NAMESPACE,
                "user",
                MetricUnit::None,
                MetricValue::set_from_str(distinct_id),
                timestamp,
                with_tag(&tags, "session.status", SessionStatus::Crashed),
            ));
        }
    }

    // Count durations for all exited/crashed sessions. Note that right now, in the product we
    // really only use durations from session.status=exited, but decided it may be worth ingesting
    // this data in case we need it. If we need to cut cost, this is one place to start though.
    if let Some((duration, status)) = session.final_duration() {
        target.push(Metric::new_mri(
            METRIC_NAMESPACE,
            "duration",
            MetricUnit::Duration(DurationUnit::Second),
            MetricValue::Distribution(duration),
            timestamp,
            with_tag(&tags, "session.status", status),
        ));
    }
}

#[cfg(test)]
mod tests {
    use relay_general::protocol::{SessionAggregates, SessionUpdate};
    use relay_metrics::MetricValue;

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
        use relay_general::protocol::SessionUpdate;
        use relay_metrics::MetricValue;

        let mut metrics = vec![];

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

        extract_session_metrics(&session.attributes, &session, &mut metrics);

        assert_eq!(metrics.len(), 2);

        let session_metric = &metrics[0];
        assert_eq!(session_metric.timestamp, started());
        assert_eq!(session_metric.name, "c:sessions/session@none");
        assert!(matches!(session_metric.value, MetricValue::Counter(_)));
        assert_eq!(session_metric.tags["session.status"], "init");
        assert_eq!(session_metric.tags["release"], "1.0.0");

        let user_metric = &metrics[1];
        assert_eq!(session_metric.timestamp, started());
        assert_eq!(user_metric.name, "s:sessions/user@none");
        assert!(matches!(user_metric.value, MetricValue::Set(_)));
        assert_eq!(session_metric.tags["session.status"], "init");
        assert_eq!(user_metric.tags["release"], "1.0.0");
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

        extract_session_metrics(&session.attributes, &session, &mut metrics);

        // A none-initial update will not trigger any metric if it's not errored/crashed
        assert_eq!(metrics.len(), 0);
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
            (update1, 4), // init == true, so expect 4 metrics
            (update2, 2),
            (update3, 2),
        ] {
            let mut metrics = vec![];
            extract_session_metrics(&update.attributes, &update, &mut metrics);

            assert_eq!(metrics.len(), expected_metrics);

            let session_metric = &metrics[expected_metrics - 2];
            assert_eq!(session_metric.timestamp, started());
            assert_eq!(session_metric.name, "s:sessions/error@none");
            assert!(matches!(session_metric.value, MetricValue::Set(_)));
            assert_eq!(session_metric.tags.len(), 1); // Only the release tag

            let user_metric = &metrics[expected_metrics - 1];
            assert_eq!(session_metric.timestamp, started());
            assert_eq!(user_metric.name, "s:sessions/user@none");
            assert!(matches!(user_metric.value, MetricValue::Set(_)));
            assert_eq!(user_metric.tags["session.status"], "errored");
            assert_eq!(user_metric.tags["release"], "1.0.0");
        }
    }

    #[test]
    fn test_extract_session_metrics_fatal() {
        for status in &[SessionStatus::Crashed, SessionStatus::Abnormal] {
            let mut session = SessionUpdate::parse(
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
            session.status = *status;

            let mut metrics = vec![];

            extract_session_metrics(&session.attributes, &session, &mut metrics);

            assert_eq!(metrics.len(), 4);

            assert_eq!(metrics[0].name, "s:sessions/error@none");
            assert_eq!(metrics[1].name, "s:sessions/user@none");
            assert_eq!(metrics[1].tags["session.status"], "errored");

            let session_metric = &metrics[2];
            assert_eq!(session_metric.timestamp, started());
            assert_eq!(session_metric.name, "c:sessions/session@none");
            assert!(matches!(session_metric.value, MetricValue::Counter(_)));
            assert_eq!(session_metric.tags["session.status"], status.to_string());

            let user_metric = &metrics[3];
            assert_eq!(session_metric.timestamp, started());
            assert_eq!(user_metric.name, "s:sessions/user@none");
            assert!(matches!(user_metric.value, MetricValue::Set(_)));
            assert_eq!(user_metric.tags["session.status"], status.to_string());
        }
    }

    #[test]
    fn test_extract_session_metrics_duration() {
        let mut metrics = vec![];

        let session = SessionUpdate::parse(
            r#"{
            "init": false,
            "started": "2021-04-26T08:00:00+0100",
            "attrs": {
                "release": "1.0.0"
            },
            "did": "user123",
            "status": "exited",
            "duration": 123.4
        }"#
            .as_bytes(),
        )
        .unwrap();

        extract_session_metrics(&session.attributes, &session, &mut metrics);

        assert_eq!(metrics.len(), 1);

        let duration_metric = &metrics[0];
        assert_eq!(duration_metric.name, "d:sessions/duration@second");
        assert!(matches!(
            duration_metric.value,
            MetricValue::Distribution(_)
        ));
    }

    #[test]
    fn test_extract_session_metrics_aggregate() {
        let mut metrics = vec![];

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
            extract_session_metrics(&session.attributes, aggregate, &mut metrics);
        }

        insta::assert_debug_snapshot!(metrics, @r###"
        [
            Metric {
                name: "c:sessions/session@none",
                unit: None,
                value: Counter(
                    135.0,
                ),
                timestamp: UnixTimestamp(1581084960),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "session.status": "init",
                },
            },
            Metric {
                name: "c:sessions/session@none",
                unit: None,
                value: Counter(
                    5.0,
                ),
                timestamp: UnixTimestamp(1581084960),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "session.status": "abnormal",
                },
            },
            Metric {
                name: "c:sessions/session@none",
                unit: None,
                value: Counter(
                    7.0,
                ),
                timestamp: UnixTimestamp(1581084960),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "session.status": "crashed",
                },
            },
            Metric {
                name: "c:sessions/session@none",
                unit: None,
                value: Counter(
                    15.0,
                ),
                timestamp: UnixTimestamp(1581084961),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "session.status": "init",
                },
            },
            Metric {
                name: "s:sessions/user@none",
                unit: None,
                value: Set(
                    3097475539,
                ),
                timestamp: UnixTimestamp(1581084961),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "session.status": "init",
                },
            },
            Metric {
                name: "c:sessions/session@none",
                unit: None,
                value: Counter(
                    3.0,
                ),
                timestamp: UnixTimestamp(1581084961),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "session.status": "errored_preaggr",
                },
            },
            Metric {
                name: "s:sessions/user@none",
                unit: None,
                value: Set(
                    3097475539,
                ),
                timestamp: UnixTimestamp(1581084961),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "session.status": "errored",
                },
            },
        ]
        "###);
    }
}
