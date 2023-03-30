use std::collections::BTreeMap;
use std::fmt::{self, Display};

use relay_common::{MetricUnit, UnixTimestamp, Uuid};
use relay_general::protocol::{
    AbnormalMechanism, SessionAttributes, SessionErrored, SessionLike, SessionStatus,
};
use relay_metrics::{
    CounterType, Metric, MetricNamespace, MetricResourceIdentifier, MetricType, MetricValue,
    ParseMetricError,
};

use super::utils::with_tag;

/// Enumerates the most common session-names.
#[derive(Clone, Debug, PartialEq)]
pub enum SessionsKind {
    Session {
        counter: CounterType,
        timestamp: UnixTimestamp,
        tags: SessionsSessionTags,
    },
    User {
        distinct_id: String,
        timestamp: UnixTimestamp,
        tags: SessionsUserTags,
    },
    Error {
        id: Uuid,
        timestamp: UnixTimestamp,
        tags: SessionsErrorTags,
    },
}

impl SessionsKind {
    fn ty(&self) -> MetricType {
        match self {
            SessionsKind::Session { .. } => MetricType::Counter,
            SessionsKind::Error { .. } => MetricType::Set,
            SessionsKind::User { .. } => MetricType::Set,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionsErrorTags {
    release: String,
    environment: Option<String>,
    sdk: Option<String>,
}

impl SessionsErrorTags {
    fn new(common: (String, Option<String>, Option<String>)) -> Self {
        Self {
            release: common.0,
            environment: common.1,
            sdk: common.2,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionsSessionTags {
    status: SessionStatus,
    release: String,
    environment: Option<String>,
    sdk: Option<String>,
}

impl SessionsSessionTags {
    fn new(status: SessionStatus, common: (String, Option<String>, Option<String>)) -> Self {
        Self {
            status,
            release: common.0,
            environment: common.1,
            sdk: common.2,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionsUserTags {
    status: Option<SessionStatus>,
    abnormal_mechanism: Option<String>,
    release: String,
    environment: Option<String>,
    sdk: Option<String>,
}

impl SessionsUserTags {
    fn new(
        status: Option<SessionStatus>,
        common: (String, Option<String>, Option<String>),
    ) -> Self {
        Self {
            status,
            abnormal_mechanism: None,
            release: common.0,
            environment: common.1,
            sdk: common.2,
        }
    }
    fn with_abnormal(
        status: Option<SessionStatus>,
        abnormal_mechanism: String,
        common: (String, Option<String>, Option<String>),
    ) -> Self {
        let mut session = Self::new(status, common);
        session.abnormal_mechanism = Some(abnormal_mechanism);
        session
    }
}

impl From<SessionsUserTags> for BTreeMap<String, String> {
    fn from(value: SessionsUserTags) -> Self {
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        if let Some(status) = value.status {
            map.insert("session.status".to_string(), status.to_string());
        }

        if let Some(abnormal_mechanism) = value.abnormal_mechanism {
            map.insert("abnormal_mechanism".to_string(), abnormal_mechanism);
        }

        map.insert("release".to_string(), value.release);

        if let Some(environment) = value.environment {
            map.insert("environment".into(), environment);
        }

        if let Some(sdk) = value.sdk {
            map.insert("sdk".to_string(), sdk);
        }

        map
    }
}

impl From<SessionsErrorTags> for BTreeMap<String, String> {
    fn from(value: SessionsErrorTags) -> Self {
        let mut map: BTreeMap<String, String> = BTreeMap::new();

        map.insert("release".to_string(), value.release);

        if let Some(environment) = value.environment {
            map.insert("environment".into(), environment);
        }

        if let Some(sdk) = value.sdk {
            map.insert("sdk".to_string(), sdk);
        }

        map
    }
}

impl From<SessionsSessionTags> for BTreeMap<String, String> {
    fn from(value: SessionsSessionTags) -> Self {
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        map.insert("session.status".to_string(), value.status.to_string());

        map.insert("release".to_string(), value.release);

        if let Some(environment) = value.environment {
            map.insert("environment".into(), environment);
        }

        if let Some(sdk) = value.sdk {
            map.insert("sdk".to_string(), sdk);
        }

        map
    }
}

impl From<SessionsKind> for Metric {
    fn from(value: SessionsKind) -> Self {
        let mri = MetricResourceIdentifier::new(
            value.ty(),
            MetricNamespace::Sessions,
            value.to_string(),
            MetricUnit::None,
        );

        let (value, timestamp, tags) = match value {
            SessionsKind::Error {
                id,
                timestamp,
                tags,
            } => (MetricValue::set_from_display(id), timestamp, tags.into()),
            SessionsKind::User {
                distinct_id,
                timestamp,
                tags,
            } => (
                MetricValue::set_from_display(distinct_id),
                timestamp,
                tags.into(),
            ),
            SessionsKind::Session {
                counter,
                timestamp,
                tags,
            } => (MetricValue::Counter(counter), timestamp, tags.into()),
        };
        Metric::new(mri, value, timestamp, tags)
    }
}

impl Display for SessionsKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session { .. } => write!(f, "session"),
            Self::User { .. } => write!(f, "user"),
            Self::Error { .. } => write!(f, "error"),
        }
    }
}

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
    target: &mut Vec<Metric>,
    extract_abnormal_mechanism: bool,
) {
    let timestamp = match UnixTimestamp::from_datetime(session.started()) {
        Some(ts) => ts,
        None => {
            relay_log::error!("invalid session started timestamp: {}", session.started());
            return;
        }
    };

    let environment = attributes.environment.clone();
    let sdk = client.map(|s| s.to_owned());
    let release = attributes.release.clone();

    // These tags are in common with all session variants
    let common_session_tags = (release, environment, sdk);

    // Always capture with "init" tag for the first session update of a session. This is used
    // for adoption and as baseline for crash rates.
    if session.total_count() > 0 {
        target.push(
            SessionsKind::Session {
                counter: session.total_count() as f64,
                timestamp,
                tags: SessionsSessionTags::new(
                    SessionStatus::Unknown("init".to_string()),
                    common_session_tags.clone(),
                ),
            }
            .into(),
        );
    }

    // Mark the session as errored, which includes fatal sessions.
    if let Some(errors) = session.all_errors() {
        target.push(match errors {
            SessionErrored::Individual(session_id) => SessionsKind::Error {
                id: session_id,
                timestamp,
                tags: SessionsErrorTags::new(common_session_tags.clone()),
            }
            .into(),

            SessionErrored::Aggregated(count) => SessionsKind::Session {
                counter: count as f64,
                timestamp,
                tags: SessionsSessionTags::new(
                    SessionStatus::Unknown("errored_preaggr".to_string()),
                    common_session_tags.clone(),
                ),
            }
            .into(),
        });

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            target.push(
                SessionsKind::User {
                    distinct_id: distinct_id.clone(),
                    timestamp,
                    tags: SessionsUserTags::new(
                        Some(SessionStatus::Errored),
                        common_session_tags.clone(),
                    ),
                }
                .into(),
            );
        }
    } else if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
        // For session updates without errors, we collect the user without a session.status tag.
        // To get the number of healthy users (i.e. users without a single errored session), query
        // |users| - |users{session.status:errored}|
        target.push(
            SessionsKind::User {
                distinct_id: distinct_id.clone(),
                timestamp,
                tags: SessionsUserTags::new(None, common_session_tags.clone()),
            }
            .into(),
        )
    }

    // Record fatal sessions for crash rate computation. This is a strict subset of errored
    // sessions above.
    if session.abnormal_count() > 0 {
        target.push(
            SessionsKind::Session {
                counter: session.abnormal_count() as f64,
                timestamp,
                tags: SessionsSessionTags::new(
                    SessionStatus::Abnormal,
                    common_session_tags.clone(),
                ),
            }
            .into(),
        );

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            let tags = if extract_abnormal_mechanism
                && session.abnormal_mechanism() != AbnormalMechanism::None
            {
                SessionsUserTags::with_abnormal(
                    Some(SessionStatus::Abnormal),
                    session.abnormal_mechanism().to_string(),
                    common_session_tags.clone(),
                )
            } else {
                SessionsUserTags::new(Some(SessionStatus::Abnormal), common_session_tags.clone())
            };
            target.push(
                SessionsKind::User {
                    distinct_id: distinct_id.clone(),
                    timestamp,
                    tags,
                }
                .into(),
            )
        }
    }

    if session.crashed_count() > 0 {
        target.push(
            SessionsKind::Session {
                counter: session.crashed_count() as f64,
                timestamp,
                tags: SessionsSessionTags::new(SessionStatus::Crashed, common_session_tags.clone()),
            }
            .into(),
        );

        if let Some(distinct_id) = nil_to_none(session.distinct_id()) {
            target.push(
                SessionsKind::User {
                    distinct_id: distinct_id.clone(),
                    timestamp,
                    tags: SessionsUserTags::new(Some(SessionStatus::Crashed), common_session_tags),
                }
                .into(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use relay_general::protocol::{AbnormalMechanism, SessionAggregates, SessionUpdate};
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
        assert!(matches!(session_metric.value, MetricValue::Counter(_)));
        assert_eq!(session_metric.tags["session.status"], "init");
        assert_eq!(session_metric.tags["release"], "1.0.0");
        assert_eq!(session_metric.tags["sdk"], client);

        let user_metric = &metrics[1];
        assert_eq!(user_metric.timestamp, started());
        assert_eq!(user_metric.name, "s:sessions/user@none");
        assert!(matches!(user_metric.value, MetricValue::Set(_)));
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
        assert!(matches!(user_metric.value, MetricValue::Set(_)));
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
            assert!(matches!(session_metric.value, MetricValue::Set(_)));
            assert_eq!(session_metric.tags.len(), 1); // Only the release tag

            let user_metric = &metrics[expected_metrics - 1];
            assert_eq!(user_metric.timestamp, started());
            assert_eq!(user_metric.name, "s:sessions/user@none");
            assert!(matches!(user_metric.value, MetricValue::Set(_)));
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
        assert!(matches!(session_metric.value, MetricValue::Counter(_)));
        assert_eq!(session_metric.tags["session.status"], "crashed");

        let user_metric = &metrics[3];
        assert_eq!(user_metric.timestamp, started());
        assert_eq!(user_metric.name, "s:sessions/user@none");
        assert!(matches!(user_metric.value, MetricValue::Set(_)));
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
            assert!(matches!(session_metric.value, MetricValue::Counter(_)));
            assert_eq!(session_metric.tags["session.status"], "abnormal");

            let session_metric_tag_keys: Vec<String> =
                session_metric.tags.keys().cloned().collect();
            assert_eq!(session_metric_tag_keys, ["release", "session.status"]);

            let user_metric = &metrics[3];
            assert_eq!(user_metric.timestamp, started());
            assert_eq!(user_metric.name, "s:sessions/user@none");
            assert!(matches!(user_metric.value, MetricValue::Set(_)));
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
            Metric {
                name: "c:sessions/session@none",
                value: Counter(
                    135.0,
                ),
                timestamp: UnixTimestamp(1581084960),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "init",
                },
            },
            Metric {
                name: "c:sessions/session@none",
                value: Counter(
                    12.0,
                ),
                timestamp: UnixTimestamp(1581084960),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "errored_preaggr",
                },
            },
            Metric {
                name: "c:sessions/session@none",
                value: Counter(
                    5.0,
                ),
                timestamp: UnixTimestamp(1581084960),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "abnormal",
                },
            },
            Metric {
                name: "c:sessions/session@none",
                value: Counter(
                    7.0,
                ),
                timestamp: UnixTimestamp(1581084960),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "crashed",
                },
            },
            Metric {
                name: "c:sessions/session@none",
                value: Counter(
                    15.0,
                ),
                timestamp: UnixTimestamp(1581084961),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "init",
                },
            },
            Metric {
                name: "c:sessions/session@none",
                value: Counter(
                    3.0,
                ),
                timestamp: UnixTimestamp(1581084961),
                tags: {
                    "environment": "development",
                    "release": "my-project-name@1.0.0",
                    "sdk": "sentry-test/1.0",
                    "session.status": "errored_preaggr",
                },
            },
            Metric {
                name: "s:sessions/user@none",
                value: Set(
                    3097475539,
                ),
                timestamp: UnixTimestamp(1581084961),
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
