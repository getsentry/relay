//! Sampling logic for performing sampling decisions of incoming events.
//!
//! In order to allow Sentry to offer performance at scale, Relay extracts key `metrics` from all
//! transactions, but only forwards a random sample of raw transaction payloads to the upstream.
//! What exact percentage is sampled is determined by `dynamic sampling rules`, and depends on the
//! project, the environment, the transaction name, etc.
//!
//! In order to determine the sample rate, Relay uses a [`SamplingConfig`] which contains a set of
//! [`SamplingRule`](crate::config::SamplingRule)s that are matched against incoming data.
//!
//! # Types of Sampling
//!
//! There are two main types of dynamic sampling:
//!
//! 1. **Trace sampling** ensures that either all transactions of a trace are sampled or none. Rules
//!    have access to information in the [`DynamicSamplingContext`].
//! 2. **Transaction sampling** does not guarantee complete traces and instead applies to individual
//!   transactions. Rules have access to the full data in transaction events.
//!
//! # Components
//!
//! The sampling system implemented in Relay is composed of the following components:
//!
//! - [`DynamicSamplingContext`]: a container for information associated with the trace.
//! - [`SamplingRule`](crate::config::SamplingRule): a rule that is matched against incoming data.
//!   It specifies a condition that acts as predicate on the incoming payload.
//! - [`SamplingMatch`](crate::evaluation::SamplingMatch): the result of the matching of one or more
//!   rules.
//!
//! # How It Works
//!
//! - The incoming data is received by Relay.
//! - Relay resolves the [`SamplingConfig`] of the project to which the incoming data belongs.
//!   Additionally, it tries to resolve the configuration of the project that started the trace, the
//!   so-called "root project".
//! - The two [`SamplingConfig`]s are merged together and dynamic sampling evaluates a result. The
//!   algorithm goes over each rule and compute either a factor or sample rate based on the
//!   value of the rule.
//! - The [`SamplingMatch`](crate::evaluation::SamplingMatch) is finally returned containing the
//!   final `sample_rate` and some additional data that will be used in `relay_server` to perform
//!   the sampling decision.
//!
//! # Determinism
//!
//! The concept of determinism is a core concept for dynamic sampling. Deterministic sampling
//! decisions allow to:
//!
//! - Run dynamic sampling repeatedly over a **chain of Relays** (e.g., we don't want to drop an
//! event that was retained by a previous Relay and vice-versa).
//! - Run dynamic sampling independently on **transactions of the same trace** (e.g., we want to be
//! able to sample all the transactions of the same trace, even though some exceptions apply).
//!
//! In order to perform deterministic sampling, Relay uses teh trace ID or event ID as the seed for
//! the random number generator.
//!
//! # Examples
//!
//! ## [`SamplingConfig`]
//!
//! ```json
#![doc = include_str!("../tests/fixtures/sampling_config.json")]
//! ```
//!
//! ## [`DynamicSamplingContext`]
//!
//! ```json
#![doc = include_str!("../tests/fixtures/dynamic_sampling_context.json")]
//! ```

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![warn(missing_docs)]

pub mod condition;
pub mod config;
pub mod dsc;
pub mod evaluation;
mod utils;

pub use config::SamplingConfig;
pub use dsc::DynamicSamplingContext;

// TODO(ja): Enable tests
#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Duration as DateDuration, TimeZone, Utc};
    use relay_base_schema::events::EventType;
    use relay_base_schema::project::ProjectKey;
    use relay_common::glob3::GlobPatterns;
    use relay_common::uuid::Uuid;
    use serde_json::Value;
    use similar_asserts::assert_eq;

    use relay_event_schema::protocol::{
        Event, EventId, Exception, Headers, IpAddr, JsonLenientString, LenientString, LogEntry,
        PairList, Request, User, Values,
    };
    use relay_protocol::Annotated;

    use crate::condition::{
        AndCondition, EqCondOptions, EqCondition, GlobCondition, NotCondition, OrCondition,
        RuleCondition,
    };
    use crate::config::{
        DecayingFunction, RuleId, RuleType, SamplingMode, SamplingRule, SamplingValue, TimeRange,
    };
    use crate::dsc::TraceUserContext;
    use crate::evaluation::{merge_configs_and_match, MatchedRuleIds, SamplingMatch};

    use super::*;

    macro_rules! assert_transaction_match {
        ($res:expr, $sr:expr, $sd:expr, $( $id:expr ),*) => {
            assert_eq!(
                $res,
                Some(SamplingMatch {
                    sample_rate: $sr,
                    seed: $sd.id.value().unwrap().0,
                    matched_rule_ids: MatchedRuleIds(vec![$(RuleId($id),)*])
                })
            )
        }
    }

    macro_rules! assert_trace_match {
        ($res:expr, $sr:expr, $sd:expr, $( $id:expr ),*) => {
            assert_eq!(
                $res,
                Some(SamplingMatch {
                    sample_rate: $sr,
                    seed: $sd.trace_id,
                    matched_rule_ids: MatchedRuleIds(vec![$(RuleId($id),)*])
                })
            )
        }
    }

    macro_rules! assert_no_match {
        ($res:expr) => {
            assert_eq!($res, None)
        };
    }

    fn eq(name: &str, value: &[&str], ignore_case: bool) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: value.iter().map(|s| s.to_string()).collect(),
            options: EqCondOptions { ignore_case },
        })
    }

    fn eq_null(name: &str) -> RuleCondition {
        RuleCondition::Eq(EqCondition {
            name: name.to_owned(),
            value: Value::Null,
            options: EqCondOptions { ignore_case: true },
        })
    }

    fn glob(name: &str, value: &[&str]) -> RuleCondition {
        RuleCondition::Glob(GlobCondition {
            name: name.to_owned(),
            value: GlobPatterns::new(value.iter().map(|s| s.to_string()).collect()),
        })
    }

    fn and(conds: Vec<RuleCondition>) -> RuleCondition {
        RuleCondition::And(AndCondition { inner: conds })
    }

    fn or(conds: Vec<RuleCondition>) -> RuleCondition {
        RuleCondition::Or(OrCondition { inner: conds })
    }

    fn not(cond: RuleCondition) -> RuleCondition {
        RuleCondition::Not(NotCondition {
            inner: Box::new(cond),
        })
    }

    fn mocked_sampling_config_with_rules(rules: Vec<SamplingRule>) -> SamplingConfig {
        SamplingConfig {
            rules: vec![],
            rules_v2: rules,
            mode: SamplingMode::Received,
        }
    }

    fn mocked_event(
        event_type: EventType,
        transaction: &str,
        release: &str,
        environment: &str,
    ) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_string()),
            release: Annotated::new(LenientString(release.to_string())),
            environment: Annotated::new(environment.to_string()),
            ..Event::default()
        }
    }

    fn mocked_dynamic_sampling_context(
        transaction: &str,
        release: &str,
        environment: &str,
        user_segment: &str,
        user_id: &str,
        replay_id: Option<Uuid>,
    ) -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: "12345678901234567890123456789012".parse().unwrap(),
            release: Some(release.to_string()),
            environment: Some(environment.to_string()),
            transaction: Some(transaction.to_string()),
            sample_rate: Some(1.0),
            user: TraceUserContext {
                user_segment: user_segment.to_string(),
                user_id: user_id.to_string(),
            },
            replay_id,
            sampled: None,
            other: Default::default(),
        }
    }

    fn mocked_simple_dynamic_sampling_context(
        sample_rate: Option<f64>,
        release: Option<&str>,
        transaction: Option<&str>,
        environment: Option<&str>,
    ) -> DynamicSamplingContext {
        DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: "12345678901234567890123456789012".parse().unwrap(),
            release: release.map(|value| value.to_string()),
            environment: environment.map(|value| value.to_string()),
            transaction: transaction.map(|value| value.to_string()),
            sample_rate,
            user: Default::default(),
            other: Default::default(),
            replay_id: None,
            sampled: None,
        }
    }

    fn mocked_sampling_config(mode: SamplingMode) -> SamplingConfig {
        SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                SamplingRule {
                    condition: eq("event.transaction", &["healthcheck"], true),
                    sampling_value: SamplingValue::SampleRate { value: 0.1 },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["bar"], true),
                    sampling_value: SamplingValue::Factor { value: 1.0 },
                    ty: RuleType::Transaction,
                    id: RuleId(2),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("event.transaction", &["foo"], true),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Transaction,
                    id: RuleId(3),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                // We put this trace rule here just for testing purposes, even though it will never
                // be considered if put within a non-root project.
                SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(4),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ],
            mode,
        }
    }

    fn mocked_root_project_sampling_config(mode: SamplingMode) -> SamplingConfig {
        SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                SamplingRule {
                    condition: eq("trace.release", &["3.0"], true),
                    sampling_value: SamplingValue::Factor { value: 1.5 },
                    ty: RuleType::Trace,
                    id: RuleId(5),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: eq("trace.environment", &["dev"], true),
                    sampling_value: SamplingValue::SampleRate { value: 1.0 },
                    ty: RuleType::Trace,
                    id: RuleId(6),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: RuleCondition::all(),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(7),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ],
            mode,
        }
    }

    fn mocked_decaying_sampling_rule(
        id: u32,
        start: Option<DateTime<Utc>>,
        end: Option<DateTime<Utc>>,
        sampling_value: SamplingValue,
        decaying_fn: DecayingFunction,
    ) -> SamplingRule {
        SamplingRule {
            condition: RuleCondition::all(),
            sampling_value,
            ty: RuleType::Transaction,
            id: RuleId(id),
            time_range: TimeRange { start, end },
            decaying_fn,
        }
    }

    fn add_sampling_rule_to_config(
        sampling_config: &mut SamplingConfig,
        sampling_rule: SamplingRule,
    ) {
        sampling_config.rules_v2.push(sampling_rule);
    }

    fn match_against_rules(
        config: &SamplingConfig,
        event: &Event,
        dsc: &DynamicSamplingContext,
        now: DateTime<Utc>,
    ) -> Option<SamplingMatch> {
        SamplingMatch::match_against_rules(config.rules_v2.iter(), Some(event), Some(dsc), now)
    }

    #[test]
    /// test matching for various rules
    fn test_matches() {
        let conditions = [
            (
                "simple",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                    eq("trace.transaction", &["transaction1"], true),
                ]),
            ),
            (
                "glob releases",
                and(vec![
                    glob("trace.release", &["1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "glob transaction",
                and(vec![glob("trace.transaction", &["trans*"])]),
            ),
            (
                "multiple releases",
                and(vec![
                    glob("trace.release", &["2.1.1", "1.1.*"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "multiple user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["paid", "vip", "free"], true),
                ]),
            ),
            (
                "multiple transactions",
                and(vec![glob("trace.transaction", &["t22", "trans*", "t33"])]),
            ),
            (
                "case insensitive user segments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["ViP", "FrEe"], true),
                ]),
            ),
            (
                "multiple user environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq(
                        "trace.environment",
                        &["integration", "debug", "production"],
                        true,
                    ),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "case insensitive environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["DeBuG", "PrOd"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "all environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "undefined environments",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            ("match no conditions", and(vec![])),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".into()),
            user: TraceUserContext {
                user_segment: "vip".into(),
                user_id: "user-id".into(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".into()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc), "{failure_name}");
        }
    }

    #[test]
    /// test matching for various rules
    fn test_matches_events() {
        let conditions = [
            ("release", and(vec![glob("event.release", &["1.1.1"])])),
            (
                "transaction",
                and(vec![glob("event.transaction", &["trans*"])]),
            ),
            (
                "environment",
                or(vec![eq("event.environment", &["prod"], true)]),
            ),
        ];

        let evt = Event {
            release: Annotated::new(LenientString("1.1.1".to_owned())),
            environment: Annotated::new("prod".to_owned()),
            user: Annotated::new(User {
                ip_address: Annotated::new(IpAddr("127.0.0.1".to_owned())),
                ..Default::default()
            }),
            exceptions: Annotated::new(Values {
                values: Annotated::new(vec![Annotated::new(Exception {
                    value: Annotated::new(JsonLenientString::from(
                        "canvas.contentDocument".to_owned(),
                    )),
                    ..Default::default()
                })]),
                ..Default::default()
            }),
            request: Annotated::new(Request {
                headers: Annotated::new(Headers(
                    PairList(vec![Annotated::new((
                        Annotated::new("user-agent".into()),
                        Annotated::new("Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)".into()),
                    ))]))),
                ..Default::default()
            }),
            logentry: Annotated::new(LogEntry {
                formatted: Annotated::new("abc".to_owned().into()),
                ..Default::default()
            }),
            transaction: Annotated::new("transaction1".into()),
            ..Default::default()
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&evt), "{failure_name}");
        }
    }

    #[test]
    fn test_or_combinator() {
        let conditions = [
            (
                "both",
                true,
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "first",
                true,
                or(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            (
                "second",
                true,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                or(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            ("empty", false, or(vec![])),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    fn test_and_combinator() {
        let conditions = [
            (
                "both",
                true,
                and(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "first",
                false,
                and(vec![
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            (
                "second",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["vip"], true),
                ]),
            ),
            (
                "none",
                false,
                and(vec![
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user.segment", &["all"], true),
                ]),
            ),
            ("empty", true, and(vec![])),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    fn test_not_combinator() {
        let conditions = [
            (
                "not true",
                false,
                not(eq("trace.environment", &["debug"], true)),
            ),
            (
                "not false",
                true,
                not(eq("trace.environment", &["prod"], true)),
            ),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, expected, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(condition.matches(&dsc) == *expected, "{failure_name}");
        }
    }

    #[test]
    /// test various rules that do not match
    fn test_does_not_match() {
        let conditions = [
            (
                "release",
                and(vec![
                    glob("trace.release", &["1.1.2"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "user segment",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["debug"], true),
                    eq("trace.user", &["all"], true),
                ]),
            ),
            (
                "environment",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.environment", &["prod"], true),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
            (
                "transaction",
                and(vec![
                    glob("trace.release", &["1.1.1"]),
                    glob("trace.transaction", &["t22"]),
                    eq("trace.user", &["vip"], true),
                ]),
            ),
        ];

        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        for (rule_test_name, condition) in conditions.iter() {
            let failure_name = format!("Failed on test: '{rule_test_name}'!!!");
            assert!(!condition.matches(&dsc), "{failure_name}");
        }
    }

    #[test]
    fn test_partial_trace_matches() {
        let condition = and(vec![
            eq("trace.environment", &["debug"], true),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing release"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.environment", &["debug"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext::default(),
            replay_id: Some(Uuid::new_v4()),
            environment: Some("debug".to_string()),
            transaction: Some("transaction1".into()),
            sample_rate: None,
            sampled: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing user segment"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: None,
            sampled: None,
            environment: None,
            transaction: Some("transaction1".into()),
            sample_rate: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing environment"
        );

        let condition = and(vec![
            glob("trace.release", &["1.1.1"]),
            eq("trace.user.segment", &["vip"], true),
        ]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user: TraceUserContext {
                user_segment: "vip".to_owned(),
                user_id: "user-id".to_owned(),
            },
            replay_id: None,
            sampled: None,
            environment: Some("debug".to_string()),
            transaction: None,
            sample_rate: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing transaction"
        );
        let condition = and(vec![]);
        let dsc = DynamicSamplingContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user: TraceUserContext::default(),
            replay_id: None,
            sampled: None,
            environment: None,
            transaction: None,
            sample_rate: None,
            other: BTreeMap::new(),
        };

        assert!(
            condition.matches(&dsc),
            "did not match with missing release, user segment, environment and transaction"
        );
    }

    #[test]
    /// test that the multi-matching returns none in case there is no match.
    fn test_multi_matching_with_transaction_event_non_decaying_rules_and_no_match() {
        let result = match_against_rules(
            &mocked_sampling_config_with_rules(vec![
                SamplingRule {
                    condition: and(vec![eq("event.transaction", &["foo"], true)]),
                    sampling_value: SamplingValue::Factor { value: 2.0 },
                    ty: RuleType::Transaction,
                    id: RuleId(1),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
                SamplingRule {
                    condition: and(vec![
                        glob("trace.release", &["1.1.1"]),
                        eq("trace.environment", &["prod"], true),
                    ]),
                    sampling_value: SamplingValue::SampleRate { value: 0.5 },
                    ty: RuleType::Trace,
                    id: RuleId(2),
                    time_range: Default::default(),
                    decaying_fn: Default::default(),
                },
            ]),
            &mocked_event(EventType::Transaction, "healthcheck", "1.1.1", "testing"),
            &mocked_dynamic_sampling_context(
                "root_transaction",
                "1.1.1",
                "debug",
                "vip",
                "user-id",
                None,
            ),
            Utc::now(),
        );
        assert_eq!(result, None, "did not return none for no match");
    }

    #[test]
    /// Tests that the multi-matching works for a mixture of trace and transaction rules with interleaved strategies.
    fn test_match_against_rules_with_multiple_event_types_non_decaying_rules_and_matches() {
        let config = mocked_sampling_config_with_rules(vec![
            SamplingRule {
                condition: and(vec![glob("event.transaction", &["*healthcheck*"])]),
                sampling_value: SamplingValue::SampleRate { value: 0.1 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![glob("trace.environment", &["*dev*"])]),
                sampling_value: SamplingValue::SampleRate { value: 1.0 },
                ty: RuleType::Trace,
                id: RuleId(2),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![eq("event.transaction", &["foo"], true)]),
                sampling_value: SamplingValue::Factor { value: 2.0 },
                ty: RuleType::Transaction,
                id: RuleId(3),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![
                    glob("trace.release", &["1.1.1"]),
                    eq("trace.user.segment", &["vip"], true),
                ]),
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Trace,
                id: RuleId(4),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["prod"], true),
                ]),
                sampling_value: SamplingValue::Factor { value: 1.5 },
                ty: RuleType::Trace,
                id: RuleId(5),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
            SamplingRule {
                condition: and(vec![]),
                sampling_value: SamplingValue::SampleRate { value: 0.02 },
                ty: RuleType::Trace,
                id: RuleId(6),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        ]);

        // early return of first rule
        let event = mocked_event(EventType::Transaction, "healthcheck", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "debug",
            "vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_transaction_match!(result, 0.1, event, 1);

        // early return of second rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "dev",
            "vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 1.0, dsc, 2);

        // factor match third rule and early return sixth rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.2",
            "testing",
            "non-vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 0.04, dsc, 3, 6);

        // factor match third rule and early return fourth rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 1.0, dsc, 3, 4);

        // factor match third, fifth rule and early return sixth rule
        let event = mocked_event(EventType::Transaction, "foo", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "non-vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 0.06, dsc, 3, 5, 6);

        // factor match fifth and early return sixth rule
        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "non-vip",
            "user-id",
            None,
        );
        let result = match_against_rules(&config, &event, &dsc, Utc::now());
        assert_trace_match!(result, 0.03, dsc, 5, 6);
    }

    #[test]
    /// Test that the multi-matching works for a mixture of decaying and non-decaying rules.
    fn test_match_against_rules_with_trace_event_type_decaying_rules_and_matches() {
        let config = mocked_sampling_config_with_rules(vec![
            SamplingRule {
                condition: and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["dev"], true),
                ]),
                sampling_value: SamplingValue::Factor { value: 2.0 },
                ty: RuleType::Trace,
                id: RuleId(1),
                time_range: TimeRange {
                    start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
                    end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
                },
                decaying_fn: DecayingFunction::Linear { decayed_value: 1.0 },
            },
            SamplingRule {
                condition: and(vec![
                    eq("trace.release", &["1.1.1"], true),
                    eq("trace.environment", &["prod"], true),
                ]),
                sampling_value: SamplingValue::SampleRate { value: 0.6 },
                ty: RuleType::Trace,
                id: RuleId(2),
                time_range: TimeRange {
                    start: Some(Utc.with_ymd_and_hms(1970, 10, 10, 0, 0, 0).unwrap()),
                    end: Some(Utc.with_ymd_and_hms(1970, 10, 12, 0, 0, 0).unwrap()),
                },
                decaying_fn: DecayingFunction::Linear { decayed_value: 0.3 },
            },
            SamplingRule {
                condition: and(vec![]),
                sampling_value: SamplingValue::SampleRate { value: 0.02 },
                ty: RuleType::Trace,
                id: RuleId(3),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        ]);

        // factor match first rule and early return third rule
        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "dev",
            "vip",
            "user-id",
            None,
        );
        // We will use a time in the middle of 10th and 11th.
        let result = match_against_rules(
            &config,
            &event,
            &dsc,
            Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap(),
        );
        assert_trace_match!(result, 0.03, dsc, 1, 3);

        // early return second rule
        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "vip",
            "user-id",
            None,
        );
        // We will use a time in the middle of 10th and 11th.
        let result = match_against_rules(
            &config,
            &event,
            &dsc,
            Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap(),
        );
        assert!(matches!(result, Some(SamplingMatch { .. })));
        if let Some(spec) = result {
            assert!(
                (spec.sample_rate - 0.45).abs() < f64::EPSILON, // 0.45
                "did not use the sample rate of the second rule"
            )
        }

        // early return third rule
        let event = mocked_event(EventType::Transaction, "transaction", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "testing",
            "vip",
            "user-id",
            None,
        );
        // We will use a time in the middle of 10th and 11th.
        let result = match_against_rules(
            &config,
            &event,
            &dsc,
            Utc.with_ymd_and_hms(1970, 10, 11, 0, 0, 0).unwrap(),
        );
        assert_trace_match!(result, 0.02, dsc, 3);
    }

    #[test]
    /// test that the correct match is performed when replay id is present in the dsc.
    fn test_sampling_match_with_trace_replay_id() {
        let event = mocked_event(EventType::Transaction, "healthcheck", "1.1.1", "testing");
        let dsc = mocked_dynamic_sampling_context(
            "root_transaction",
            "1.1.1",
            "prod",
            "vip",
            "user-id",
            Some(Uuid::new_v4()),
        );

        let result = match_against_rules(
            &mocked_sampling_config_with_rules(vec![SamplingRule {
                condition: and(vec![not(eq_null("trace.replay_id"))]),
                sampling_value: SamplingValue::SampleRate { value: 1.0 },
                ty: RuleType::Trace,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            }]),
            &event,
            &dsc,
            Utc::now(),
        );
        assert_trace_match!(result, 1.0, dsc, 1)
    }

    #[test]
    /// Tests that no match is done when there are no matching rules.
    fn test_get_sampling_match_result_with_no_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            None,
            None,
            Some(&event),
            Utc::now(),
        );
        assert_no_match!(result);
    }

    #[test]
    /// Tests that matching is still done on the transaction rules in case trace params are invalid.
    fn test_get_sampling_match_result_with_invalid_trace_params() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);

        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");
        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            None,
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 3);

        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0", "");
        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            None,
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that a match with early return is done in the project sampling config.
    fn test_get_sampling_match_result_with_project_config_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that a match with early return is done in the root project sampling config.
    fn test_get_sampling_match_result_with_root_project_config_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));
        let event = mocked_event(EventType::Transaction, "my_transaction", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_trace_match!(result, 1.0, dsc, 6);
    }

    #[test]
    /// Tests that the multiple matches are done across root and non-root project sampling configs.
    fn test_get_sampling_match_result_with_both_project_configs_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None);
        let event = mocked_event(EventType::Transaction, "bar", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_trace_match!(result, 0.75, dsc, 2, 5, 7);
    }

    #[test]
    /// Tests that a match is done when no dynamic sampling context and root project state are
    /// available.
    fn test_get_sampling_match_result_with_no_dynamic_sampling_context_and_no_root_project_state() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "foo", "1.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            None,
            None,
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 3);
    }

    #[test]
    /// Tests that a match is done and the sample rate is adjusted when sampling mode is total.
    fn test_get_sampling_match_result_with_total_sampling_mode_in_project_state() {
        let sampling_config = mocked_sampling_config(SamplingMode::Total);
        let root_project_sampling_config = mocked_root_project_sampling_config(SamplingMode::Total);
        let dsc = mocked_simple_dynamic_sampling_context(Some(0.8), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.625, event, 3);
    }

    #[test]
    /// Tests that the correct match is raised in case we have unsupported rules with processing both
    /// enabled and disabled.
    fn test_get_sampling_match_result_with_unsupported_rules() {
        let mut sampling_config = mocked_sampling_config(SamplingMode::Received);
        add_sampling_rule_to_config(
            &mut sampling_config,
            SamplingRule {
                condition: RuleCondition::Unsupported,
                sampling_value: SamplingValue::SampleRate { value: 0.5 },
                ty: RuleType::Transaction,
                id: RuleId(1),
                time_range: Default::default(),
                decaying_fn: Default::default(),
            },
        );

        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");

        let result = merge_configs_and_match(
            false,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_no_match!(result);

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.5, event, 3);
    }

    #[test]
    /// Tests that a no match is raised in case we have an unsupported sampling mode and a match.
    fn test_get_sampling_match_result_with_unsupported_sampling_mode_and_match() {
        let sampling_config = mocked_sampling_config(SamplingMode::Unsupported);
        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Unsupported);
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let event = mocked_event(EventType::Transaction, "foo", "2.0", "");

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_no_match!(result);
    }

    #[test]
    /// Tests that only transaction rules are matched in case no root project or dsc are supplied.
    fn test_get_sampling_match_result_with_invalid_root_project_and_dsc_combination() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let event = mocked_event(EventType::Transaction, "healthcheck", "2.0", "");

        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, None);
        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            None,
            Some(&dsc),
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);

        let root_project_sampling_config =
            mocked_root_project_sampling_config(SamplingMode::Received);
        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_project_sampling_config),
            None,
            Some(&event),
            Utc::now(),
        );
        assert_transaction_match!(result, 0.1, event, 1);
    }

    #[test]
    /// Tests that match is returned with sample rate value interpolated with linear decaying function.
    fn test_get_sampling_match_result_with_linear_decaying_function() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_transaction_match!(result, 0.75, event, 1);

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now),
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_transaction_match!(result, 1.0, event, 1);

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                Some(now),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_no_match!(result);
    }

    #[test]
    /// Tests that no match is returned when the linear decaying function has invalid time range.
    fn test_get_sampling_match_result_with_linear_decaying_function_and_invalid_time_range() {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                Some(now - DateDuration::days(1)),
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_no_match!(result);

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                None,
                Some(now + DateDuration::days(1)),
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_no_match!(result);

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![mocked_decaying_sampling_rule(
                1,
                None,
                None,
                SamplingValue::SampleRate { value: 1.0 },
                DecayingFunction::Linear { decayed_value: 0.5 },
            )],
            mode: SamplingMode::Received,
        };
        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert_no_match!(result);
    }

    #[test]
    /// Tests that match is returned when there are multiple decaying rules with factor and sample rate.
    fn test_get_sampling_match_result_with_multiple_decaying_functions_with_factor_and_sample_rate()
    {
        let now = Utc::now();
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");

        let sampling_config = SamplingConfig {
            rules: vec![],
            rules_v2: vec![
                mocked_decaying_sampling_rule(
                    1,
                    Some(now - DateDuration::days(1)),
                    Some(now + DateDuration::days(1)),
                    SamplingValue::Factor { value: 5.0 },
                    DecayingFunction::Linear { decayed_value: 1.0 },
                ),
                mocked_decaying_sampling_rule(
                    2,
                    Some(now - DateDuration::days(1)),
                    Some(now + DateDuration::days(1)),
                    SamplingValue::SampleRate { value: 0.3 },
                    DecayingFunction::Constant,
                ),
            ],
            mode: SamplingMode::Received,
        };

        let result =
            merge_configs_and_match(true, Some(&sampling_config), None, None, Some(&event), now);
        assert!(result.is_some());
        if let Some(SamplingMatch {
            sample_rate,
            seed,
            matched_rule_ids,
        }) = result
        {
            assert!((sample_rate - 0.9).abs() < f64::EPSILON);
            assert_eq!(seed, event.id.0.unwrap().0);
            assert_eq!(matched_rule_ids, MatchedRuleIds(vec![RuleId(1), RuleId(2)]))
        }
    }

    #[test]
    /// Tests that match is returned when there are only dsc and root sampling config.
    fn test_get_sampling_match_result_with_no_event_and_with_dsc() {
        let now = Utc::now();

        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));
        let root_sampling_config = mocked_root_project_sampling_config(SamplingMode::Total);
        let result = merge_configs_and_match(
            true,
            None,
            Some(&root_sampling_config),
            Some(&dsc),
            None,
            now,
        );
        assert_trace_match!(result, 1.0, dsc, 6);
    }

    #[test]
    /// Tests that no match is returned when no event and no dsc are passed.
    fn test_get_sampling_match_result_with_no_event_and_no_dsc() {
        let sampling_config = mocked_sampling_config(SamplingMode::Received);
        let root_sampling_config = mocked_root_project_sampling_config(SamplingMode::Total);

        let result = merge_configs_and_match(
            true,
            Some(&sampling_config),
            Some(&root_sampling_config),
            None,
            None,
            Utc::now(),
        );
        assert_no_match!(result);
    }

    #[test]
    /// Tests that no match is returned when no sampling configs are passed.
    fn test_get_sampling_match_result_with_no_sampling_configs() {
        let event = mocked_event(EventType::Transaction, "transaction", "2.0", "");
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("1.0"), None, Some("dev"));

        let result =
            merge_configs_and_match(true, None, None, Some(&dsc), Some(&event), Utc::now());
        assert_no_match!(result);
    }
}
