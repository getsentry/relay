//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::convert::TryInto;
use std::fmt;

use actix::prelude::*;
use futures::{future, prelude::*};
use rand::{distributions::Uniform, Rng};
use rand_pcg::Pcg32;
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize};

use relay_common::{ProjectId, ProjectKey, Uuid};
use relay_filter::GlobPatterns;
use relay_general::protocol::{Event, EventId};

use crate::actors::project::{GetCachedProjectState, GetProjectState, Project, ProjectState};
use crate::envelope::{Envelope, ItemType};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum SamplingStrategy {
    Trace, // Rules that apply to Transaction items
    Event, // Rules that apply to Event items
}

/// A sampling rule defined by user in Organization options.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingRule {
    /// The project ids for which the sample rule applies, empty applies to all
    #[serde(default)]
    pub project_ids: Vec<ProjectId>,
    /// A set of Glob patterns for which the rule applies, empty applies to all
    #[serde(default)]
    pub releases: GlobPatterns,
    /// A set of user_segments for which the rule applies, empty applies to all
    #[serde(default)]
    pub user_segments: Vec<LowerCaseString>,
    #[serde(default)]
    pub environments: Vec<LowerCaseString>,
    /// The sampling rate for trace matching this rule
    pub sample_rate: f64,
    /// Specifies to what type of item does this rule apply
    pub strategy: SamplingStrategy,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct LowerCaseString(String);

impl LowerCaseString {
    pub fn new(val: &str) -> LowerCaseString {
        LowerCaseString(val.to_lowercase())
    }
}

impl From<&str> for LowerCaseString {
    fn from(val: &str) -> Self {
        LowerCaseString(val.to_lowercase())
    }
}

impl PartialEq for LowerCaseString {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for LowerCaseString {}

struct LowerCaseStringVisitor;

impl<'de> Visitor<'de> for LowerCaseStringVisitor {
    type Value = LowerCaseString;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string to be converted to a lowercase string ")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        Ok(LowerCaseString::new(v))
    }
}

impl<'de> Deserialize<'de> for LowerCaseString {
    fn deserialize<D>(deserializer: D) -> Result<LowerCaseString, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(LowerCaseStringVisitor)
    }
}

impl SamplingRule {
    /// Tests whether a rule matches a trace context
    fn matches(
        &self,
        release: Option<&str>,
        user_segment: &Option<LowerCaseString>,
        environment: &Option<LowerCaseString>,
        project_id: ProjectId,
        strategy: SamplingStrategy,
    ) -> bool {
        // check we are matching the right type of rule
        if self.strategy != strategy {
            return false;
        }

        // match against the environment
        if !self.environments.is_empty() {
            match environment {
                None => return false,
                Some(ref environment) => {
                    if !self.environments.contains(environment) {
                        return false;
                    }
                }
            }
        }

        // match against the project
        if !self.project_ids.is_empty() && !self.project_ids.contains(&project_id) {
            return false;
        }

        // match against the release
        if !self.releases.is_empty() {
            match release {
                None => return false,
                Some(ref release) => {
                    if !self.releases.is_match(release) {
                        return false;
                    }
                }
            }
        }

        // match against the user_segment
        if !self.user_segments.is_empty() {
            match user_segment {
                None => return false,
                Some(ref user_segment) => {
                    if !self.user_segments.contains(user_segment) {
                        return false;
                    }
                }
            }
        }
        true
    }
}

/// Represents the dynamic sampling configuration available to a project.
/// Note: This comes from the organization data
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SamplingConfig {
    /// The sampling rules for the project
    pub rules: Vec<SamplingRule>,
}

/// Represents an object that can provide the context needed to make a sampling decision.
///
/// TraceContext and Event are implementors of this trait.
trait SamplingContextProvider {
    fn release(&self) -> Option<&str>;
    fn environment(&self) -> Option<&str>;
    fn user_segment(&self) -> Option<&str>;
}

/// TraceContext created by the first Sentry SDK in the call chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    /// IID created by SDK to represent the current call flow
    pub trace_id: Uuid,
    /// The project key
    pub public_key: ProjectKey,
    /// the release
    #[serde(default)]
    pub release: Option<String>,
    /// the user segment
    #[serde(default)]
    pub user_segment: Option<String>,
    /// the environment
    #[serde(default)]
    pub environment: Option<String>,
}

impl TraceContext {
    /// Returns the decision of whether to sample or not a trace based on the configuration rules
    /// If None then a decision can't be made either because of an invalid of missing trace context or
    /// because no applicable sampling rule could be found.
    fn should_sample(
        &self,
        config: &SamplingConfig,
        project_id: ProjectId,
        strategy: SamplingStrategy,
    ) -> Option<bool> {
        let rule = get_matching_rule(config, self, project_id, strategy)?;
        let rate = pseudo_random_from_uuid(self.trace_id)?;
        Some(rate < rule.sample_rate)
    }
}

impl SamplingContextProvider for TraceContext {
    fn release(&self) -> Option<&str> {
        self.release.as_deref()
    }

    fn environment(&self) -> Option<&str> {
        self.environment.as_deref()
    }

    fn user_segment(&self) -> Option<&str> {
        self.user_segment.as_deref()
    }
}

/// NOTE: since relay-general doesn't know anything about dynamic sampling
/// SamplingContextProvider for Event is implemented here.
impl SamplingContextProvider for Event {
    fn release(&self) -> Option<&str> {
        self.release.as_str()
    }

    fn environment(&self) -> Option<&str> {
        self.environment.as_str()
    }

    fn user_segment(&self) -> Option<&str> {
        None // TODO RaduW at the moment (10.12.2020) we don't have this (discussions pending)
    }
}

// Checks whether an event should be kept or removed by dynamic sampling
pub fn should_keep_event(
    event: &Event,
    project_state: &ProjectState,
    project_id: ProjectId,
) -> Option<bool> {
    let sampling_config = match &project_state.config.sampling {
        None => return None, // without config there is not enough info to make up my mind
        Some(config) => config,
    };

    let event_id = match event.id.0 {
        None => return None, // if no eventID we can't really sample so keep everything
        Some(EventId(id)) => id,
    };

    if let Some(rule) =
        get_matching_rule(sampling_config, event, project_id, SamplingStrategy::Event)
    {
        if let Some(random_number) = pseudo_random_from_uuid(event_id) {
            return Some(rule.sample_rate > random_number);
        }
    }
    None // if no matching rule there is not enough info to make a decision
}

/// Takes an envelope and potentially removes the transaction item from it if that
/// transaction item should be sampled out according to the dynamic sampling configuration
/// and the trace context.
fn sample_transaction_internal(
    mut envelope: Envelope,
    project_state: Option<&ProjectState>,
) -> Envelope {
    let project_state = match project_state {
        None => return envelope,
        Some(project_state) => project_state,
    };

    let sampling_config = match project_state.config.sampling {
        // without sampling config we cannot sample transactions so give up here
        None => return envelope,
        Some(ref sampling_config) => sampling_config,
    };

    let project_id = match project_state.project_id {
        None => return envelope,
        Some(project_id) => project_id,
    };

    let trace_context = envelope.trace_context();
    let transaction_item = envelope.get_item_by(|item| item.ty() == ItemType::Transaction);

    let trace_context = match (trace_context, transaction_item) {
        // we don't have what we need, can't sample the transactions in this envelope
        (None, _) | (_, None) => return envelope,
        // see if we need to sample the transaction
        (Some(trace_context), Some(_)) => trace_context,
    };

    let should_sample = trace_context
        // see if we should sample
        .should_sample(sampling_config, project_id, SamplingStrategy::Trace)
        // TODO verify that this is the desired behaviour (i.e. if we can't find a rule
        // for sampling, include the transaction)
        .unwrap_or(true);

    if !should_sample {
        // finally we decided that we should sample the transaction
        envelope.take_item_by(|item| item.ty() == ItemType::Transaction);
    }

    envelope
}

/// Check if we should remove transactions from this envelope (because of trace sampling) and
/// return what is left of the envelope
pub fn sample_transaction(
    envelope: Envelope,
    project: Option<Addr<Project>>,
    fast_processing: bool,
) -> ResponseFuture<Envelope, ()> {
    let project = match project {
        None => return Box::new(future::ok(envelope)),
        Some(project) => project,
    };

    let trace_context = envelope.trace_context();
    let transaction_item = envelope.get_item_by(|item| item.ty() == ItemType::Transaction);

    // if there is no trace context or there are no transactions to sample return here
    if trace_context.is_none() || transaction_item.is_none() {
        return Box::new(future::ok(envelope));
    }
    //we have a trace_context and we have a transaction_item see if we can sample them
    if fast_processing {
        let fut = project.send(GetCachedProjectState).then(|project_state| {
            let project_state = match project_state {
                // error getting the project, give up and return envelope unchanged
                Err(_) => return Ok(envelope),
                Ok(project_state) => project_state,
            };
            Ok(sample_transaction_internal(
                envelope,
                project_state.as_deref(),
            ))
        });
        Box::new(fut) as ResponseFuture<_, _>
    } else {
        let fut = project.send(GetProjectState).then(|project_state| {
            let project_state = match project_state {
                // error getting the project, give up and return envelope unchanged
                Err(_) => return Ok(envelope),
                Ok(project_state) => project_state,
            };
            Ok(sample_transaction_internal(
                envelope,
                project_state.ok().as_deref(),
            ))
        });
        Box::new(fut) as ResponseFuture<_, _>
    }
}

fn get_matching_rule<'a, T>(
    config: &'a SamplingConfig,
    context: &T,
    project_id: ProjectId,
    strategy: SamplingStrategy,
) -> Option<&'a SamplingRule>
where
    T: SamplingContextProvider,
{
    let user_segment = context.user_segment().map(LowerCaseString::new);
    let environment = context.environment().as_deref().map(LowerCaseString::new);
    let release = context.release();

    config
        .rules
        .iter()
        .find(|rule| rule.matches(release, &user_segment, &environment, project_id, strategy))
}

/// Generates a pseudo random number by seeding the generator with the given id.
/// The return is deterministic, always generates the same number from the same id.
/// If there's an error in parsing the id into an UUID it will return None.
fn pseudo_random_from_uuid(id: Uuid) -> Option<f64> {
    let big_seed = id.as_u128();
    let seed: u64 = big_seed.overflowing_shr(64).0.try_into().ok()?;
    let stream: u64 = (big_seed & 0xffffffff00000000).try_into().ok()?;
    let mut generator = Pcg32::new(seed, stream);
    let dist = Uniform::new(0f64, 1f64);
    Some(generator.sample(dist))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    /// test matching for various rules
    fn test_matches() {
        let project_id = ProjectId::new(22);
        let project_id2 = ProjectId::new(23);
        let project_id3 = ProjectId::new(24);
        let release = Some("1.1.1".to_string());
        let user_segment = Some(LowerCaseString::new("vip"));
        let environment = Some(LowerCaseString::new("debug"));
        let rules = [
            (
                "simple",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "multiple projects",
                SamplingRule {
                    project_ids: vec![project_id2, project_id, project_id3],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "all projects",
                SamplingRule {
                    project_ids: vec![],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "glob releases",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.*".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "multiple releases",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["2.1.1".to_string(), "1.1.*".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "multiple user segments",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["paid".into(), "vip".into(), "free".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "case insensitive user segments",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["ViP".into(), "FrEe".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "multiple user environments",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["integration".into(), "debug".into(), "production".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "case insensitive environments",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["DeBuG".into(), "PrOd".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "all user environments",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec![],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "match all",
                SamplingRule {
                    project_ids: vec![],
                    releases: GlobPatterns::new(vec![]),
                    user_segments: vec![],
                    environments: vec![],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
        ];

        for (rule_test_name, rule) in rules.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(
                rule.matches(
                    release.as_deref(),
                    &user_segment,
                    &environment,
                    project_id,
                    SamplingStrategy::Trace
                ),
                failure_name
            );
        }
    }

    #[test]
    /// test various rules that do not match
    fn test_not_matches() {
        let project_id = ProjectId::new(22);
        let project_id2 = ProjectId::new(23);
        let release = Some("1.1.1".to_string());
        let user_segment = Some(LowerCaseString::new("vip"));
        let environment = Some(LowerCaseString::new("debug"));
        let rules = [
            (
                "project id",
                SamplingRule {
                    project_ids: vec![project_id2],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "release",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.2".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "user segment",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["all".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "user environment",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["prod".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Trace,
                },
            ),
            (
                "category",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 1.0,
                    strategy: SamplingStrategy::Event,
                },
            ),
        ];

        for (rule_test_name, rule) in rules.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(
                !rule.matches(
                    release.as_deref(),
                    &user_segment,
                    &environment,
                    project_id,
                    SamplingStrategy::Trace
                ),
                failure_name
            );
        }
    }

    #[test]
    ///Test SamplingRule deserialization
    fn test_sampling_rule_deserialization() {
        let serialized_rule = r#"{
            "projectIds": [1,2],
            "sampleRate": 0.7,
            "releases": ["1.1.1", "1.1.2"],
            "userSegments": ["FirstSegment", "SeCoNd"],
            "environments": ["DeV", "pRoD"],
            "strategy": "trace"
        }"#;
        let rule: Result<SamplingRule, _> = serde_json::from_str(serialized_rule);

        assert!(rule.is_ok());
        let rule = rule.unwrap();
        assert_eq!(rule.project_ids, [ProjectId::new(1), ProjectId::new(2)]);
        assert!(approx_eq(rule.sample_rate, 0.7f64));
        assert_eq!(
            rule.environments,
            [LowerCaseString::new("dev"), LowerCaseString::new("prod")]
        );
        assert_eq!(
            rule.user_segments,
            [
                LowerCaseString::new("firstsegment"),
                LowerCaseString::new("second")
            ]
        );
        assert_eq!(rule.strategy, SamplingStrategy::Trace);
    }

    #[test]
    /// Test LowerCaseString deserialization
    fn test_sampling_lower_case_string_deserialization() {
        let some_string = r#""FiRsTSEGment""#;

        let lower_case: Result<LowerCaseString, _> = serde_json::from_str(some_string);

        assert!(lower_case.is_ok());
        let lower_case = lower_case.unwrap();
        assert_eq!(lower_case, LowerCaseString::new("firstsegment"));
    }

    #[test]
    fn test_partial_trace_matches() {
        let project_id = ProjectId::new(22);

        let release = Option::<String>::None;
        let user_segment = Some(LowerCaseString::new("vip"));
        let environment = Some(LowerCaseString::new("debug"));

        let rule = SamplingRule {
            project_ids: vec![project_id],
            releases: GlobPatterns::new(vec![]),
            user_segments: vec!["vip".into()],
            environments: vec!["debug".into()],
            sample_rate: 1.0,
            strategy: SamplingStrategy::Trace,
        };
        assert!(
            rule.matches(
                release.as_deref(),
                &user_segment,
                &environment,
                project_id,
                SamplingStrategy::Trace
            ),
            "did not match with missing release"
        );

        let release = Some("1.1.1".to_string());
        let user_segment = Option::<LowerCaseString>::None;
        let environment = Some(LowerCaseString::new("debug"));

        let rule = SamplingRule {
            project_ids: vec![project_id],
            releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
            user_segments: vec![],
            environments: vec!["debug".into()],
            sample_rate: 1.0,
            strategy: SamplingStrategy::Trace,
        };
        assert!(
            rule.matches(
                release.as_deref(),
                &user_segment,
                &environment,
                project_id,
                SamplingStrategy::Trace
            ),
            "did not match with missing user segment"
        );

        let release = Some("1.1.1".to_string());
        let user_segment = Some(LowerCaseString::new("vip"));
        let environment = Option::<LowerCaseString>::None;

        let rule = SamplingRule {
            project_ids: vec![project_id],
            releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
            user_segments: vec!["vip".into()],
            environments: vec![],
            sample_rate: 1.0,
            strategy: SamplingStrategy::Trace,
        };
        assert!(
            rule.matches(
                release.as_deref(),
                &user_segment,
                &environment,
                project_id,
                SamplingStrategy::Trace
            ),
            "did not match with missing environment"
        );

        let release = Option::<String>::None;
        let user_segment = Option::<LowerCaseString>::None;
        let environment = Option::<LowerCaseString>::None;

        let rule = SamplingRule {
            project_ids: vec![project_id],
            releases: GlobPatterns::new(vec![]),
            user_segments: vec![],
            environments: vec![],
            sample_rate: 1.0,
            strategy: SamplingStrategy::Trace,
        };
        assert!(
            rule.matches(
                release.as_deref(),
                &user_segment,
                &environment,
                project_id,
                SamplingStrategy::Trace
            ),
            "did not match with missing release, user segment and environment"
        );
    }

    fn approx_eq(left: f64, right: f64) -> bool {
        let diff = left - right;
        diff < 0.001 && diff > -0.001
    }

    #[test]
    /// test that the first rule that mathces is selected
    fn test_rule_precedence() {
        let project_id = ProjectId::new(22);

        let rules = SamplingConfig {
            rules: vec![
                //everything specified
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 0.1,
                    strategy: SamplingStrategy::Trace,
                },
                // no user segments
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.2".to_string()]),
                    user_segments: vec![],
                    environments: vec!["debug".into()],
                    sample_rate: 0.2,
                    strategy: SamplingStrategy::Trace,
                },
                // no releases
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec![]),
                    user_segments: vec!["vip".into()],
                    environments: vec!["debug".into()],
                    sample_rate: 0.3,
                    strategy: SamplingStrategy::Trace,
                },
                // no environments
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".into()],
                    environments: vec![],
                    sample_rate: 0.4,
                    strategy: SamplingStrategy::Trace,
                },
                // no user segments releases or environments
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec![]),
                    user_segments: vec![],
                    environments: vec![],
                    sample_rate: 0.5,
                    strategy: SamplingStrategy::Trace,
                },
            ],
        };

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, project_id, SamplingStrategy::Trace);
        // complete match with first rule
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.1),
            "did not match the expected first rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.2".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, project_id, SamplingStrategy::Trace);
        // should mach the second rule because of the release
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.2),
            "did not match the expected second rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.3".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, project_id, SamplingStrategy::Trace);
        // should match the third rule because of the unknown release
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.3),
            "did not match the expected third rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
            environment: Some("production".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, project_id, SamplingStrategy::Trace);
        // should match the fourth rule because of the unknown environment
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.4),
            "did not match the expected fourth rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("all".to_string()),
            environment: Some("debug".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, project_id, SamplingStrategy::Trace);
        // should match the fourth rule because of the unknown user segment
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.5),
            "did not match the expected fourth rule"
        );
    }

    #[test]
    /// Test that we can convert the full range of UUID into a pseudo random number
    fn test_id_range() {
        let highest = Uuid::from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();

        let val = pseudo_random_from_uuid(highest);
        assert!(val.is_some());

        let lowest = Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap();
        let val = pseudo_random_from_uuid(lowest);
        assert!(val.is_some());
    }

    #[test]
    /// Test that the we get the same sampling decision from the same trace id
    fn test_repeatable_sampling_decision() {
        let id = Uuid::new_v4();

        let val1 = pseudo_random_from_uuid(id);
        let val2 = pseudo_random_from_uuid(id);

        assert!(val1.is_some());
        assert_eq!(val1, val2);
    }
}
