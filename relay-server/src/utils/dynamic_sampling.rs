//! Functionality for calculating if a trace should be processed or dropped.
//!
use std::convert::TryInto;

use rand::{distributions::Uniform, Rng};
use rand_pcg::Pcg32;
use serde::{Deserialize, Serialize};

use relay_common::{ProjectId, ProjectKey, Uuid};
use relay_filter::GlobPatterns;

/// A sampling rule defined by user in Organization options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingRule {
    /// The project ids for which the sample rule applies, empty applies to all
    #[serde(default)]
    pub project_ids: Vec<ProjectId>,
    /// A set of Glob patterns for which the rule applies, empty applies to all
    #[serde(default)]
    pub releases: GlobPatterns,
    /// A set of user_segments for which the rule applies, empty applies to all
    #[serde(default)]
    pub user_segments: Vec<String>,
    /// the sampling rate for trace matching this rule
    pub sample_rate: f64,
}

/// Represents the dynamic sampling configuration available to a project.
/// Note: This comes from the organization data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamplingConfig {
    /// The sampling rules for the project
    pub rules: Vec<SamplingRule>,
}

/// TraceContext created by the first Sentry SDK in the call chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceContext {
    /// IID created by SDK to represent the current call flow
    /// TODO RaduW. Should we deserialize directly into an Uuid ?
    /// Also since trace_id and public_key are compulsory for tracing functionality should
    /// they be an option or not ?
    pub trace_id: Uuid,
    /// The project key
    pub public_key: ProjectKey,
    /// the release
    #[serde(default)]
    pub release: Option<String>,
    /// the user segment
    #[serde(default)]
    pub user_segment: Option<String>,
}

impl TraceContext {
    /// Returns the decision of whether to sample or not a trace based on the configuration rules
    /// If None then a decision can't be made either because of an invalid of missing trace context or
    /// because no applicable sampling rule could be found.
    pub fn should_sample(&self, config: &SamplingConfig, project_id: ProjectId) -> Option<bool> {
        let rule = get_matching_rule(config, self, project_id)?;
        let rate = pseudo_random_from_trace_id(self.trace_id)?;
        Some(rate < rule.sample_rate)
    }
}

/// Tests whether a rule matches a trace context
fn matches(rule: &SamplingRule, context: &TraceContext, project_id: ProjectId) -> bool {
    // match against the project
    if !rule.project_ids.is_empty() && !rule.project_ids.contains(&project_id) {
        return false;
    }
    // match against the release
    if !rule.releases.is_empty() {
        match context.release {
            None => return false,
            Some(ref release) => {
                if !rule.releases.is_match(release) {
                    return false;
                }
            }
        }
    }
    // match against the user_segment
    if !rule.user_segments.is_empty() {
        match context.user_segment {
            None => return false,
            Some(ref user_segment) => {
                if !rule.user_segments.contains(user_segment) {
                    return false;
                }
            }
        }
    }
    true
}

fn get_matching_rule<'a>(
    config: &'a SamplingConfig,
    context: &TraceContext,
    project_id: ProjectId,
) -> Option<&'a SamplingRule> {
    config
        .rules
        .iter()
        .find(|rule| matches(rule, context, project_id))
}

/// Generates a pseudo random number by seeding the generator with the trace_id
/// The return is deterministic, always generates the same number from the same trace_id.
/// If there's an error in parsing the trace_id into an UUID it will return None.
fn pseudo_random_from_trace_id(trace_id: Uuid) -> Option<f64> {
    let big_seed = trace_id.as_u128();
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
        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
        };
        let rules = [
            (
                "simple",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".to_string()],
                    sample_rate: 1.0,
                },
            ),
            (
                "multiple projects",
                SamplingRule {
                    project_ids: vec![project_id2, project_id, project_id3],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".to_string()],
                    sample_rate: 1.0,
                },
            ),
            (
                "all projects",
                SamplingRule {
                    project_ids: vec![],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".to_string()],
                    sample_rate: 1.0,
                },
            ),
            (
                "glob releases",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.*".to_string()]),
                    user_segments: vec!["vip".to_string()],
                    sample_rate: 1.0,
                },
            ),
            (
                "multiple releases",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["2.1.1".to_string(), "1.1.*".to_string()]),
                    user_segments: vec!["vip".to_string()],
                    sample_rate: 1.0,
                },
            ),
            (
                "multiple user segments",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["paid".to_string(), "vip".to_string(), "free".to_string()],
                    sample_rate: 1.0,
                },
            ),
            (
                "all user segments",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec![],
                    sample_rate: 1.0,
                },
            ),
            (
                "match all",
                SamplingRule {
                    project_ids: vec![],
                    releases: GlobPatterns::new(vec![]),
                    user_segments: vec![],
                    sample_rate: 1.0,
                },
            ),
        ];

        for (rule_test_name, rule) in rules.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(matches(rule, &trace_context, project_id), failure_name);
        }
    }

    #[test]
    /// test various rules that do not match
    fn test_not_matches() {
        let project_id = ProjectId::new(22);
        let project_id2 = ProjectId::new(23);
        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
        };
        let rules = [
            (
                "simple",
                SamplingRule {
                    project_ids: vec![project_id2],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".to_string()],
                    sample_rate: 1.0,
                },
            ),
            (
                "project id",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.2".to_string()]),
                    user_segments: vec!["vip".to_string()],
                    sample_rate: 1.0,
                },
            ),
            (
                "user segment",
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["all".to_string()],
                    sample_rate: 1.0,
                },
            ),
        ];

        for (rule_test_name, rule) in rules.iter() {
            let failure_name = format!("Failed on test: '{}'!!!", rule_test_name);
            assert!(!matches(rule, &trace_context, project_id), failure_name);
        }
    }

    #[test]
    fn test_partial_trace_matches() {
        let project_id = ProjectId::new(22);
        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user_segment: Some("vip".to_string()),
        };
        let rule = SamplingRule {
            project_ids: vec![project_id],
            releases: GlobPatterns::new(vec![]),
            user_segments: vec!["vip".to_string()],
            sample_rate: 1.0,
        };
        assert!(
            matches(&rule, &trace_context, project_id),
            "did not match with missing release"
        );
        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: None,
        };

        let rule = SamplingRule {
            project_ids: vec![project_id],
            releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
            user_segments: vec![],
            sample_rate: 1.0,
        };
        assert!(
            matches(&rule, &trace_context, project_id),
            "did not match with missing release"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: None,
            user_segment: None,
        };
        let rule = SamplingRule {
            project_ids: vec![project_id],
            releases: GlobPatterns::new(vec![]),
            user_segments: vec![],
            sample_rate: 1.0,
        };
        assert!(
            matches(&rule, &trace_context, project_id),
            "did not match with missing release and user segment"
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
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.1".to_string()]),
                    user_segments: vec!["vip".to_string()],
                    sample_rate: 0.1,
                },
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec!["1.1.2".to_string()]),
                    user_segments: vec![],
                    sample_rate: 0.2,
                },
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec![]),
                    user_segments: vec!["vip".to_string()],
                    sample_rate: 0.3,
                },
                SamplingRule {
                    project_ids: vec![project_id],
                    releases: GlobPatterns::new(vec![]),
                    user_segments: vec![],
                    sample_rate: 0.4,
                },
            ],
        };

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("vip".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, project_id);
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
        };

        let result = get_matching_rule(&rules, &trace_context, project_id);
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
        };

        let result = get_matching_rule(&rules, &trace_context, project_id);
        // should match the third rule because of the unknown release
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.3),
            "did not match the expected third rule"
        );

        let trace_context = TraceContext {
            trace_id: Uuid::new_v4(),
            public_key: ProjectKey::parse("abd0f232775f45feab79864e580d160b").unwrap(),
            release: Some("1.1.1".to_string()),
            user_segment: Some("all".to_string()),
        };

        let result = get_matching_rule(&rules, &trace_context, project_id);
        // should match the fourth rule because of the unknown user segment
        assert!(
            approx_eq(result.unwrap().sample_rate, 0.4),
            "did not match the expected fourth rule"
        );
    }

    #[test]
    /// Test that we can convert the full range of UUID into a pseudo random number
    fn test_trace_id_range() {
        let highest = Uuid::from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap();

        let val = pseudo_random_from_trace_id(highest);
        assert!(val.is_some());

        let lowest = Uuid::from_str("00000000-0000-0000-0000-000000000000").unwrap();
        let val = pseudo_random_from_trace_id(lowest);
        assert!(val.is_some());
    }

    #[test]
    /// Test that the we get the same sampling decision from the same trace id
    fn test_repeatable_sampling_decision() {
        let trace_id = Uuid::new_v4();

        let val1 = pseudo_random_from_trace_id(trace_id);
        let val2 = pseudo_random_from_trace_id(trace_id);

        assert!(val1.is_some());
        assert_eq!(val1, val2);
    }
}
