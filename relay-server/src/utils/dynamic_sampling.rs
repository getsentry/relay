//! Functionality for calculating if a trace should be processed or dropped.
//!
//! TODO probably move dynamic sampling it into its own crate
use std::convert::TryInto;

use rand::{distributions::Uniform, Rng};
use rand_pcg::Pcg32;
use serde::{Deserialize, Serialize};

use relay_common::{ProjectId, ProjectKey, Uuid};
use relay_filter::GlobPatterns;

//TODO move to utils::dynamic_sampling
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

    #[test]
    /// Tests that we actually do generate a random number from a UUID string
    fn generates_random_from_uuid() {}

    #[test]
    /// Test we support both hyphenated and simple formats for the trace_id
    fn test_trace_id_format() {
        let mut trace_id = "ffffffff-ffff-ffff-ffff-ffffffffffff";
        let mut val = pseudo_random_from_trace_id(trace_id);
        assert!(val.is_some());
        trace_id = "ffffffffffffffffffffffffffffffff";
        val = pseudo_random_from_trace_id(trace_id);
        assert!(val.is_some());
    }

    #[test]
    /// Test that the we get the same sampling decision from the same trace id
    fn test_repeatable_sampling_decision() {
        let trace_id = Uuid::new_v4()
            .to_hyphenated()
            .encode_upper(&mut Uuid::encode_buffer())
            .to_string();

        let val1 = pseudo_random_from_trace_id(&trace_id);
        let val2 = pseudo_random_from_trace_id(&trace_id);

        assert!(val1.is_some());
        assert_eq!(val1, val2);
    }
}
