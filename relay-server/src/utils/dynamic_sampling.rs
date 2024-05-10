//! Functionality for calculating if a trace should be processed or dropped.
use std::cmp::Ordering;
use std::collections::HashMap;
use std::ops::ControlFlow;

use chrono::Utc;
use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectKey;
use relay_event_schema::protocol::{Event, TraceContext};
use relay_sampling::config::{RuleType, SamplingConfig};
use relay_sampling::dsc::{DynamicSamplingContext, TraceUserContext};
use relay_sampling::evaluation::{SamplingEvaluator, SamplingMatch};

use crate::envelope::{Envelope, ItemType};
use crate::statsd::RelayCounters;
use once_cell::sync::Lazy;

static SUPPORTED_SDK_VERSIONS: Lazy<HashMap<&'static str, &'static str>> = Lazy::new(|| {
    let mut supported_sdk_versions = HashMap::new();

    supported_sdk_versions.insert("sentry-python", "1.7.2");
    supported_sdk_versions.insert("sentry.python.tornado", "1.7.2");
    supported_sdk_versions.insert("sentry.python.starlette", "1.7.2");
    supported_sdk_versions.insert("sentry.python.flask", "1.7.2");
    supported_sdk_versions.insert("sentry.python.fastapi", "1.7.2");
    supported_sdk_versions.insert("sentry.python.falcon", "1.7.2");
    supported_sdk_versions.insert("sentry.python.django", "1.7.2");
    supported_sdk_versions.insert("sentry.python.bottle", "1.7.2");
    supported_sdk_versions.insert("sentry.python.aws_lambda", "1.7.2");
    supported_sdk_versions.insert("sentry.python.aiohttp", "1.7.2");
    supported_sdk_versions.insert("sentry.python", "1.7.2");
    supported_sdk_versions.insert("sentry-browser", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.angular", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.astro", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.browser", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.ember", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.gatsby", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.nextjs", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.react", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.remix", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.serverless", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.svelte", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.vue", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.node", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.angular-ivy", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.sveltekit", "7.6.0");
    supported_sdk_versions.insert("sentry.javascript.bun", "7.70.0");
    supported_sdk_versions.insert("sentry-cocoa", "7.23.0");
    supported_sdk_versions.insert("sentry-objc", "7.23.0");
    supported_sdk_versions.insert("sentry-swift", "7.23.0");
    supported_sdk_versions.insert("sentry.cocoa", "7.18.0");
    supported_sdk_versions.insert("sentry.swift", "7.23.0");
    supported_sdk_versions.insert("SentrySwift", "7.23.0");
    supported_sdk_versions.insert("sentry-android", "6.5.0");
    supported_sdk_versions.insert("sentry.java.android.timber", "6.5.0");
    supported_sdk_versions.insert("sentry.java.android", "6.5.0");
    supported_sdk_versions.insert("sentry.native.android", "6.5.0");
    supported_sdk_versions.insert("sentry-react-native", "4.3.0");
    supported_sdk_versions.insert("sentry.cocoa.react-native", "4.3.0");
    supported_sdk_versions.insert("sentry.java.android.react-native", "4.3.0");
    supported_sdk_versions.insert("sentry.javascript.react-native", "4.3.0");
    supported_sdk_versions.insert("sentry.native.android.react-native", "4.3.0");
    supported_sdk_versions.insert("sentry.javascript.react-native.expo", "6.0.0");
    supported_sdk_versions.insert("sentry.javascript.react.expo", "6.0.0");
    supported_sdk_versions.insert("dart", "6.11.0");
    supported_sdk_versions.insert("dart-sentry-client", "6.11.0");
    supported_sdk_versions.insert("sentry.dart", "6.11.0");
    supported_sdk_versions.insert("sentry.dart.logging", "6.11.0");
    supported_sdk_versions.insert("sentry.cocoa.flutter", "6.11.0");
    supported_sdk_versions.insert("sentry.dart.flutter", "6.11.0");
    supported_sdk_versions.insert("sentry.java.android.flutter", "6.11.0");
    supported_sdk_versions.insert("sentry.native.android.flutter", "6.11.0");
    supported_sdk_versions.insert("sentry.dart.browser", "6.11.0");
    supported_sdk_versions.insert("sentry-php", "3.9.0");
    supported_sdk_versions.insert("sentry.php", "3.9.0");
    supported_sdk_versions.insert("sentry-laravel", "3.0.0");
    supported_sdk_versions.insert("sentry.php.laravel", "3.0.0");
    supported_sdk_versions.insert("sentry-symfony", "4.4.0");
    supported_sdk_versions.insert("sentry.php.symfony", "4.4.0");
    supported_sdk_versions.insert("Symphony.SentryClient", "4.4.0");
    supported_sdk_versions.insert("sentry-ruby", "5.5.0");
    supported_sdk_versions.insert("sentry.ruby", "5.5.0");
    supported_sdk_versions.insert("sentry.ruby.delayed_job", "5.5.0");
    supported_sdk_versions.insert("sentry.ruby.rails", "5.5.0");
    supported_sdk_versions.insert("sentry.ruby.resque", "5.5.0");
    supported_sdk_versions.insert("sentry.ruby.sidekiq", "5.5.0");
    supported_sdk_versions.insert("sentry-java", "6.5.0");
    supported_sdk_versions.insert("sentry.java", "6.5.0");
    supported_sdk_versions.insert("sentry.java.jul", "6.5.0");
    supported_sdk_versions.insert("sentry.java.log4j2", "6.5.0");
    supported_sdk_versions.insert("sentry.java.logback", "6.5.0");
    supported_sdk_versions.insert("sentry.java.spring", "6.5.0");
    supported_sdk_versions.insert("sentry.java.spring-boot", "6.5.0");
    supported_sdk_versions.insert("sentry.java.spring-boot.jakarta", "6.5.0");
    supported_sdk_versions.insert("sentry.java.spring.jakarta", "6.5.0");
    supported_sdk_versions.insert("sentry.aspnetcore", "3.22.0");
    supported_sdk_versions.insert("Sentry.AspNetCore", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.android", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.aspnet", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.aspnetcore", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.aspnetcore.grpc", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.atlasproper", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.cocoa", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.ef", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.extensions.logging", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.google-cloud-function", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.log4net", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.maui", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.nlog", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.serilog", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.xamarin", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.xamarin-forms", "3.22.0");
    supported_sdk_versions.insert("Sentry.Extensions.Logging", "3.22.0");
    supported_sdk_versions.insert("Sentry.NET", "3.22.0");
    supported_sdk_versions.insert("Sentry.UWP", "3.22.0");
    supported_sdk_versions.insert("SentryDotNet", "3.22.0");
    supported_sdk_versions.insert("SentryDotNet.AspNetCore", "3.22.0");
    supported_sdk_versions.insert("sentry.dotnet.unity", "0.24.0");
    supported_sdk_versions.insert("sentry.cocoa.unity", "0.24.0");
    supported_sdk_versions.insert("sentry.java.android.unity", "0.24.0");
    supported_sdk_versions.insert("sentry.go", "0.16.0");

    supported_sdk_versions
});

/// Represents the specification for sampling an incoming event.
#[derive(Default, Clone, Debug, PartialEq)]
pub enum SamplingResult {
    /// The event matched a sampling condition.
    Match(SamplingMatch),
    /// The event did not match a sampling condition.
    NoMatch,
    /// The event has yet to be run a dynamic sampling decision.
    #[default]
    Pending,
}

impl SamplingResult {
    /// Returns `true` if the event matched on any rules.
    #[cfg(test)]
    pub fn is_no_match(&self) -> bool {
        matches!(self, &Self::NoMatch)
    }

    /// Returns `true` if the event did not match on any rules.
    #[cfg(test)]
    pub fn is_match(&self) -> bool {
        matches!(self, &Self::Match(_))
    }

    /// Returns `true` if the event should be dropped.
    pub fn should_drop(&self) -> bool {
        !self.should_keep()
    }

    /// Returns `true` if the event should be kept.
    pub fn should_keep(&self) -> bool {
        match self {
            SamplingResult::Match(sampling_match) => sampling_match.should_keep(),
            // If no rules matched on an event, we want to keep it.
            SamplingResult::NoMatch => true,
            SamplingResult::Pending => true,
        }
    }
}

impl From<ControlFlow<SamplingMatch, SamplingEvaluator<'_>>> for SamplingResult {
    fn from(value: ControlFlow<SamplingMatch, SamplingEvaluator>) -> Self {
        match value {
            ControlFlow::Break(sampling_match) => Self::Match(sampling_match),
            ControlFlow::Continue(_) => Self::NoMatch,
        }
    }
}

/// Runs dynamic sampling if the dsc and root project state are not None and returns whether the
/// transactions received with such dsc and project state would be kept or dropped by dynamic
/// sampling.
pub fn is_trace_fully_sampled(
    root_project_config: &SamplingConfig,
    dsc: &DynamicSamplingContext,
) -> Option<bool> {
    // If the sampled field is not set, we prefer to not tag the error since we have no clue on
    // whether the head of the trace was kept or dropped on the client side.
    // In addition, if the head of the trace was dropped on the client we will immediately mark
    // the trace as not fully sampled.
    if !(dsc.sampled?) {
        return Some(false);
    }

    // TODO(tor): pass correct now timestamp
    let evaluator = SamplingEvaluator::new(Utc::now());

    let rules = root_project_config.filter_rules(RuleType::Trace);

    let evaluation = evaluator.match_rules(dsc.trace_id, dsc, rules);
    Some(SamplingResult::from(evaluation).should_keep())
}

/// Returns the project key defined in the `trace` header of the envelope.
///
/// This function returns `None` if:
///  - there is no [`DynamicSamplingContext`] in the envelope headers.
///  - there are no transactions or events in the envelope, since in this case sampling by trace is redundant.
pub fn get_sampling_key(envelope: &Envelope) -> Option<ProjectKey> {
    // If the envelope item is not of type transaction or event, we will not return a sampling key
    // because it doesn't make sense to load the root project state if we don't perform trace
    // sampling.
    envelope
        .get_item_by(|item| item.ty() == &ItemType::Transaction || item.ty() == &ItemType::Event)?;
    envelope.dsc().map(|dsc| dsc.public_key)
}

/// Compares two semantic versions.
///
/// This function is just temporary since it's used to compare SDK versions when an [`Event`] is
/// received.
fn compare_versions(version1: &str, version2: &str) -> std::cmp::Ordering {
    // Split the version strings into individual numbers
    let nums1: Vec<&str> = version1.split('.').collect();
    let nums2: Vec<&str> = version2.split('.').collect();

    // Pad the shorter version with zeros to ensure equal length
    let length = usize::max(nums1.len(), nums2.len());
    let nums1 = {
        let mut padded = vec!["0"; length - nums1.len()];
        padded.extend(nums1);
        padded
    };
    let nums2 = {
        let mut padded = vec!["0"; length - nums2.len()];
        padded.extend(nums2);
        padded
    };

    // Compare the numbers from left to right
    for (num1, num2) in nums1.iter().zip(nums2.iter()) {
        let num1 = num1.parse::<i32>().unwrap_or(0);
        let num2 = num2.parse::<i32>().unwrap_or(0);

        match num1.cmp(&num2) {
            Ordering::Greater => return Ordering::Greater,
            Ordering::Less => return Ordering::Less,
            Ordering::Equal => continue,
        }
    }

    // All numbers are equal
    Ordering::Equal
}

/// Emits a metric when an [`Event`] is inside an [`Envelope`] without [`DynamicSamplingContext`].
///
/// This function is a temporary function which has been added mainly for debugging purposes. Our
/// goal with this function is to validate how many times the DSC is not
fn track_missing_dsc(event: &Event) {
    let Some(client_sdk_info) = event.client_sdk.value() else {
        return;
    };

    let (Some(sdk_name), Some(sdk_version)) = (
        client_sdk_info.name.value(),
        client_sdk_info.version.value(),
    ) else {
        return;
    };

    let Some(min_sdk_version) = SUPPORTED_SDK_VERSIONS.get(sdk_name.as_str()) else {
        return;
    };

    match compare_versions(sdk_version, min_sdk_version) {
        Ordering::Greater | Ordering::Equal => {
            relay_statsd::metric!(
                counter(RelayCounters::MissingDynamicSamplingContext) += 1,
                sdk_name = sdk_name.as_str()
            );
        }
        _ => (),
    };
}

/// Computes a dynamic sampling context from a transaction event.
///
/// Returns `None` if the passed event is not a transaction event, or if it does not contain a
/// trace ID in its trace context. All optional fields in the dynamic sampling context are
/// populated with the corresponding attributes from the event payload if they are available.
///
/// Since sampling information is not available in the event payload, the `sample_rate` field
/// cannot be set when computing the dynamic sampling context from a transaction event.
pub fn dsc_from_event(public_key: ProjectKey, event: &Event) -> Option<DynamicSamplingContext> {
    if event.ty.value() != Some(&EventType::Transaction) {
        return None;
    }

    let trace = event.context::<TraceContext>()?;
    let trace_id = trace.trace_id.value()?.0.parse().ok()?;
    let user = event.user.value();

    // When we arrive at this point, we know that a DSC can be built from the event, implying that
    // the event doesn't have one. We want to track a metric for this.
    track_missing_dsc(event);

    Some(DynamicSamplingContext {
        trace_id,
        public_key,
        release: event.release.as_str().map(str::to_owned),
        environment: event.environment.value().cloned(),
        transaction: event.transaction.value().cloned(),
        replay_id: None,
        sample_rate: None,
        user: TraceUserContext {
            user_segment: user
                .and_then(|u| u.segment.value().cloned())
                .unwrap_or_default(),
            user_id: user
                .and_then(|u| u.id.as_str())
                .unwrap_or_default()
                .to_owned(),
        },
        sampled: None,
        other: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{EventId, LenientString};
    use relay_protocol::Annotated;
    use relay_protocol::RuleCondition;
    use relay_sampling::config::{RuleId, SamplingRule, SamplingValue};
    use uuid::Uuid;

    fn mocked_event(event_type: EventType, transaction: &str, release: &str) -> Event {
        Event {
            id: Annotated::new(EventId::new()),
            ty: Annotated::new(event_type),
            transaction: Annotated::new(transaction.to_string()),
            release: Annotated::new(LenientString(release.to_string())),
            ..Event::default()
        }
    }

    use super::*;

    fn mocked_simple_dynamic_sampling_context(
        sample_rate: Option<f64>,
        release: Option<&str>,
        transaction: Option<&str>,
        environment: Option<&str>,
        sampled: Option<bool>,
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
            sampled,
        }
    }

    fn mocked_sampling_rule(id: u32, ty: RuleType, sample_rate: f64) -> SamplingRule {
        SamplingRule {
            condition: RuleCondition::all(),
            sampling_value: SamplingValue::SampleRate { value: sample_rate },
            ty,
            id: RuleId(id),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }
    }

    #[test]
    /// Tests that an event is kept when there is a match and we have 100% sample rate.
    fn test_match_rules_return_keep_with_match_and_100_sample_rate() {
        let event = mocked_event(EventType::Transaction, "bar", "2.0");
        let rules = [mocked_sampling_rule(1, RuleType::Transaction, 1.0)];
        let seed = Uuid::default();

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(seed, &event, rules.iter())
            .into();

        assert!(result.is_match());
        assert!(result.should_keep());
    }
    #[test]
    /// Tests that an event is dropped when there is a match and we have 0% sample rate.
    fn test_match_rules_return_drop_with_match_and_0_sample_rate() {
        let event = mocked_event(EventType::Transaction, "bar", "2.0");
        let rules = [mocked_sampling_rule(1, RuleType::Transaction, 0.0)];
        let seed = Uuid::default();

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(seed, &event, rules.iter())
            .into();

        assert!(result.is_match());
        assert!(result.should_drop());
    }

    #[test]
    /// Tests that an event is kept when there is no match.
    fn test_match_rules_return_keep_with_no_match() {
        let rules = [SamplingRule {
            condition: RuleCondition::eq_ignore_case("event.transaction", "foo"),
            sampling_value: SamplingValue::SampleRate { value: 0.5 },
            ty: RuleType::Transaction,
            id: RuleId(3),
            time_range: Default::default(),
            decaying_fn: Default::default(),
        }];

        let event = mocked_event(EventType::Transaction, "bar", "2.0");
        let seed = Uuid::default();

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(seed, &event, rules.iter())
            .into();

        assert!(result.is_no_match());
        assert!(result.should_keep());
    }

    #[test]
    /// Tests that an event is kept when there is a trace match and we have 100% sample rate.
    fn test_match_rules_with_traces_rules_return_keep_when_match() {
        let rules = [mocked_sampling_rule(1, RuleType::Trace, 1.0)];
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result: SamplingResult = SamplingEvaluator::new(Utc::now())
            .match_rules(Uuid::default(), &dsc, rules.iter())
            .into();

        assert!(result.is_match());
        assert!(result.should_keep());
    }

    #[test]
    fn test_is_trace_fully_sampled_return_true_with_unsupported_rules() {
        let config = SamplingConfig {
            rules: vec![
                mocked_sampling_rule(1, RuleType::Unsupported, 1.0),
                mocked_sampling_rule(1, RuleType::Trace, 0.0),
            ],
            ..SamplingConfig::new()
        };

        let dsc = mocked_simple_dynamic_sampling_context(None, None, None, None, Some(true));

        // If processing is enabled, we simply log an error and otherwise proceed as usual.
        assert_eq!(is_trace_fully_sampled(&config, &dsc), Some(false));
    }

    #[test]
    /// Tests that a trace is marked as fully sampled correctly when dsc and project state are set.
    fn test_is_trace_fully_sampled_with_valid_dsc_and_sampling_config() {
        // We test with `sampled = true` and 100% rule.

        let config = SamplingConfig {
            rules: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            ..SamplingConfig::new()
        };

        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = is_trace_fully_sampled(&config, &dsc).unwrap();
        assert!(result);

        // We test with `sampled = true` and 0% rule.
        let config = SamplingConfig {
            rules: vec![mocked_sampling_rule(1, RuleType::Trace, 0.0)],
            ..SamplingConfig::new()
        };

        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(true));

        let result = is_trace_fully_sampled(&config, &dsc).unwrap();
        assert!(!result);

        // We test with `sampled = false` and 100% rule.
        let config = SamplingConfig {
            rules: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            ..SamplingConfig::new()
        };

        let dsc =
            mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, Some(false));

        let result = is_trace_fully_sampled(&config, &dsc).unwrap();
        assert!(!result);
    }

    #[test]
    /// Tests that a trace is not marked as fully sampled or not if inputs are invalid.
    fn test_is_trace_fully_sampled_with_invalid_inputs() {
        // We test with missing `sampled`.
        let config = SamplingConfig {
            rules: vec![mocked_sampling_rule(1, RuleType::Trace, 1.0)],
            ..SamplingConfig::new()
        };
        let dsc = mocked_simple_dynamic_sampling_context(Some(1.0), Some("3.0"), None, None, None);

        let result = is_trace_fully_sampled(&config, &dsc);
        assert!(result.is_none());
    }

    #[test]
    fn test_sdk_versions_comparison() {
        let minimum_sdk_version = SUPPORTED_SDK_VERSIONS.get("sentry.python.flask").unwrap();
        let received_sdk_versions = ["1.0.0", "0.1", "1.7.3", "1.10.20", "1.0", "1.7.2"];
        let expected_comparisons = [
            Ordering::Less,
            Ordering::Less,
            Ordering::Greater,
            Ordering::Greater,
            Ordering::Less,
            Ordering::Equal,
        ];

        for (received_sdk_version, expected_comparison) in received_sdk_versions
            .iter()
            .zip(expected_comparisons.iter())
        {
            let comparison = compare_versions(received_sdk_version, minimum_sdk_version);
            assert_eq!(comparison, *expected_comparison);
        }
    }
}
