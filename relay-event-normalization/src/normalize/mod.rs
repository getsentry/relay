use std::hash::Hash;
use std::sync::Arc;

use itertools::Itertools;
use relay_base_schema::metrics::{MetricResourceIdentifier, MetricUnit};
use relay_event_schema::processor::{
    ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::{
    ClientSdkInfo, Event, EventId, EventType, Level, MetricSummaryMapping, NelContext,
    ReplayContext, TraceContext, VALID_PLATFORMS,
};
use relay_protocol::{
    Annotated, Empty, Error, FromValue, IntoValue, Meta, Object, Remark, RemarkType, RuleCondition,
    Value,
};
use serde::{Deserialize, Serialize};

use crate::{MaxChars, StoreConfig};

pub mod breakdowns;
pub mod contexts;
pub mod nel;
pub mod request;
pub mod span;
pub mod user_agent;
pub mod utils;

/// Defines a builtin measurement.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct BuiltinMeasurementKey {
    name: String,
    unit: MetricUnit,
    #[serde(skip_serializing_if = "is_false")]
    allow_negative: bool,
}

fn is_false(b: &bool) -> bool {
    !b
}

impl BuiltinMeasurementKey {
    /// Creates a new [`BuiltinMeasurementKey`].
    pub fn new(name: impl Into<String>, unit: MetricUnit) -> Self {
        Self {
            name: name.into(),
            unit,
            allow_negative: false,
        }
    }

    /// Returns the name of the built in measurement key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the unit of the built in measurement key.
    pub fn unit(&self) -> &MetricUnit {
        &self.unit
    }

    /// Return true if the built in measurement key allows negative values.
    pub fn allow_negative(&self) -> &bool {
        &self.allow_negative
    }
}

/// Configuration for measurements normalization.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Hash)]
#[serde(default, rename_all = "camelCase")]
pub struct MeasurementsConfig {
    /// A list of measurements that are built-in and are not subject to custom measurement limits.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub builtin_measurements: Vec<BuiltinMeasurementKey>,

    /// The maximum number of measurements allowed per event that are not known measurements.
    pub max_custom_measurements: usize,
}

impl MeasurementsConfig {
    /// The length of a full measurement MRI, minus the name and the unit. This length is the same
    /// for every measurement-mri.
    pub const MEASUREMENT_MRI_OVERHEAD: usize = 29;
}

/// Returns `true` if the given platform string is a known platform identifier.
///
/// See [`VALID_PLATFORMS`] for a list of all known platforms.
pub fn is_valid_platform(platform: &str) -> bool {
    VALID_PLATFORMS.contains(&platform)
}

/// The processor that normalizes events for store.
pub struct StoreNormalizeProcessor {
    config: Arc<StoreConfig>,
}

impl StoreNormalizeProcessor {
    /// Creates a new normalization processor.
    pub fn new(config: Arc<StoreConfig>) -> Self {
        StoreNormalizeProcessor { config }
    }

    /// Returns the SDK info from the config.
    fn get_sdk_info(&self) -> Option<ClientSdkInfo> {
        self.config.client.as_ref().and_then(|client| {
            client
                .splitn(2, '/')
                .collect_tuple()
                .or_else(|| client.splitn(2, ' ').collect_tuple())
                .map(|(name, version)| ClientSdkInfo {
                    name: Annotated::new(name.to_owned()),
                    version: Annotated::new(version.to_owned()),
                    ..Default::default()
                })
        })
    }

    fn normalize_metrics_summaries(&self, event: &mut Event) {
        if event.ty.value() == Some(&EventType::Transaction) {
            normalize_all_metrics_summaries(event);
        }
    }

    fn normalize_trace_context(&self, event: &mut Event) {
        if let Some(context) = event.context_mut::<TraceContext>() {
            context.client_sample_rate = Annotated::from(self.config.client_sample_rate);
        }
    }

    fn normalize_replay_context(&self, event: &mut Event) {
        if let Some(ref mut contexts) = event.contexts.value_mut() {
            if let Some(replay_id) = self.config.replay_id {
                contexts.add(ReplayContext {
                    replay_id: Annotated::new(EventId(replay_id)),
                    other: Object::default(),
                });
            }
        }
    }

    /// Infers the `EventType` from the event's interfaces.
    fn infer_event_type(&self, event: &Event) -> EventType {
        // The event type may be set explicitly when constructing the event items from specific
        // items. This is DEPRECATED, and each distinct event type may get its own base class. For
        // the time being, this is only implemented for transactions, so be specific:
        if event.ty.value() == Some(&EventType::Transaction) {
            return EventType::Transaction;
        }
        if event.ty.value() == Some(&EventType::UserReportV2) {
            return EventType::UserReportV2;
        }

        // The SDKs do not describe event types, and we must infer them from available attributes.
        let has_exceptions = event
            .exceptions
            .value()
            .and_then(|exceptions| exceptions.values.value())
            .filter(|values| !values.is_empty())
            .is_some();

        if has_exceptions {
            EventType::Error
        } else if event.csp.value().is_some() {
            EventType::Csp
        } else if event.hpkp.value().is_some() {
            EventType::Hpkp
        } else if event.expectct.value().is_some() {
            EventType::ExpectCt
        } else if event.expectstaple.value().is_some() {
            EventType::ExpectStaple
        } else if event.context::<NelContext>().is_some() {
            EventType::Nel
        } else {
            EventType::Default
        }
    }
}

/// Replaces snake_case app start spans op with dot.case op.
///
/// This is done for the affected React Native SDK versions (from 3 to 4.4).
pub fn normalize_app_start_spans(event: &mut Event) {
    if !event.sdk_name().eq("sentry.javascript.react-native")
        || !(event.sdk_version().starts_with("4.4")
            || event.sdk_version().starts_with("4.3")
            || event.sdk_version().starts_with("4.2")
            || event.sdk_version().starts_with("4.1")
            || event.sdk_version().starts_with("4.0")
            || event.sdk_version().starts_with('3'))
    {
        return;
    }

    if let Some(spans) = event.spans.value_mut() {
        for span in spans {
            if let Some(span) = span.value_mut() {
                if let Some(op) = span.op.value() {
                    if op == "app_start_cold" {
                        span.op.set_value(Some("app.start.cold".to_string()));
                        break;
                    } else if op == "app_start_warm" {
                        span.op.set_value(Some("app.start.warm".to_string()));
                        break;
                    }
                }
            }
        }
    }
}

/// Replaces all incoming metric identifiers in the metric summary with the correct MRI.
///
/// The reasoning behind this normalization, is that the SDK sends namespace-agnostic metric
/// identifiers in the form `metric_type:metric_name@metric_unit` and those identifiers need to be
/// converted to MRIs in the form `metric_type:metric_namespace/metric_name@metric_unit`.
fn normalize_metrics_summary_mris(m: MetricSummaryMapping) -> MetricSummaryMapping {
    m.into_iter()
        .map(|(key, value)| match MetricResourceIdentifier::parse(&key) {
            Ok(mri) => (mri.to_string(), value),
            Err(err) => (
                key,
                Annotated::from_error(Error::invalid(err), value.0.map(IntoValue::into_value)),
            ),
        })
        .collect()
}

/// Normalizes all the metrics summaries across the event payload.
fn normalize_all_metrics_summaries(event: &mut Event) {
    if let Some(metrics_summary) = event._metrics_summary.value_mut().as_mut() {
        metrics_summary.update_value(normalize_metrics_summary_mris);
    }

    let Some(spans) = event.spans.value_mut() else {
        return;
    };

    for span in spans.iter_mut() {
        let metrics_summary = span
            .value_mut()
            .as_mut()
            .and_then(|span| span._metrics_summary.value_mut().as_mut());

        if let Some(ms) = metrics_summary {
            ms.update_value(normalize_metrics_summary_mris);
        }
    }
}

/// Container for global and project level [`MeasurementsConfig`]. The purpose is to handle
/// the merging logic.
#[derive(Clone, Debug)]
pub struct DynamicMeasurementsConfig<'a> {
    project: Option<&'a MeasurementsConfig>,
    global: Option<&'a MeasurementsConfig>,
}

impl<'a> DynamicMeasurementsConfig<'a> {
    /// Constructor for [`DynamicMeasurementsConfig`].
    pub fn new(
        project: Option<&'a MeasurementsConfig>,
        global: Option<&'a MeasurementsConfig>,
    ) -> Self {
        DynamicMeasurementsConfig { project, global }
    }

    /// Returns an iterator over the merged builtin measurement keys.
    ///
    /// Items from the project config are prioritized over global config, and
    /// there are no duplicates.
    pub fn builtin_measurement_keys(
        &'a self,
    ) -> impl Iterator<Item = &'a BuiltinMeasurementKey> + '_ {
        let project = self
            .project
            .map(|p| p.builtin_measurements.as_slice())
            .unwrap_or_default();

        let global = self
            .global
            .map(|g| g.builtin_measurements.as_slice())
            .unwrap_or_default();

        project
            .iter()
            .chain(global.iter().filter(|key| !project.contains(key)))
    }

    /// Gets the max custom measurements value from the [`MeasurementsConfig`] from project level or
    /// global level. If both of them are available, it will choose the most restrictive.
    pub fn max_custom_measurements(&'a self) -> Option<usize> {
        match (&self.project, &self.global) {
            (None, None) => None,
            (None, Some(global)) => Some(global.max_custom_measurements),
            (Some(project), None) => Some(project.max_custom_measurements),
            (Some(project), Some(global)) => Some(std::cmp::min(
                project.max_custom_measurements,
                global.max_custom_measurements,
            )),
        }
    }
}

/// Defines a weighted component for a performance score.
///
/// Weight is the % of score it can take up (eg. LCP is a max of 35% weight for desktops)
/// Currently also contains (p10, p50) which are used for log CDF normalization of the weight score
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PerformanceScoreWeightedComponent {
    /// Measurement (eg. measurements.lcp) to be matched against. If this measurement is missing the entire
    /// profile will be discarded.
    pub measurement: String,
    /// Weight [0,1.0] of this component in the performance score
    pub weight: f64,
    /// p10 used to define the log-normal for calculation
    pub p10: f64,
    /// Median used to define the log-normal for calculation
    pub p50: f64,
    /// Whether the measurement is optional. If the measurement is missing, performance score processing
    /// may still continue, and the weight will be 0.
    #[serde(default)]
    pub optional: bool,
}

/// Defines a profile for performance score.
///
/// A profile contains weights for a score of 100% and match against an event using a condition.
/// eg. Desktop vs. Mobile(web) profiles for better web vital score calculation.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PerformanceScoreProfile {
    /// Name of the profile, used for debugging and faceting multiple profiles
    pub name: Option<String>,
    /// Score components
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub score_components: Vec<PerformanceScoreWeightedComponent>,
    /// See [`RuleCondition`] for all available options to specify and combine conditions.
    pub condition: Option<RuleCondition>,
    /// The version of the profile, used to isolate changes to score calculations.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Defines the performance configuration for the project.
///
/// Includes profiles matching different behaviour (desktop / mobile) and weights matching those
/// specific conditions.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PerformanceScoreConfig {
    /// List of performance profiles, only the first with matching conditions will be applied.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub profiles: Vec<PerformanceScoreProfile>,
}

impl Processor for StoreNormalizeProcessor {
    fn process_event(
        &mut self,
        event: &mut Event,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        event.process_child_values(self, state)?;

        // Override internal attributes, even if they were set in the payload
        let event_type = self.infer_event_type(event);
        event.ty = Annotated::from(event_type);
        event.project = Annotated::from(self.config.project_id);
        event.key_id = Annotated::from(self.config.key_id.clone());
        event.version = Annotated::from(self.config.protocol_version.clone());
        event.grouping_config = self
            .config
            .grouping_config
            .clone()
            .map_or(Annotated::empty(), |x| {
                FromValue::from_value(Annotated::<Value>::from(x))
            });

        // Validate basic attributes
        relay_event_schema::processor::apply(&mut event.platform, |platform, _| {
            if is_valid_platform(platform) {
                Ok(())
            } else {
                Err(ProcessingAction::DeleteValueSoft)
            }
        })?;

        // Default required attributes, even if they have errors
        event.errors.get_or_insert_with(Vec::new);
        event.id.get_or_insert_with(EventId::new);
        event.platform.get_or_insert_with(|| "other".to_string());
        event.logger.get_or_insert_with(String::new);
        event.extra.get_or_insert_with(Object::new);
        event.level.get_or_insert_with(|| match event_type {
            EventType::Transaction => Level::Info,
            _ => Level::Error,
        });
        if event.client_sdk.value().is_none() {
            event.client_sdk.set_value(self.get_sdk_info());
        }

        if event.platform.as_str() == Some("java") {
            if let Some(event_logger) = event.logger.value_mut().take() {
                let shortened = shorten_logger(event_logger, meta);
                event.logger.set_value(Some(shortened));
            }
        }

        // Normalize connected attributes and interfaces
        self.normalize_metrics_summaries(event);
        self.normalize_trace_context(event);
        self.normalize_replay_context(event);

        Ok(())
    }
}

/// If the logger is longer than [`MaxChars::Logger`], it returns a String with
/// a shortened version of the logger. If not, the same logger is returned as a
/// String. The resulting logger is always trimmed.
///
/// To shorten the logger, all extra chars that don't fit into the maximum limit
/// are removed, from the beginning of the logger.  Then, if the remaining
/// substring contains a `.` somewhere but in the end, all chars until `.`
/// (exclusive) are removed.
///
/// Additionally, the new logger is prefixed with `*`, to indicate it was
/// shortened.
fn shorten_logger(logger: String, meta: &mut Meta) -> String {
    let original_len = bytecount::num_chars(logger.as_bytes());
    let trimmed = logger.trim();
    let logger_len = bytecount::num_chars(trimmed.as_bytes());
    if logger_len <= MaxChars::Logger.limit() {
        if trimmed == logger {
            return logger;
        } else {
            if trimmed.is_empty() {
                meta.add_remark(Remark {
                    ty: RemarkType::Removed,
                    rule_id: "@logger:remove".to_owned(),
                    range: Some((0, original_len)),
                });
            } else {
                meta.add_remark(Remark {
                    ty: RemarkType::Substituted,
                    rule_id: "@logger:trim".to_owned(),
                    range: None,
                });
            }
            meta.set_original_length(Some(original_len));
            return trimmed.to_string();
        };
    }

    let mut tokens = trimmed.split("").collect_vec();
    // Remove empty str tokens from the beginning and end.
    tokens.pop();
    tokens.reverse(); // Prioritize chars from the end of the string.
    tokens.pop();

    let word_cut = remove_logger_extra_chars(&mut tokens);
    if word_cut {
        remove_logger_word(&mut tokens);
    }

    tokens.reverse();
    meta.add_remark(Remark {
        ty: RemarkType::Substituted,
        rule_id: "@logger:replace".to_owned(),
        range: Some((0, logger_len - tokens.len())),
    });
    meta.set_original_length(Some(original_len));

    format!("*{}", tokens.join(""))
}

/// Remove as many tokens as needed to match the maximum char limit defined in
/// [`MaxChars::Logger`], and an extra token for the logger prefix. Returns
/// whether a word has been cut.
///
/// A word is considered any non-empty substring that doesn't contain a `.`.
fn remove_logger_extra_chars(tokens: &mut Vec<&str>) -> bool {
    // Leave one slot of space for the prefix
    let mut remove_chars = tokens.len() - MaxChars::Logger.limit() + 1;
    let mut word_cut = false;
    while remove_chars > 0 {
        if let Some(c) = tokens.pop() {
            if !word_cut && c != "." {
                word_cut = true;
            } else if word_cut && c == "." {
                word_cut = false;
            }
        }
        remove_chars -= 1;
    }
    word_cut
}

/// If the `.` token is present, removes all tokens from the end of the vector
/// until `.`. If it isn't present, nothing is removed.
fn remove_logger_word(tokens: &mut Vec<&str>) {
    let mut delimiter_found = false;
    for token in tokens.iter() {
        if *token == "." {
            delimiter_found = true;
            break;
        }
    }
    if !delimiter_found {
        return;
    }
    while let Some(i) = tokens.last() {
        if *i == "." {
            break;
        }
        tokens.pop();
    }
}
#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use insta::{assert_debug_snapshot, assert_json_snapshot};
    use relay_base_schema::metrics::DurationUnit;
    use relay_base_schema::spans::SpanStatus;
    use relay_event_schema::processor::process_value;
    use relay_event_schema::protocol::{
        Context, ContextInner, Contexts, DebugImage, DebugMeta, Exception, Frame, Geo, IpAddr,
        LenientString, LogEntry, MetricSummary, MetricsSummary, PairList, RawStacktrace, Request,
        Span, SpanId, Stacktrace, TagEntry, Tags, TraceId, User, Values,
    };
    use relay_protocol::{
        assert_annotated_snapshot, get_path, get_value, ErrorKind, SerializableAnnotated,
    };
    use serde_json::json;
    use similar_asserts::assert_eq;
    use std::collections::BTreeMap;
    use uuid::Uuid;

    use crate::stacktrace::normalize_non_raw_frame;
    use crate::{
        normalize_event, validate_event_timestamps, validate_transaction, EventValidationConfig,
        GeoIpLookup, NormalizationConfig, TransactionValidationConfig,
    };

    use super::*;

    impl Default for StoreNormalizeProcessor {
        fn default() -> Self {
            StoreNormalizeProcessor::new(Arc::new(StoreConfig::default()))
        }
    }

    #[test]
    fn test_merge_builtin_measurement_keys() {
        let foo = BuiltinMeasurementKey::new("foo", MetricUnit::Duration(DurationUnit::Hour));
        let bar = BuiltinMeasurementKey::new("bar", MetricUnit::Duration(DurationUnit::Day));
        let baz = BuiltinMeasurementKey::new("baz", MetricUnit::Duration(DurationUnit::Week));

        let proj = MeasurementsConfig {
            builtin_measurements: vec![foo.clone(), bar.clone()],
            max_custom_measurements: 4,
        };

        let glob = MeasurementsConfig {
            // The 'bar' here will be ignored since it's a duplicate from the project level.
            builtin_measurements: vec![baz.clone(), bar.clone()],
            max_custom_measurements: 4,
        };
        let dynamic_config = DynamicMeasurementsConfig::new(Some(&proj), Some(&glob));

        let keys = dynamic_config.builtin_measurement_keys().collect_vec();

        assert_eq!(keys, vec![&foo, &bar, &baz]);
    }

    #[test]
    fn test_max_custom_measurement() {
        // Empty configs will return a None value for max measurements.
        let dynamic_config = DynamicMeasurementsConfig::new(None, None);
        assert!(dynamic_config.max_custom_measurements().is_none());

        let proj = MeasurementsConfig {
            builtin_measurements: vec![],
            max_custom_measurements: 3,
        };

        let glob = MeasurementsConfig {
            builtin_measurements: vec![],
            max_custom_measurements: 4,
        };

        // If only project level measurement config is there, return its max custom measurement variable.
        let dynamic_config = DynamicMeasurementsConfig::new(Some(&proj), None);
        assert_eq!(dynamic_config.max_custom_measurements().unwrap(), 3);

        // Same logic for when only global level measurement config exists.
        let dynamic_config = DynamicMeasurementsConfig::new(None, Some(&glob));
        assert_eq!(dynamic_config.max_custom_measurements().unwrap(), 4);

        // If both is available, pick the smallest number.
        let dynamic_config = DynamicMeasurementsConfig::new(Some(&proj), Some(&glob));
        assert_eq!(dynamic_config.max_custom_measurements().unwrap(), 3);
    }

    #[test]
    fn test_geo_from_ip_address() {
        let lookup = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();

        let json = r#"{
            "user": {
                "ip_address": "2.125.160.216"
            }
        }"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                geoip_lookup: Some(&lookup),
                ..Default::default()
            },
        );

        let expected = Annotated::new(Geo {
            country_code: Annotated::new("GB".to_string()),
            city: Annotated::new("Boxford".to_string()),
            subdivision: Annotated::new("England".to_string()),
            region: Annotated::new("United Kingdom".to_string()),
            ..Geo::default()
        });
        assert_eq!(get_value!(event.user!).geo, expected);
    }

    #[test]
    fn test_user_ip_from_remote_addr() {
        let mut event = Annotated::new(Event {
            request: Annotated::from(Request {
                env: Annotated::new({
                    let mut map = Object::new();
                    map.insert(
                        "REMOTE_ADDR".to_string(),
                        Annotated::new(Value::String("2.125.160.216".to_string())),
                    );
                    map
                }),
                ..Request::default()
            }),
            platform: Annotated::new("javascript".to_owned()),
            ..Event::default()
        });

        let config = StoreConfig::default();
        let mut processor = StoreNormalizeProcessor::new(Arc::new(config));
        normalize_event(&mut event, &NormalizationConfig::default());

        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let ip_addr = get_value!(event.user.ip_address!);
        assert_eq!(ip_addr, &IpAddr("2.125.160.216".to_string()));
    }

    #[test]
    fn test_user_ip_from_invalid_remote_addr() {
        let mut event = Annotated::new(Event {
            request: Annotated::from(Request {
                env: Annotated::new({
                    let mut map = Object::new();
                    map.insert(
                        "REMOTE_ADDR".to_string(),
                        Annotated::new(Value::String("whoops".to_string())),
                    );
                    map
                }),
                ..Request::default()
            }),
            platform: Annotated::new("javascript".to_owned()),
            ..Event::default()
        });

        let config = StoreConfig::default();
        let mut processor = StoreNormalizeProcessor::new(Arc::new(config));
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(Annotated::empty(), event.value().unwrap().user);
    }

    #[test]
    fn test_user_ip_from_client_ip_without_auto() {
        let mut event = Annotated::new(Event {
            platform: Annotated::new("javascript".to_owned()),
            ..Default::default()
        });

        let ip_address = IpAddr::parse("2.125.160.216").unwrap();
        let config = StoreConfig {
            client_ip: Some(ip_address.clone()),
            ..StoreConfig::default()
        };

        let mut processor = StoreNormalizeProcessor::new(Arc::new(config));
        normalize_event(
            &mut event,
            &NormalizationConfig {
                client_ip: Some(&ip_address),
                ..Default::default()
            },
        );
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let ip_addr = get_value!(event.user.ip_address!);
        assert_eq!(ip_addr, &IpAddr("2.125.160.216".to_string()));
    }

    #[test]
    fn test_user_ip_from_client_ip_with_auto() {
        let mut event = Annotated::new(Event {
            user: Annotated::new(User {
                ip_address: Annotated::new(IpAddr::auto()),
                ..Default::default()
            }),
            ..Default::default()
        });

        let ip_address = IpAddr::parse("2.125.160.216").unwrap();

        let geo = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();
        normalize_event(
            &mut event,
            &NormalizationConfig {
                client_ip: Some(&ip_address),
                geoip_lookup: Some(&geo),
                ..Default::default()
            },
        );

        let user = get_value!(event.user!);
        let ip_addr = user.ip_address.value().expect("ip address missing");

        assert_eq!(ip_addr, &IpAddr("2.125.160.216".to_string()));
        assert!(user.geo.value().is_some());
    }

    #[test]
    fn test_user_ip_from_client_ip_without_appropriate_platform() {
        let mut event = Annotated::new(Event::default());

        let ip_address = IpAddr::parse("2.125.160.216").unwrap();
        let geo = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();
        normalize_event(
            &mut event,
            &NormalizationConfig {
                client_ip: Some(&ip_address),
                geoip_lookup: Some(&geo),
                ..Default::default()
            },
        );

        let user = get_value!(event.user!);
        assert!(user.ip_address.value().is_none());
        assert!(user.geo.value().is_none());
    }

    #[test]
    fn test_event_level_defaulted() {
        let processor = &mut StoreNormalizeProcessor::default();
        let mut event = Annotated::new(Event::default());
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, processor, ProcessingState::root()).unwrap();
        assert_eq!(get_value!(event.level), Some(&Level::Error));
    }

    #[test]
    fn test_transaction_level_untouched() {
        let processor = &mut StoreNormalizeProcessor::default();
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(1987, 6, 5, 4, 3, 2).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(1987, 6, 5, 4, 3, 2).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Event::default()
        });
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, processor, ProcessingState::root()).unwrap();
        assert_eq!(get_value!(event.level), Some(&Level::Info));
    }

    #[test]
    fn test_environment_tag_is_moved() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![Annotated::new(TagEntry(
                Annotated::new("environment".to_string()),
                Annotated::new("despacito".to_string()),
            ))]))),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();

        assert_eq!(event.environment.as_str(), Some("despacito"));
        assert_eq!(event.tags.value(), Some(&Tags(vec![].into())));
    }

    #[test]
    fn test_empty_environment_is_removed_and_overwritten_with_tag() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![Annotated::new(TagEntry(
                Annotated::new("environment".to_string()),
                Annotated::new("despacito".to_string()),
            ))]))),
            environment: Annotated::new("".to_string()),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();

        assert_eq!(event.environment.as_str(), Some("despacito"));
        assert_eq!(event.tags.value(), Some(&Tags(vec![].into())));
    }

    #[test]
    fn test_empty_environment_is_removed() {
        let mut event = Annotated::new(Event {
            environment: Annotated::new("".to_string()),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_eq!(get_value!(event.environment), None);
    }
    #[test]
    fn test_replay_id_added_from_dsc() {
        let replay_id = Uuid::new_v4();
        let mut event = Annotated::new(Event {
            contexts: Annotated::new(Contexts(Object::new())),
            ..Event::default()
        });
        let config = StoreConfig {
            replay_id: Some(replay_id),
            ..StoreConfig::default()
        };
        let mut processor = StoreNormalizeProcessor::new(Arc::new(config));
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();

        assert_eq!(event.contexts, {
            let mut contexts = Contexts::new();
            contexts.add(ReplayContext {
                replay_id: Annotated::new(EventId(replay_id)),
                other: Object::default(),
            });
            Annotated::new(contexts)
        })
    }

    #[test]
    fn test_none_environment_errors() {
        let mut event = Annotated::new(Event {
            environment: Annotated::new("none".to_string()),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let environment = get_path!(event.environment!);
        let expected_original = &Value::String("none".to_string());

        assert_eq!(
            environment.meta().iter_errors().collect::<Vec<&Error>>(),
            vec![&Error::new(ErrorKind::InvalidData)],
        );
        assert_eq!(
            environment.meta().original_value().unwrap(),
            expected_original
        );
        assert_eq!(environment.value(), None);
    }

    #[test]
    fn test_invalid_release_removed() {
        let mut event = Annotated::new(Event {
            release: Annotated::new(LenientString("Latest".to_string())),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let release = get_path!(event.release!);
        let expected_original = &Value::String("Latest".to_string());

        assert_eq!(
            release.meta().iter_errors().collect::<Vec<&Error>>(),
            vec![&Error::new(ErrorKind::InvalidData)],
        );
        assert_eq!(release.meta().original_value().unwrap(), expected_original);
        assert_eq!(release.value(), None);
    }

    #[test]
    fn test_top_level_keys_moved_into_tags() {
        let mut event = Annotated::new(Event {
            server_name: Annotated::new("foo".to_string()),
            site: Annotated::new("foo".to_string()),
            tags: Annotated::new(Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("site".to_string()),
                    Annotated::new("old".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("server_name".to_string()),
                    Annotated::new("old".to_string()),
                )),
            ]))),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(get_value!(event.site), None);
        assert_eq!(get_value!(event.server_name), None);

        assert_eq!(
            get_value!(event.tags!),
            &Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("site".to_string()),
                    Annotated::new("foo".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("server_name".to_string()),
                    Annotated::new("foo".to_string()),
                )),
            ]))
        );
    }

    #[test]
    fn test_internal_tags_removed() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("release".to_string()),
                    Annotated::new("foo".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("dist".to_string()),
                    Annotated::new("foo".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("user".to_string()),
                    Annotated::new("foo".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("filename".to_string()),
                    Annotated::new("foo".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("function".to_string()),
                    Annotated::new("foo".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("something".to_string()),
                    Annotated::new("else".to_string()),
                )),
            ]))),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(get_value!(event.tags!).len(), 1);
    }

    #[test]
    fn test_empty_tags_removed() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("".to_string()),
                    Annotated::new("foo".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_string()),
                    Annotated::new("".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("something".to_string()),
                    Annotated::new("else".to_string()),
                )),
            ]))),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(
            get_value!(event.tags!),
            &Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::from_error(Error::nonempty(), None),
                    Annotated::new("foo".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_string()),
                    Annotated::from_error(Error::nonempty(), None),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("something".to_string()),
                    Annotated::new("else".to_string()),
                )),
            ]))
        );
    }

    #[test]
    fn test_tags_deduplicated() {
        let mut event = Annotated::new(Event {
            tags: Annotated::new(Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_string()),
                    Annotated::new("1".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("bar".to_string()),
                    Annotated::new("1".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_string()),
                    Annotated::new("2".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("bar".to_string()),
                    Annotated::new("2".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_string()),
                    Annotated::new("3".to_string()),
                )),
            ]))),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        // should keep the first occurrence of every tag
        assert_eq!(
            get_value!(event.tags!),
            &Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("foo".to_string()),
                    Annotated::new("1".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("bar".to_string()),
                    Annotated::new("1".to_string()),
                )),
            ]))
        );
    }

    #[test]
    fn test_transaction_status_defaulted_to_unknown() {
        let mut object = Object::new();
        let trace_context = TraceContext {
            // We assume the status to be null.
            status: Annotated::empty(),
            ..TraceContext::default()
        };
        object.insert(
            "trace".to_string(),
            Annotated::new(ContextInner(Context::Trace(Box::new(trace_context)))),
        );

        let mut event = Annotated::new(Event {
            contexts: Annotated::new(Contexts(object)),
            ..Event::default()
        });
        let config = StoreConfig {
            ..StoreConfig::default()
        };
        let mut processor = StoreNormalizeProcessor::new(Arc::new(config));
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();

        let event_trace_context = event.context::<TraceContext>().unwrap();
        assert_eq!(
            event_trace_context.status,
            Annotated::new(SpanStatus::Unknown)
        )
    }

    #[test]
    fn test_unknown_debug_image() {
        let mut event = Annotated::new(Event {
            debug_meta: Annotated::new(DebugMeta {
                images: Annotated::new(vec![Annotated::new(DebugImage::Other(Object::default()))]),
                ..DebugMeta::default()
            }),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(
            get_path!(event.debug_meta!),
            &Annotated::new(DebugMeta {
                images: Annotated::new(vec![Annotated::from_error(
                    Error::invalid("unsupported debug image type"),
                    Some(Value::Object(Object::default())),
                )]),
                ..DebugMeta::default()
            })
        );
    }

    #[test]
    fn test_context_line_default() {
        let mut frame = Annotated::new(Frame {
            pre_context: Annotated::new(vec![Annotated::default(), Annotated::new("".to_string())]),
            post_context: Annotated::new(vec![
                Annotated::new("".to_string()),
                Annotated::default(),
            ]),
            ..Frame::default()
        });

        normalize_non_raw_frame(&mut frame);

        let frame = frame.value().unwrap();
        assert_eq!(frame.context_line.as_str(), Some(""));
    }

    #[test]
    fn test_context_line_retain() {
        let mut frame = Annotated::new(Frame {
            pre_context: Annotated::new(vec![Annotated::default(), Annotated::new("".to_string())]),
            post_context: Annotated::new(vec![
                Annotated::new("".to_string()),
                Annotated::default(),
            ]),
            context_line: Annotated::new("some line".to_string()),
            ..Frame::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        process_value(&mut frame, &mut processor, ProcessingState::root()).unwrap();

        let frame = frame.value().unwrap();
        assert_eq!(frame.context_line.as_str(), Some("some line"));
    }

    #[test]
    fn test_frame_null_context_lines() {
        let mut frame = Annotated::new(Frame {
            pre_context: Annotated::new(vec![Annotated::default(), Annotated::new("".to_string())]),
            post_context: Annotated::new(vec![
                Annotated::new("".to_string()),
                Annotated::default(),
            ]),
            ..Frame::default()
        });

        normalize_non_raw_frame(&mut frame);

        assert_eq!(
            *get_value!(frame.pre_context!),
            vec![
                Annotated::new("".to_string()),
                Annotated::new("".to_string())
            ],
        );
        assert_eq!(
            *get_value!(frame.post_context!),
            vec![
                Annotated::new("".to_string()),
                Annotated::new("".to_string())
            ],
        );
    }

    #[test]
    fn test_too_long_tags() {
        let mut event = Annotated::new(Event {
        tags: Annotated::new(Tags(PairList(
            vec![Annotated::new(TagEntry(
                Annotated::new("foobar".to_string()),
                Annotated::new("...........................................................................................................................................................................................................".to_string()),
            )), Annotated::new(TagEntry(
                Annotated::new("foooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo".to_string()),
                Annotated::new("bar".to_string()),
            ))]),
        )),
        ..Event::default()
    });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(
            get_value!(event.tags!),
            &Tags(PairList(vec![
                Annotated::new(TagEntry(
                    Annotated::new("foobar".to_string()),
                    Annotated::from_error(Error::new(ErrorKind::ValueTooLong), None),
                )),
                Annotated::new(TagEntry(
                    Annotated::from_error(Error::new(ErrorKind::ValueTooLong), None),
                    Annotated::new("bar".to_string()),
                )),
            ]))
        );
    }

    #[test]
    fn test_too_long_distribution() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "fingerprint": [
    "{{ default }}"
  ],
  "platform": "other",
  "dist": "52df9022835246eeb317dbd739ccd059-52df9022835246eeb317dbd739ccd059-52df9022835246eeb317dbd739ccd059"
}"#;

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let dist = &event.value().unwrap().dist;
        let result = &Annotated::<String>::from_error(
            Error::new(ErrorKind::ValueTooLong),
            Some(Value::String("52df9022835246eeb317dbd739ccd059-52df9022835246eeb317dbd739ccd059-52df9022835246eeb317dbd739ccd059".to_string()))
        );
        assert_eq!(dist, result);
    }

    #[test]
    fn test_regression_backfills_abs_path_even_when_moving_stacktrace() {
        let mut event = Annotated::new(Event {
            exceptions: Annotated::new(Values::new(vec![Annotated::new(Exception {
                ty: Annotated::new("FooDivisionError".to_string()),
                value: Annotated::new("hi".to_string().into()),
                ..Exception::default()
            })])),
            stacktrace: Annotated::new(
                RawStacktrace {
                    frames: Annotated::new(vec![Annotated::new(Frame {
                        module: Annotated::new("MyModule".to_string()),
                        filename: Annotated::new("MyFilename".into()),
                        function: Annotated::new("Void FooBar()".to_string()),
                        ..Frame::default()
                    })]),
                    ..RawStacktrace::default()
                }
                .into(),
            ),
            ..Event::default()
        });

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(
            get_value!(event.exceptions.values[0].stacktrace!),
            &Stacktrace(RawStacktrace {
                frames: Annotated::new(vec![Annotated::new(Frame {
                    module: Annotated::new("MyModule".to_string()),
                    filename: Annotated::new("MyFilename".into()),
                    abs_path: Annotated::new("MyFilename".into()),
                    function: Annotated::new("Void FooBar()".to_string()),
                    ..Frame::default()
                })]),
                ..RawStacktrace::default()
            })
        );
    }

    #[test]
    fn test_parses_sdk_info_from_header() {
        let mut event = Annotated::new(Event::default());
        let mut processor = StoreNormalizeProcessor::new(Arc::new(StoreConfig {
            client: Some("_fooBar/0.0.0".to_string()),
            ..StoreConfig::default()
        }));

        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(
            get_path!(event.client_sdk!),
            &Annotated::new(ClientSdkInfo {
                name: Annotated::new("_fooBar".to_string()),
                version: Annotated::new("0.0.0".to_string()),
                ..ClientSdkInfo::default()
            })
        );
    }

    #[test]
    fn test_discards_received() {
        let mut event = Annotated::new(Event {
            received: FromValue::from_value(Annotated::new(Value::U64(696_969_696_969))),
            ..Default::default()
        });

        let mut processor = StoreNormalizeProcessor::default();

        validate_event_timestamps(&mut event, &EventValidationConfig::default()).unwrap();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(get_value!(event.received!), get_value!(event.timestamp!));
    }

    #[test]
    fn test_grouping_config() {
        let mut event = Annotated::new(Event {
            logentry: Annotated::from(LogEntry {
                message: Annotated::new("Hello World!".to_string().into()),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut processor = StoreNormalizeProcessor::new(Arc::new(StoreConfig {
            grouping_config: Some(json!({
                "id": "legacy:1234-12-12".to_string(),
            })),
            ..Default::default()
        }));

        validate_event_timestamps(&mut event, &EventValidationConfig::default()).unwrap();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        insta::assert_ron_snapshot!(SerializableAnnotated(&event), {
            ".event_id" => "[event-id]",
            ".received" => "[received]",
            ".timestamp" => "[timestamp]"
        }, @r#"
        {
          "event_id": "[event-id]",
          "level": "error",
          "type": "default",
          "logentry": {
            "formatted": "Hello World!",
          },
          "logger": "",
          "platform": "other",
          "timestamp": "[timestamp]",
          "received": "[received]",
          "grouping_config": {
            "id": "legacy:1234-12-12",
          },
        }
        "#);
    }

    #[test]
    fn test_logentry_error() {
        let json = r#"
{
    "event_id": "74ad1301f4df489ead37d757295442b1",
    "timestamp": 1668148328.308933,
    "received": 1668148328.308933,
    "level": "error",
    "platform": "python",
    "logentry": {
        "params": [
            "bogus"
        ],
        "formatted": 42
    }
}
"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        let mut processor = StoreNormalizeProcessor::default();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_json_snapshot!(SerializableAnnotated(&event), {".received" => "[received]"}, @r#"
        {
          "event_id": "74ad1301f4df489ead37d757295442b1",
          "level": "error",
          "type": "default",
          "logentry": null,
          "logger": "",
          "platform": "python",
          "timestamp": 1668148328.308933,
          "received": "[received]",
          "_meta": {
            "logentry": {
              "": {
                "err": [
                  [
                    "invalid_data",
                    {
                      "reason": "no message present"
                    }
                  ]
                ],
                "val": {
                  "formatted": null,
                  "message": null,
                  "params": [
                    "bogus"
                  ]
                }
              }
            }
          }
        }"#)
    }

    #[test]
    fn test_future_timestamp() {
        let mut event = Annotated::new(Event {
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 3, 0, 2, 0).unwrap().into()),
            ..Default::default()
        });

        let received_at = Some(Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap());
        let max_secs_in_past = Some(30 * 24 * 3600);
        let max_secs_in_future = Some(60);

        let mut processor = StoreNormalizeProcessor::new(Arc::new(StoreConfig {
            received_at,
            max_secs_in_past,
            max_secs_in_future,
            ..Default::default()
        }));
        validate_event_timestamps(
            &mut event,
            &EventValidationConfig {
                received_at,
                max_secs_in_past,
                max_secs_in_future,
                is_validated: false,
            },
        )
        .unwrap();
        validate_transaction(&mut event, &TransactionValidationConfig::default()).unwrap();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        insta::assert_ron_snapshot!(SerializableAnnotated(&event), {
        ".event_id" => "[event-id]",
    }, @r#"
    {
      "event_id": "[event-id]",
      "level": "error",
      "type": "default",
      "logger": "",
      "platform": "other",
      "timestamp": 946857600.0,
      "received": 946857600.0,
      "_meta": {
        "timestamp": {
          "": Meta(Some(MetaInner(
            err: [
              [
                "future_timestamp",
                {
                  "sdk_time": "2000-01-03T00:02:00+00:00",
                  "server_time": "2000-01-03T00:00:00+00:00",
                },
              ],
            ],
          ))),
        },
      },
    }
    "#);
    }

    #[test]
    fn test_past_timestamp() {
        let mut event = Annotated::new(Event {
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 3, 0, 0, 0).unwrap().into()),
            ..Default::default()
        });

        let received_at = Some(Utc.with_ymd_and_hms(2000, 3, 3, 0, 0, 0).unwrap());
        let max_secs_in_past = Some(30 * 24 * 3600);
        let max_secs_in_future = Some(60);

        let mut processor = StoreNormalizeProcessor::new(Arc::new(StoreConfig {
            received_at,
            max_secs_in_past,
            max_secs_in_future,
            ..Default::default()
        }));
        validate_event_timestamps(
            &mut event,
            &EventValidationConfig {
                received_at,
                max_secs_in_past,
                max_secs_in_future,
                is_validated: false,
            },
        )
        .unwrap();
        normalize_event(&mut event, &NormalizationConfig::default());
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        insta::assert_ron_snapshot!(SerializableAnnotated(&event), {
        ".event_id" => "[event-id]",
    }, @r#"
    {
      "event_id": "[event-id]",
      "level": "error",
      "type": "default",
      "logger": "",
      "platform": "other",
      "timestamp": 952041600.0,
      "received": 952041600.0,
      "_meta": {
        "timestamp": {
          "": Meta(Some(MetaInner(
            err: [
              [
                "past_timestamp",
                {
                  "sdk_time": "2000-01-03T00:00:00+00:00",
                  "server_time": "2000-03-03T00:00:00+00:00",
                },
              ],
            ],
          ))),
        },
      },
    }
    "#);
    }

    #[test]
    fn test_normalize_logger_empty() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "",
            })
            .into(),
        );

        let mut processor = StoreNormalizeProcessor::default();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_trimmed() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": " \t  \t   ",
            })
            .into(),
        );

        let mut processor = StoreNormalizeProcessor::default();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_short_no_trimming() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "my.short-logger.isnt_trimmed",
            })
            .into(),
        );

        let mut processor = StoreNormalizeProcessor::default();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_exact_length() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "this_is-exactly-the_max_len.012345678901234567890123456789012345",
            })
            .into(),
        );

        let mut processor = StoreNormalizeProcessor::default();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_too_long_single_word() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "this_is-way_too_long-and_we_only_have_one_word-so_we_cant_smart_trim",
            })
            .into(),
        );

        let mut processor = StoreNormalizeProcessor::default();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_word_trimmed_at_max() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "already_out.out.in.this_part-is-kept.this_right_here-is_an-extremely_long_word",
            })
            .into(),
        );

        let mut processor = StoreNormalizeProcessor::default();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_word_trimmed_before_max() {
        // This test verifies the "smart" trimming on words -- the logger name
        // should be cut before the max limit, removing entire words.
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "super_out.this_is_already_out_too.this_part-is-kept.this_right_here-is_a-very_long_word",
            })
            .into(),
        );

        let mut processor = StoreNormalizeProcessor::default();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_normalize_logger_word_leading_dots() {
        let mut event = Event::from_value(
            serde_json::json!({
                "event_id": "7637af36578e4e4592692e28a1d6e2ca",
                "platform": "java",
                "logger": "io.this-tests-the-smart-trimming-and-word-removal-around-dot.words",
            })
            .into(),
        );

        let mut processor = StoreNormalizeProcessor::default();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_light_normalization_is_idempotent() {
        // get an event, light normalize it. the result of that must be the same as light normalizing it once more
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            timestamp: Annotated::new(end.into()),
            start_timestamp: Annotated::new(start.into()),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                    op: Annotated::new("http.server".to_owned()),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),

                ..Default::default()
            })]),
            ..Default::default()
        });

        fn remove_received_from_event(event: &mut Annotated<Event>) -> &mut Annotated<Event> {
            relay_event_schema::processor::apply(event, |e, _m| {
                e.received = Annotated::empty();
                Ok(())
            })
            .unwrap();
            event
        }

        normalize_event(&mut event, &NormalizationConfig::default());
        let first = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        // Expected some fields (such as timestamps) exist after first light normalization.

        normalize_event(&mut event, &NormalizationConfig::default());
        let second = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        assert_eq!(&first, &second, "idempotency check failed");

        normalize_event(&mut event, &NormalizationConfig::default());
        let third = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        assert_eq!(&second, &third, "idempotency check failed");
    }

    #[test]
    fn test_light_normalize_validates_spans() {
        let event = Annotated::<Event>::from_json(
            r#"
            {
                "type": "transaction",
                "transaction": "/",
                "timestamp": 946684810.0,
                "start_timestamp": 946684800.0,
                "contexts": {
                    "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "http.server",
                    "type": "trace"
                    }
                },
                "spans": []
            }
            "#,
        )
        .unwrap();

        // All incomplete spans should be caught by light normalization:
        for span in [
            r#"null"#,
            r#"{
              "timestamp": 946684810.0,
              "start_timestamp": 946684900.0,
              "span_id": "fa90fdead5f74053",
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }"#,
            r#"{
              "timestamp": 946684810.0,
              "span_id": "fa90fdead5f74053",
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }"#,
            r#"{
              "timestamp": 946684810.0,
              "start_timestamp": 946684800.0,
              "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
            }"#,
            r#"{
              "timestamp": 946684810.0,
              "start_timestamp": 946684800.0,
              "span_id": "fa90fdead5f74053"
            }"#,
        ] {
            let mut modified_event = event.clone();
            let event_ref = modified_event.value_mut().as_mut().unwrap();
            event_ref
                .spans
                .set_value(Some(vec![Annotated::<Span>::from_json(span).unwrap()]));

            let res =
                validate_transaction(&mut modified_event, &TransactionValidationConfig::default());

            assert!(res.is_err(), "{span:?}");
        }
    }

    #[test]
    fn test_light_normalization_respects_is_renormalize() {
        let mut event = Annotated::<Event>::from_json(
            r#"
            {
                "type": "default",
                "tags": [["environment", "some_environment"]]
            }
            "#,
        )
        .unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                is_renormalize: true,
                ..Default::default()
            },
        );

        assert_debug_snapshot!(event.value().unwrap().tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "environment",
                        "some_environment",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_geo_in_light_normalize() {
        let mut event = Annotated::<Event>::from_json(
            r#"
            {
                "type": "transaction",
                "transaction": "/foo/",
                "timestamp": 946684810.0,
                "start_timestamp": 946684800.0,
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053",
                        "op": "http.server",
                        "type": "trace"
                    }
                },
                "transaction_info": {
                    "source": "url"
                },
                "user": {
                    "ip_address": "2.125.160.216"
                }
            }
            "#,
        )
        .unwrap();

        let lookup = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();

        // Extract user's geo information before normalization.
        let user_geo = event.value().unwrap().user.value().unwrap().geo.value();
        assert!(user_geo.is_none());

        normalize_event(
            &mut event,
            &NormalizationConfig {
                geoip_lookup: Some(&lookup),
                ..Default::default()
            },
        );

        // Extract user's geo information after normalization.
        let user_geo = event
            .value()
            .unwrap()
            .user
            .value()
            .unwrap()
            .geo
            .value()
            .unwrap();

        assert_eq!(user_geo.country_code.value().unwrap(), "GB");
        assert_eq!(user_geo.city.value().unwrap(), "Boxford");
    }

    #[test]
    fn test_normalize_app_start_spans_only_for_react_native_3_to_4_4() {
        let mut event = Event {
            spans: Annotated::new(vec![Annotated::new(Span {
                op: Annotated::new("app_start_cold".to_owned()),
                ..Default::default()
            })]),
            client_sdk: Annotated::new(ClientSdkInfo {
                name: Annotated::new("sentry.javascript.react-native".to_owned()),
                version: Annotated::new("4.5.0".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        normalize_app_start_spans(&mut event);
        assert_debug_snapshot!(event.spans, @r###"
        [
            Span {
                timestamp: ~,
                start_timestamp: ~,
                exclusive_time: ~,
                description: ~,
                op: "app_start_cold",
                span_id: ~,
                parent_span_id: ~,
                trace_id: ~,
                segment_id: ~,
                is_segment: ~,
                status: ~,
                tags: ~,
                origin: ~,
                profile_id: ~,
                data: ~,
                sentry_tags: ~,
                received: ~,
                measurements: ~,
                _metrics_summary: ~,
                platform: ~,
                was_transaction: ~,
                other: {},
            },
        ]
        "###);
    }

    #[test]
    fn test_normalize_app_start_cold_spans_for_react_native() {
        let mut event = Event {
            spans: Annotated::new(vec![Annotated::new(Span {
                op: Annotated::new("app_start_cold".to_owned()),
                ..Default::default()
            })]),
            client_sdk: Annotated::new(ClientSdkInfo {
                name: Annotated::new("sentry.javascript.react-native".to_owned()),
                version: Annotated::new("4.4.0".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        normalize_app_start_spans(&mut event);
        assert_debug_snapshot!(event.spans, @r###"
        [
            Span {
                timestamp: ~,
                start_timestamp: ~,
                exclusive_time: ~,
                description: ~,
                op: "app.start.cold",
                span_id: ~,
                parent_span_id: ~,
                trace_id: ~,
                segment_id: ~,
                is_segment: ~,
                status: ~,
                tags: ~,
                origin: ~,
                profile_id: ~,
                data: ~,
                sentry_tags: ~,
                received: ~,
                measurements: ~,
                _metrics_summary: ~,
                platform: ~,
                was_transaction: ~,
                other: {},
            },
        ]
        "###);
    }

    #[test]
    fn test_normalize_app_start_warm_spans_for_react_native() {
        let mut event = Event {
            spans: Annotated::new(vec![Annotated::new(Span {
                op: Annotated::new("app_start_warm".to_owned()),
                ..Default::default()
            })]),
            client_sdk: Annotated::new(ClientSdkInfo {
                name: Annotated::new("sentry.javascript.react-native".to_owned()),
                version: Annotated::new("4.4.0".to_owned()),
                ..Default::default()
            }),
            ..Default::default()
        };
        normalize_app_start_spans(&mut event);
        assert_debug_snapshot!(event.spans, @r###"
        [
            Span {
                timestamp: ~,
                start_timestamp: ~,
                exclusive_time: ~,
                description: ~,
                op: "app.start.warm",
                span_id: ~,
                parent_span_id: ~,
                trace_id: ~,
                segment_id: ~,
                is_segment: ~,
                status: ~,
                tags: ~,
                origin: ~,
                profile_id: ~,
                data: ~,
                sentry_tags: ~,
                received: ~,
                measurements: ~,
                _metrics_summary: ~,
                platform: ~,
                was_transaction: ~,
                other: {},
            },
        ]
        "###);
    }

    #[test]
    fn test_normalize_metrics_summary_metric_identifiers() {
        let mut metric_tags = BTreeMap::new();
        metric_tags.insert(
            "transaction".to_string(),
            Annotated::new("/hello".to_string()),
        );

        let metric_summary = vec![Annotated::new(MetricSummary {
            min: Annotated::new(1.0),
            max: Annotated::new(20.0),
            sum: Annotated::new(21.0),
            count: Annotated::new(2),
            tags: Annotated::new(metric_tags),
        })];

        let mut metrics_summary = BTreeMap::new();
        metrics_summary.insert(
            "d:page_duration@millisecond".to_string(),
            Annotated::new(metric_summary.clone()),
        );
        metrics_summary.insert(
            "c:custom/page_click@none".to_string(),
            Annotated::new(Default::default()),
        );
        metrics_summary.insert(
            "s:user@none".to_string(),
            Annotated::new(metric_summary.clone()),
        );
        metrics_summary.insert("invalid".to_string(), Annotated::new(metric_summary));
        metrics_summary.insert(
            "g:page_load@second".to_string(),
            Annotated::new(Default::default()),
        );
        let metrics_summary = MetricsSummary(metrics_summary);

        let mut event = Annotated::new(Event {
            spans: Annotated::new(vec![Annotated::new(Span {
                op: Annotated::new("my_span".to_owned()),
                _metrics_summary: Annotated::new(metrics_summary.clone()),
                ..Default::default()
            })]),
            _metrics_summary: Annotated::new(metrics_summary),
            ..Default::default()
        });
        normalize_all_metrics_summaries(event.value_mut().as_mut().unwrap());
        assert_json_snapshot!(SerializableAnnotated(&event), @r###"
        {
          "spans": [
            {
              "op": "my_span",
              "_metrics_summary": {
                "c:custom/page_click@none": [],
                "d:custom/page_duration@millisecond": [
                  {
                    "min": 1.0,
                    "max": 20.0,
                    "sum": 21.0,
                    "count": 2,
                    "tags": {
                      "transaction": "/hello"
                    }
                  }
                ],
                "g:custom/page_load@second": [],
                "invalid": null,
                "s:custom/user@none": [
                  {
                    "min": 1.0,
                    "max": 20.0,
                    "sum": 21.0,
                    "count": 2,
                    "tags": {
                      "transaction": "/hello"
                    }
                  }
                ]
              }
            }
          ],
          "_metrics_summary": {
            "c:custom/page_click@none": [],
            "d:custom/page_duration@millisecond": [
              {
                "min": 1.0,
                "max": 20.0,
                "sum": 21.0,
                "count": 2,
                "tags": {
                  "transaction": "/hello"
                }
              }
            ],
            "g:custom/page_load@second": [],
            "invalid": null,
            "s:custom/user@none": [
              {
                "min": 1.0,
                "max": 20.0,
                "sum": 21.0,
                "count": 2,
                "tags": {
                  "transaction": "/hello"
                }
              }
            ]
          },
          "_meta": {
            "_metrics_summary": {
              "invalid": {
                "": {
                  "err": [
                    [
                      "invalid_data",
                      {
                        "reason": "failed to parse metric"
                      }
                    ]
                  ],
                  "val": [
                    {
                      "count": 2,
                      "max": 20.0,
                      "min": 1.0,
                      "sum": 21.0,
                      "tags": {
                        "transaction": "/hello"
                      }
                    }
                  ]
                }
              }
            },
            "spans": {
              "0": {
                "_metrics_summary": {
                  "invalid": {
                    "": {
                      "err": [
                        [
                          "invalid_data",
                          {
                            "reason": "failed to parse metric"
                          }
                        ]
                      ],
                      "val": [
                        {
                          "count": 2,
                          "max": 20.0,
                          "min": 1.0,
                          "sum": 21.0,
                          "tags": {
                            "transaction": "/hello"
                          }
                        }
                      ]
                    }
                  }
                }
              }
            }
          }
        }
        "###);
    }
}
