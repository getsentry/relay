use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::Range;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use regex::Regex;
use relay_base_schema::metrics::{is_valid_metric_name, DurationUnit, FractionUnit, MetricUnit};
use relay_common::time::UnixTimestamp;
use relay_event_schema::processor::{
    self, MaxChars, ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::{
    AsPair, Breadcrumb, ClientSdkInfo, Context, ContextInner, Contexts, DebugImage, DeviceClass,
    Event, EventId, EventType, Exception, Frame, Headers, IpAddr, Level, LogEntry, Measurement,
    Measurements, ReplayContext, Request, SpanAttribute, SpanStatus, Stacktrace, Tags,
    TraceContext, User, VALID_PLATFORMS,
};
use relay_protocol::{
    Annotated, Empty, Error, ErrorKind, FromValue, Meta, Object, Remark, RemarkType, Value,
};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::span::tag_extraction::{self, extract_span_tags};
use crate::{
    schema, transactions, trimming, BreakdownsConfig, ClockDriftProcessor, GeoIpLookup,
    RawUserAgentInfo, SpanDescriptionRule, StoreConfig, TransactionNameConfig,
};

pub mod breakdowns;
pub mod span;
pub mod user_agent;
pub mod utils;

mod contexts;
mod logentry;
mod mechanism;
mod request;
mod stacktrace;

/// Defines a builtin measurement.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Hash, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct BuiltinMeasurementKey {
    name: String,
    unit: MetricUnit,
}

impl BuiltinMeasurementKey {
    /// Creates a new [`BuiltinMeasurementKey`].
    pub fn new(name: impl Into<String>, unit: MetricUnit) -> Self {
        Self {
            name: name.into(),
            unit,
        }
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

/// Validate fields that go into a `sentry.models.BoundedIntegerField`.
fn validate_bounded_integer_field(value: u64) -> ProcessingResult {
    if value < 2_147_483_647 {
        Ok(())
    } else {
        Err(ProcessingAction::DeleteValueHard)
    }
}

struct DedupCache(SmallVec<[u64; 16]>);

impl DedupCache {
    pub fn new() -> Self {
        Self(SmallVec::default())
    }

    pub fn probe<H: Hash>(&mut self, element: H) -> bool {
        let mut hasher = DefaultHasher::new();
        element.hash(&mut hasher);
        let hash = hasher.finish();

        if self.0.contains(&hash) {
            false
        } else {
            self.0.push(hash);
            true
        }
    }
}

/// Returns `true` if the given platform string is a known platform identifier.
///
/// See [`VALID_PLATFORMS`] for a list of all known platforms.
pub fn is_valid_platform(platform: &str) -> bool {
    VALID_PLATFORMS.contains(&platform)
}

fn normalize_dist(distribution: &mut Annotated<String>) -> ProcessingResult {
    processor::apply(distribution, |dist, meta| {
        let trimmed = dist.trim();
        if trimmed.is_empty() {
            return Err(ProcessingAction::DeleteValueHard);
        } else if bytecount::num_chars(trimmed.as_bytes()) > MaxChars::Distribution.limit() {
            meta.add_error(Error::new(ErrorKind::ValueTooLong));
            return Err(ProcessingAction::DeleteValueSoft);
        } else if trimmed != dist {
            *dist = trimmed.to_string();
        }
        Ok(())
    })
}

/// Compute additional measurements derived from existing ones.
///
/// The added measurements are:
///
/// ```text
/// frames_slow_rate := measurements.frames_slow / measurements.frames_total
/// frames_frozen_rate := measurements.frames_frozen / measurements.frames_total
/// stall_percentage := measurements.stall_total_time / transaction.duration
/// ```
fn compute_measurements(transaction_duration_ms: f64, measurements: &mut Measurements) {
    if let Some(frames_total) = measurements.get_value("frames_total") {
        if frames_total > 0.0 {
            if let Some(frames_frozen) = measurements.get_value("frames_frozen") {
                let frames_frozen_rate = Measurement {
                    value: (frames_frozen / frames_total).into(),
                    unit: (MetricUnit::Fraction(FractionUnit::Ratio)).into(),
                };
                measurements.insert("frames_frozen_rate".to_owned(), frames_frozen_rate.into());
            }
            if let Some(frames_slow) = measurements.get_value("frames_slow") {
                let frames_slow_rate = Measurement {
                    value: (frames_slow / frames_total).into(),
                    unit: MetricUnit::Fraction(FractionUnit::Ratio).into(),
                };
                measurements.insert("frames_slow_rate".to_owned(), frames_slow_rate.into());
            }
        }
    }

    // Get stall_percentage
    if transaction_duration_ms > 0.0 {
        if let Some(stall_total_time) = measurements
            .get("stall_total_time")
            .and_then(Annotated::value)
        {
            if matches!(
                stall_total_time.unit.value(),
                // Accept milliseconds or None, but not other units
                Some(&MetricUnit::Duration(DurationUnit::MilliSecond) | &MetricUnit::None) | None
            ) {
                if let Some(stall_total_time) = stall_total_time.value.0 {
                    let stall_percentage = Measurement {
                        value: (stall_total_time / transaction_duration_ms).into(),
                        unit: (MetricUnit::Fraction(FractionUnit::Ratio)).into(),
                    };
                    measurements.insert("stall_percentage".to_owned(), stall_percentage.into());
                }
            }
        }
    }
}

/// The processor that normalizes events for store.
pub struct NormalizeProcessor<'a> {
    config: Arc<StoreConfig>,
    geoip_lookup: Option<&'a GeoIpLookup>,
}

impl<'a> NormalizeProcessor<'a> {
    /// Creates a new normalization processor.
    pub fn new(config: Arc<StoreConfig>, geoip_lookup: Option<&'a GeoIpLookup>) -> Self {
        NormalizeProcessor {
            config,
            geoip_lookup,
        }
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

    fn normalize_spans(&self, event: &mut Event) {
        if event.ty.value() == Some(&EventType::Transaction) {
            normalize_app_start_spans(event);
            span::attributes::normalize_spans(event, &self.config.span_attributes);
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
        } else {
            EventType::Default
        }
    }
}

/// Replaces snake_case app start spans op with dot.case op.
///
/// This is done for the affected React Native SDK versions (from 3 to 4.4).
fn normalize_app_start_spans(event: &mut Event) {
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

/// Emit any breakdowns
fn normalize_breakdowns(event: &mut Event, breakdowns_config: Option<&BreakdownsConfig>) {
    match breakdowns_config {
        None => {}
        Some(config) => breakdowns::normalize_breakdowns(event, config),
    }
}

/// Remove measurements that do not conform to the given config.
///
/// Built-in measurements are accepted if their unit is correct, dropped otherwise.
/// Custom measurements are accepted up to a limit.
///
/// Note that [`Measurements`] is a BTreeMap, which means its keys are sorted.
/// This ensures that for two events with the same measurement keys, the same set of custom
/// measurements is retained.
fn remove_invalid_measurements(
    measurements: &mut Measurements,
    meta: &mut Meta,
    measurements_config: DynamicMeasurementsConfig,
    max_name_and_unit_len: Option<usize>,
) {
    let max_custom_measurements = measurements_config.max_custom_measurements().unwrap_or(0);

    let mut custom_measurements_count = 0;
    let mut removed_measurements = Object::new();

    measurements.retain(|name, value| {
        let measurement = match value.value_mut() {
            Some(m) => m,
            None => return false,
        };

        if !is_valid_metric_name(name) {
            meta.add_error(Error::invalid(format!(
                "Metric name contains invalid characters: \"{name}\""
            )));
            removed_measurements.insert(name.clone(), Annotated::new(std::mem::take(measurement)));
            return false;
        }

        // TODO(jjbayer): Should we actually normalize the unit into the event?
        let unit = measurement.unit.value().unwrap_or(&MetricUnit::None);

        if let Some(max_name_and_unit_len) = max_name_and_unit_len {
            let max_name_len = max_name_and_unit_len - unit.to_string().len();

            if name.len() > max_name_len {
                meta.add_error(Error::invalid(format!(
                    "Metric name too long {}/{max_name_len}: \"{name}\"",
                    name.len(),
                )));
                removed_measurements
                    .insert(name.clone(), Annotated::new(std::mem::take(measurement)));
                return false;
            }
        }

        // Check if this is a builtin measurement:
        for builtin_measurement in measurements_config.builtin_measurement_keys() {
            if &builtin_measurement.name == name {
                // If the unit matches a built-in measurement, we allow it.
                // If the name matches but the unit is wrong, we do not even accept it as a custom measurement,
                // and just drop it instead.
                return &builtin_measurement.unit == unit;
            }
        }

        // For custom measurements, check the budget:
        if custom_measurements_count < max_custom_measurements {
            custom_measurements_count += 1;
            return true;
        }

        meta.add_error(Error::invalid(format!("Too many measurements: {name}")));
        removed_measurements.insert(name.clone(), Annotated::new(std::mem::take(measurement)));

        false
    });

    if !removed_measurements.is_empty() {
        meta.set_original_value(Some(removed_measurements));
    }
}

/// Returns the unit of the provided metric.
///
/// For known measurements, this returns `Some(MetricUnit)`, which can also include
/// `Some(MetricUnit::None)`. For unknown measurement names, this returns `None`.
fn get_metric_measurement_unit(measurement_name: &str) -> Option<MetricUnit> {
    match measurement_name {
        // Web
        "fcp" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "lcp" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "fid" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "fp" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "inp" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "ttfb" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "ttfb.requesttime" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "cls" => Some(MetricUnit::None),

        // Mobile
        "app_start_cold" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "app_start_warm" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "frames_total" => Some(MetricUnit::None),
        "frames_slow" => Some(MetricUnit::None),
        "frames_slow_rate" => Some(MetricUnit::Fraction(FractionUnit::Ratio)),
        "frames_frozen" => Some(MetricUnit::None),
        "frames_frozen_rate" => Some(MetricUnit::Fraction(FractionUnit::Ratio)),
        "time_to_initial_display" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "time_to_first_display" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),

        // React-Native
        "stall_count" => Some(MetricUnit::None),
        "stall_total_time" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "stall_longest_time" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),
        "stall_percentage" => Some(MetricUnit::Fraction(FractionUnit::Ratio)),

        // Default
        _ => None,
    }
}

/// Replaces dot.case app start measurements keys with snake_case keys.
///
/// The dot.case app start measurements keys are treated as custom measurements.
/// The snake_case is the key expected by the Sentry UI to aggregate and display in graphs.
fn normalize_app_start_measurements(measurements: &mut Measurements) {
    if let Some(app_start_cold_value) = measurements.remove("app.start.cold") {
        measurements.insert("app_start_cold".to_string(), app_start_cold_value);
    }
    if let Some(app_start_warm_value) = measurements.remove("app.start.warm") {
        measurements.insert("app_start_warm".to_string(), app_start_warm_value);
    }
}

fn normalize_units(measurements: &mut Measurements) {
    for (name, measurement) in measurements.iter_mut() {
        let measurement = match measurement.value_mut() {
            Some(m) => m,
            None => continue,
        };

        let stated_unit = measurement.unit.value().copied();
        let default_unit = get_metric_measurement_unit(name);
        measurement
            .unit
            .set_value(Some(stated_unit.or(default_unit).unwrap_or_default()))
    }
}

/// Ensure measurements interface is only present for transaction events.
fn normalize_measurements(
    event: &mut Event,
    measurements_config: Option<DynamicMeasurementsConfig>,
    max_mri_len: Option<usize>,
) {
    if event.ty.value() != Some(&EventType::Transaction) {
        // Only transaction events may have a measurements interface
        event.measurements = Annotated::empty();
    } else if let Annotated(Some(ref mut measurements), ref mut meta) = event.measurements {
        normalize_app_start_measurements(measurements);
        normalize_units(measurements);
        if let Some(measurements_config) = measurements_config {
            remove_invalid_measurements(measurements, meta, measurements_config, max_mri_len);
        }

        let duration_millis = match (event.start_timestamp.0, event.timestamp.0) {
            (Some(start), Some(end)) => relay_common::time::chrono_to_positive_millis(end - start),
            _ => 0.0,
        };

        compute_measurements(duration_millis, measurements);
    }
}

fn normalize_user_agent(_event: &mut Event, normalize_user_agent: Option<bool>) {
    if normalize_user_agent.unwrap_or(false) {
        user_agent::normalize_user_agent(_event);
    }
}

fn normalize_exceptions(event: &mut Event) -> ProcessingResult {
    let os_hint = mechanism::OsHint::from_event(event);

    if let Some(exception_values) = event.exceptions.value_mut() {
        if let Some(exceptions) = exception_values.values.value_mut() {
            if exceptions.len() == 1 && event.stacktrace.value().is_some() {
                if let Some(exception) = exceptions.get_mut(0) {
                    if let Some(exception) = exception.value_mut() {
                        mem::swap(&mut exception.stacktrace, &mut event.stacktrace);
                        event.stacktrace = Annotated::empty();
                    }
                }
            }

            // Exception mechanism needs SDK information to resolve proper names in
            // exception meta (such as signal names). "SDK Information" really means
            // the operating system version the event was generated on. Some
            // normalization still works without sdk_info, such as mach_exception
            // names (they can only occur on macOS).
            //
            // We also want to validate some other aspects of it.
            for exception in exceptions {
                if let Some(exception) = exception.value_mut() {
                    if let Some(mechanism) = exception.mechanism.value_mut() {
                        mechanism::normalize_mechanism(mechanism, os_hint)?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Process the required stacktraces for light normalization.
///
/// The browser extension filter requires the last frame of the stacktrace of the first exception
/// processed. There's no need to do further processing at this early stage.
fn light_normalize_stacktraces(event: &mut Event) -> ProcessingResult {
    match event.exceptions.value_mut() {
        None => Ok(()),
        Some(exception) => match exception.values.value_mut() {
            None => Ok(()),
            Some(exceptions) => match exceptions.first_mut() {
                None => Ok(()),
                Some(first) => normalize_last_stacktrace_frame(first),
            },
        },
    }
}

fn normalize_last_stacktrace_frame(exception: &mut Annotated<Exception>) -> ProcessingResult {
    processor::apply(exception, |e, _| {
        processor::apply(&mut e.stacktrace, |s, _| match s.frames.value_mut() {
            None => Ok(()),
            Some(frames) => match frames.last_mut() {
                None => Ok(()),
                Some(frame) => processor::apply(frame, stacktrace::process_non_raw_frame),
            },
        })
    })
}

/// Removes internal tags and adds tags for well-known attributes.
fn normalize_event_tags(event: &mut Event) -> ProcessingResult {
    let tags = &mut event.tags.value_mut().get_or_insert_with(Tags::default).0;
    let environment = &mut event.environment;
    if environment.is_empty() {
        *environment = Annotated::empty();
    }

    // Fix case where legacy apps pass environment as a tag instead of a top level key
    if let Some(tag) = tags.remove("environment").and_then(Annotated::into_value) {
        environment.get_or_insert_with(|| tag);
    }

    // Remove internal tags, that are generated with a `sentry:` prefix when saving the event.
    // They are not allowed to be set by the client due to ambiguity. Also, deduplicate tags.
    let mut tag_cache = DedupCache::new();
    tags.retain(|entry| {
        match entry.value() {
            Some(tag) => match tag.key() {
                Some("release") | Some("dist") | Some("user") | Some("filename")
                | Some("function") => false,
                name => tag_cache.probe(name),
            },
            // ToValue will decide if we should skip serializing Annotated::empty()
            None => true,
        }
    });

    for tag in tags.iter_mut() {
        processor::apply(tag, |tag, _| {
            if let Some(key) = tag.key() {
                if key.is_empty() {
                    tag.0 = Annotated::from_error(Error::nonempty(), None);
                } else if bytecount::num_chars(key.as_bytes()) > MaxChars::TagKey.limit() {
                    tag.0 = Annotated::from_error(Error::new(ErrorKind::ValueTooLong), None);
                }
            }

            if let Some(value) = tag.value() {
                if value.is_empty() {
                    tag.1 = Annotated::from_error(Error::nonempty(), None);
                } else if bytecount::num_chars(value.as_bytes()) > MaxChars::TagValue.limit() {
                    tag.1 = Annotated::from_error(Error::new(ErrorKind::ValueTooLong), None);
                }
            }

            Ok(())
        })?;
    }

    let server_name = std::mem::take(&mut event.server_name);
    if server_name.value().is_some() {
        let tag_name = "server_name".to_string();
        tags.insert(tag_name, server_name);
    }

    let site = std::mem::take(&mut event.site);
    if site.value().is_some() {
        let tag_name = "site".to_string();
        tags.insert(tag_name, site);
    }

    Ok(())
}

/// Validates the timestamp range and sets a default value.
fn normalize_timestamps(
    event: &mut Event,
    meta: &mut Meta,
    received_at: Option<DateTime<Utc>>,
    max_secs_in_past: Option<i64>,
    max_secs_in_future: Option<i64>,
) -> ProcessingResult {
    let received_at = received_at.unwrap_or_else(Utc::now);

    let mut sent_at = None;
    let mut error_kind = ErrorKind::ClockDrift;

    processor::apply(&mut event.timestamp, |timestamp, _meta| {
        if let Some(secs) = max_secs_in_future {
            if *timestamp > received_at + Duration::seconds(secs) {
                error_kind = ErrorKind::FutureTimestamp;
                sent_at = Some(*timestamp);
                return Ok(());
            }
        }

        if let Some(secs) = max_secs_in_past {
            if *timestamp < received_at - Duration::seconds(secs) {
                error_kind = ErrorKind::PastTimestamp;
                sent_at = Some(*timestamp);
                return Ok(());
            }
        }

        Ok(())
    })?;

    ClockDriftProcessor::new(sent_at.map(|ts| ts.into_inner()), received_at)
        .error_kind(error_kind)
        .process_event(event, meta, ProcessingState::root())?;

    // Apply this after clock drift correction, otherwise we will malform it.
    event.received = Annotated::new(received_at.into());

    if event.timestamp.value().is_none() {
        event.timestamp.set_value(Some(received_at.into()));
    }

    processor::apply(&mut event.time_spent, |time_spent, _| {
        validate_bounded_integer_field(*time_spent)
    })?;

    Ok(())
}

/// Ensures that the `release` and `dist` fields match up.
fn normalize_release_dist(event: &mut Event) -> ProcessingResult {
    normalize_dist(&mut event.dist)
}

fn is_security_report(event: &Event) -> bool {
    event.csp.value().is_some()
        || event.expectct.value().is_some()
        || event.expectstaple.value().is_some()
        || event.hpkp.value().is_some()
}

/// Backfills common security report attributes.
fn normalize_security_report(
    event: &mut Event,
    client_ip: Option<&IpAddr>,
    user_agent: &RawUserAgentInfo<&str>,
) {
    if !is_security_report(event) {
        // This event is not a security report, exit here.
        return;
    }

    event.logger.get_or_insert_with(|| "csp".to_string());

    if let Some(client_ip) = client_ip {
        let user = event.user.value_mut().get_or_insert_with(User::default);
        user.ip_address = Annotated::new(client_ip.to_owned());
    }

    if !user_agent.is_empty() {
        let headers = event
            .request
            .value_mut()
            .get_or_insert_with(Request::default)
            .headers
            .value_mut()
            .get_or_insert_with(Headers::default);

        user_agent.populate_event_headers(headers);
    }
}

/// Backfills IP addresses in various places.
pub fn normalize_ip_addresses(
    request: &mut Annotated<Request>,
    user: &mut Annotated<User>,
    platform: Option<&str>,
    client_ip: Option<&IpAddr>,
) {
    // NOTE: This is highly order dependent, in the sense that both the statements within this
    // function need to be executed in a certain order, and that other normalization code
    // (geoip lookup) needs to run after this.
    //
    // After a series of regressions over the old Python spaghetti code we decided to put it
    // back into one function. If a desire to split this code up overcomes you, put this in a
    // new processor and make sure all of it runs before the rest of normalization.

    // Resolve {{auto}}
    if let Some(client_ip) = client_ip {
        if let Some(ref mut request) = request.value_mut() {
            if let Some(ref mut env) = request.env.value_mut() {
                if let Some(&mut Value::String(ref mut http_ip)) = env
                    .get_mut("REMOTE_ADDR")
                    .and_then(|annotated| annotated.value_mut().as_mut())
                {
                    if http_ip == "{{auto}}" {
                        *http_ip = client_ip.to_string();
                    }
                }
            }
        }

        if let Some(ref mut user) = user.value_mut() {
            if let Some(ref mut user_ip) = user.ip_address.value_mut() {
                if user_ip.is_auto() {
                    *user_ip = client_ip.to_owned();
                }
            }
        }
    }

    // Copy IPs from request interface to user, and resolve platform-specific backfilling
    let http_ip = request
        .value()
        .and_then(|request| request.env.value())
        .and_then(|env| env.get("REMOTE_ADDR"))
        .and_then(Annotated::<Value>::as_str)
        .and_then(|ip| IpAddr::parse(ip).ok());

    if let Some(http_ip) = http_ip {
        let user = user.value_mut().get_or_insert_with(User::default);
        user.ip_address.value_mut().get_or_insert(http_ip);
    } else if let Some(client_ip) = client_ip {
        let user = user.value_mut().get_or_insert_with(User::default);
        // auto is already handled above
        if user.ip_address.value().is_none() {
            // In an ideal world all SDKs would set {{auto}} explicitly.
            if let Some("javascript") | Some("cocoa") | Some("objc") = platform {
                user.ip_address = Annotated::new(client_ip.to_owned());
            }
        }
    }
}

fn normalize_logentry(logentry: &mut Annotated<LogEntry>, _meta: &mut Meta) -> ProcessingResult {
    processor::apply(logentry, logentry::normalize_logentry)
}

// Reads device specs (family, memory, cpu, etc) from context and sets the device.class tag to high,
// medium, or low.
fn normalize_device_class(event: &mut Event) {
    let tags = &mut event.tags.value_mut().get_or_insert_with(Tags::default).0;
    let tag_name = "device.class".to_owned();
    // Remove any existing device.class tag set by the client, since this should only be set by relay.
    tags.remove("device.class");
    if let Some(contexts) = event.contexts.value() {
        if let Some(device_class) = DeviceClass::from_contexts(contexts) {
            tags.insert(tag_name, Annotated::new(device_class.to_string()));
        }
    }
}

// Sets the user's GeoIp info based on user's IP address.
fn normalize_user_geoinfo(geoip_lookup: &GeoIpLookup, user: &mut User) {
    // Infer user.geo from user.ip_address
    if user.geo.value().is_none() {
        if let Some(ip_address) = user.ip_address.value() {
            if let Ok(Some(geo)) = geoip_lookup.lookup(ip_address.as_str()) {
                user.geo.set_value(Some(geo));
            }
        }
    }
}

/// Normalizes incoming contexts for the downstream metric extraction.
fn normalize_contexts(contexts: &mut Contexts, _: &mut Meta) -> ProcessingResult {
    for annotated in &mut contexts.0.values_mut() {
        if let Some(ContextInner(Context::Trace(context))) = annotated.value_mut() {
            normalize_trace_context(context)?
        }
    }

    Ok(())
}

fn normalize_trace_context(context: &mut TraceContext) -> ProcessingResult {
    context.status.get_or_insert_with(|| SpanStatus::Unknown);
    Ok(())
}

/// Configuration for [`light_normalize_event`].
#[derive(Clone, Debug)]
pub struct LightNormalizationConfig<'a> {
    /// The IP address of the SDK that sent the event.
    ///
    /// When `{{auto}}` is specified and there is no other IP address in the payload, such as in the
    /// `request` context, this IP address gets added to the `user` context.
    pub client_ip: Option<&'a IpAddr>,

    /// The user-agent and client hints obtained from the submission request headers.
    ///
    /// Client hints are the preferred way to infer device, operating system, and browser
    /// information should the event payload contain no such data. If no client hints are present,
    /// normalization falls back to the user agent.
    pub user_agent: RawUserAgentInfo<&'a str>,

    /// The time at which the event was received in this Relay.
    ///
    /// This timestamp is persisted into the event payload.
    pub received_at: Option<DateTime<Utc>>,

    /// The maximum amount of seconds an event can be dated in the past.
    ///
    /// If the event's timestamp is older, the received timestamp is assumed.
    pub max_secs_in_past: Option<i64>,

    /// The maximum amount of seconds an event can be predated into the future.
    ///
    /// If the event's timestamp lies further into the future, the received timestamp is assumed.
    pub max_secs_in_future: Option<i64>,

    /// The valid time range for transaction events.
    ///
    /// This time range should be inferred from storage dependencies, such as metrics storage.
    /// Transactions with an end timestamp outside of this time range are dropped as invalid.
    pub transaction_range: Option<Range<UnixTimestamp>>,

    /// The maximum length for names of custom measurements.
    ///
    /// Measurements with longer names are removed from the transaction event and replaced with a
    /// metadata entry.
    pub max_name_and_unit_len: Option<usize>,

    /// Configuration for measurement normalization in transaction events.
    ///
    /// Has an optional [`MeasurementsConfig`] from both the project and the global level.
    /// If at least one is provided, then normalization will truncate custom measurements
    /// and add units of known built-in measurements.
    pub measurements: Option<DynamicMeasurementsConfig<'a>>,

    /// Emit breakdowns based on given configuration.
    pub breakdowns_config: Option<&'a BreakdownsConfig>,

    /// When `Some(true)`, context information is extracted from the user agent.
    pub normalize_user_agent: Option<bool>,

    /// Configuration for sanitizing unparameterized transaction names.
    pub transaction_name_config: TransactionNameConfig<'a>,

    /// When `Some(true)`, it is assumed that the event has been normalized before.
    ///
    /// This disables certain normalizations, especially all that are not idempotent. The
    /// renormalize mode is intended for the use in the processing pipeline, so an event modified
    /// during ingestion can be validated against the schema and large data can be trimmed. However,
    /// advanced normalizations such as inferring contexts or clock drift correction are disabled.
    ///
    /// `None` equals to `false`.
    pub is_renormalize: bool,

    /// When `true`, infers the device class from CPU and model.
    pub device_class_synthesis_config: bool,

    /// When `true`, extracts tags from event and spans and materializes them into `span.data`.
    pub enrich_spans: bool,

    /// When `true`, computes and materializes attributes in spans based on the given configuration.
    pub light_normalize_spans: bool,

    /// The maximum allowed size of tag values in bytes. Longer values will be cropped.
    pub max_tag_value_length: usize, // TODO: move span related fields into separate config.

    /// Configuration for replacing identifiers in the span description with placeholders.
    ///
    /// This is similar to `transaction_name_config`, but applies to span descriptions.
    pub span_description_rules: Option<&'a Vec<SpanDescriptionRule>>,

    /// An initialized GeoIP lookup.
    pub geoip_lookup: Option<&'a GeoIpLookup>,

    /// When `Some(true)`, individual parts of the event payload is trimmed to a maximum size.
    ///
    /// See the event schema for size declarations.
    pub enable_trimming: bool,
}

impl Default for LightNormalizationConfig<'_> {
    fn default() -> Self {
        Self {
            client_ip: Default::default(),
            user_agent: Default::default(),
            received_at: Default::default(),
            max_secs_in_past: Default::default(),
            max_secs_in_future: Default::default(),
            transaction_range: Default::default(),
            max_name_and_unit_len: Default::default(),
            breakdowns_config: Default::default(),
            normalize_user_agent: Default::default(),
            transaction_name_config: Default::default(),
            is_renormalize: Default::default(),
            device_class_synthesis_config: Default::default(),
            enrich_spans: Default::default(),
            light_normalize_spans: Default::default(),
            max_tag_value_length: usize::MAX,
            span_description_rules: Default::default(),
            geoip_lookup: Default::default(),
            enable_trimming: false,
            measurements: None,
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

/// Normalizes data in the event payload.
///
/// This function applies a series of transformations on the event payload based
/// on the passed configuration. See the config fields for a description of the
/// normalization steps. There is extended normalization available in the
/// [`StoreProcessor`](crate::StoreProcessor).
///
/// The returned [`ProcessingResult`] indicates whether the passed event should
/// be ingested or dropped.
pub fn light_normalize_event(
    event: &mut Annotated<Event>,
    config: LightNormalizationConfig,
) -> ProcessingResult {
    if config.is_renormalize {
        return Ok(());
    }

    processor::apply(event, |event, meta| {
        // Validate and normalize transaction
        // (internally noops for non-transaction events).
        // TODO: Parts of this processor should probably be a filter so we
        // can revert some changes to ProcessingAction)
        let mut transactions_processor = transactions::TransactionsProcessor::new(
            config.transaction_name_config,
            config.enrich_spans,
            config.span_description_rules,
            config.transaction_range,
        );
        transactions_processor.process_event(event, meta, ProcessingState::root())?;

        // Check for required and non-empty values
        schema::SchemaProcessor.process_event(event, meta, ProcessingState::root())?;

        // Process security reports first to ensure all props.
        normalize_security_report(event, config.client_ip, &config.user_agent);

        // Insert IP addrs before recursing, since geo lookup depends on it.
        normalize_ip_addresses(
            &mut event.request,
            &mut event.user,
            event.platform.as_str(),
            config.client_ip,
        );

        if let Some(geoip_lookup) = config.geoip_lookup {
            if let Some(user) = event.user.value_mut() {
                normalize_user_geoinfo(geoip_lookup, user)
            }
        }

        // Validate the basic attributes we extract metrics from
        processor::apply(&mut event.release, |release, meta| {
            if crate::validate_release(release).is_ok() {
                Ok(())
            } else {
                meta.add_error(ErrorKind::InvalidData);
                Err(ProcessingAction::DeleteValueSoft)
            }
        })?;
        processor::apply(&mut event.environment, |environment, meta| {
            if crate::validate_environment(environment).is_ok() {
                Ok(())
            } else {
                meta.add_error(ErrorKind::InvalidData);
                Err(ProcessingAction::DeleteValueSoft)
            }
        })?;

        // Default required attributes, even if they have errors
        normalize_logentry(&mut event.logentry, meta)?;
        normalize_release_dist(event)?; // dist is a tag extracted along with other metrics from transactions
        normalize_timestamps(
            event,
            meta,
            config.received_at,
            config.max_secs_in_past,
            config.max_secs_in_future,
        )?; // Timestamps are core in the metrics extraction
        normalize_event_tags(event)?; // Tags are added to every metric

        // TODO: Consider moving to store normalization
        if config.device_class_synthesis_config {
            normalize_device_class(event);
        }
        light_normalize_stacktraces(event)?;
        normalize_exceptions(event)?; // Browser extension filters look at the stacktrace
        normalize_user_agent(event, config.normalize_user_agent); // Legacy browsers filter
        normalize_measurements(event, config.measurements, config.max_name_and_unit_len); // Measurements are part of the metric extraction
        normalize_breakdowns(event, config.breakdowns_config); // Breakdowns are part of the metric extraction too

        // Some contexts need to be normalized before metrics extraction takes place.
        processor::apply(&mut event.contexts, normalize_contexts)?;

        if config.light_normalize_spans && event.ty.value() == Some(&EventType::Transaction) {
            // XXX(iker): span normalization runs in the store processor, but
            // the exclusive time is required for span metrics. Most of
            // transactions don't have many spans, but if this is no longer the
            // case and we roll this flag out for most projects, we may want to
            // reconsider this approach.
            normalize_app_start_spans(event);
            span::attributes::normalize_spans(
                event,
                &BTreeSet::from([SpanAttribute::ExclusiveTime]),
            );
        }

        if config.enrich_spans {
            extract_span_tags(
                event,
                &tag_extraction::Config {
                    max_tag_value_size: config.max_tag_value_length,
                },
            );
        }

        if config.enable_trimming {
            // Trim large strings and databags down
            trimming::TrimmingProcessor::new().process_event(
                event,
                meta,
                ProcessingState::root(),
            )?;
        }

        Ok(())
    })
}

impl<'a> Processor for NormalizeProcessor<'a> {
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
        processor::apply(&mut event.platform, |platform, _| {
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
        self.normalize_spans(event);
        self.normalize_trace_context(event);
        self.normalize_replay_context(event);

        Ok(())
    }

    fn process_breadcrumb(
        &mut self,
        breadcrumb: &mut Breadcrumb,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        breadcrumb.process_child_values(self, state)?;

        if breadcrumb.ty.value().is_empty() {
            breadcrumb.ty.set_value(Some("default".to_string()));
        }

        if breadcrumb.level.value().is_none() {
            breadcrumb.level.set_value(Some(Level::Info));
        }

        Ok(())
    }

    fn process_request(
        &mut self,
        request: &mut Request,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        request.process_child_values(self, state)?;

        request::normalize_request(request)?;

        Ok(())
    }

    fn process_user(
        &mut self,
        user: &mut User,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if !user.other.is_empty() {
            let data = user.data.value_mut().get_or_insert_with(Object::new);
            data.extend(std::mem::take(&mut user.other));
        }

        user.process_child_values(self, state)?;

        // Infer user.geo from user.ip_address
        if let Some(geoip_lookup) = self.geoip_lookup {
            normalize_user_geoinfo(geoip_lookup, user)
        }

        Ok(())
    }

    fn process_debug_image(
        &mut self,
        image: &mut DebugImage,
        meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        match image {
            DebugImage::Other(_) => {
                meta.add_error(Error::invalid("unsupported debug image type"));
                Err(ProcessingAction::DeleteValueSoft)
            }
            _ => Ok(()),
        }
    }

    fn process_exception(
        &mut self,
        exception: &mut Exception,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        exception.process_child_values(self, state)?;

        static TYPE_VALUE_RE: OnceCell<Regex> = OnceCell::new();
        let regex = TYPE_VALUE_RE.get_or_init(|| Regex::new(r"^(\w+):(.*)$").unwrap());

        if exception.ty.value().is_empty() {
            if let Some(value_str) = exception.value.value_mut() {
                let new_values = regex
                    .captures(value_str)
                    .map(|cap| (cap[1].to_string(), cap[2].trim().to_string().into()));

                if let Some((new_type, new_value)) = new_values {
                    exception.ty.set_value(Some(new_type));
                    *value_str = new_value;
                }
            }
        }

        if exception.ty.value().is_empty() && exception.value.value().is_empty() {
            meta.add_error(Error::with(ErrorKind::MissingAttribute, |error| {
                error.insert("attribute", "type or value");
            }));
            return Err(ProcessingAction::DeleteValueSoft);
        }

        Ok(())
    }

    fn process_frame(
        &mut self,
        frame: &mut Frame,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        frame.process_child_values(self, state)?;

        if frame.function.as_str() == Some("?") {
            frame.function.set_value(None);
        }

        if frame.symbol.as_str() == Some("?") {
            frame.symbol.set_value(None);
        }

        if let Some(lines) = frame.pre_context.value_mut() {
            for line in lines.iter_mut() {
                line.get_or_insert_with(String::new);
            }
        }

        if let Some(lines) = frame.post_context.value_mut() {
            for line in lines.iter_mut() {
                line.get_or_insert_with(String::new);
            }
        }

        if frame.context_line.value().is_none()
            && (!frame.pre_context.is_empty() || !frame.post_context.is_empty())
        {
            frame.context_line.set_value(Some(String::new()));
        }

        Ok(())
    }

    fn process_stacktrace(
        &mut self,
        stacktrace: &mut Stacktrace,
        meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        stacktrace::process_stacktrace(&mut stacktrace.0, meta)?;
        Ok(())
    }

    fn process_context(
        &mut self,
        context: &mut Context,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        contexts::normalize_context(context);
        Ok(())
    }

    fn process_contexts(
        &mut self,
        contexts: &mut Contexts,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        // Reprocessing context sent from SDKs must not be accepted, it is a Sentry-internal
        // construct.
        // This processor does not run on renormalization anyway.
        contexts.0.remove("reprocessing");
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
    use std::collections::BTreeMap;

    use chrono::TimeZone;
    use insta::assert_debug_snapshot;
    use relay_event_schema::processor::process_value;
    use relay_event_schema::protocol::{
        Csp, DebugMeta, DeviceContext, Frame, Geo, LenientString, LogEntry, PairList,
        RawStacktrace, Span, SpanId, TagEntry, TraceId, Values,
    };
    use relay_protocol::{
        assert_annotated_snapshot, get_path, get_value, FromValue, SerializableAnnotated,
    };
    use serde_json::json;
    use similar_asserts::assert_eq;
    use uuid::Uuid;

    use crate::user_agent::ClientHints;

    use super::*;

    impl Default for NormalizeProcessor<'_> {
        fn default() -> Self {
            NormalizeProcessor::new(Arc::new(StoreConfig::default()), None)
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
    fn test_handles_type_in_value() {
        let mut processor = NormalizeProcessor::default();

        let mut exception = Annotated::new(Exception {
            value: Annotated::new("ValueError: unauthorized".to_string().into()),
            ..Exception::default()
        });

        process_value(&mut exception, &mut processor, ProcessingState::root()).unwrap();
        let exception = exception.value().unwrap();
        assert_eq!(exception.value.as_str(), Some("unauthorized"));
        assert_eq!(exception.ty.as_str(), Some("ValueError"));

        let mut exception = Annotated::new(Exception {
            value: Annotated::new("ValueError:unauthorized".to_string().into()),
            ..Exception::default()
        });

        process_value(&mut exception, &mut processor, ProcessingState::root()).unwrap();
        let exception = exception.value().unwrap();
        assert_eq!(exception.value.as_str(), Some("unauthorized"));
        assert_eq!(exception.ty.as_str(), Some("ValueError"));
    }

    #[test]
    fn test_rejects_empty_exception_fields() {
        let mut processor = NormalizeProcessor::new(Arc::new(StoreConfig::default()), None);

        let mut exception = Annotated::new(Exception {
            value: Annotated::new("".to_string().into()),
            ty: Annotated::new("".to_string()),
            ..Default::default()
        });

        process_value(&mut exception, &mut processor, ProcessingState::root()).unwrap();
        assert!(exception.value().is_none());
        assert!(exception.meta().has_errors());
    }

    #[test]
    fn test_json_value() {
        let mut processor = NormalizeProcessor::default();

        let mut exception = Annotated::new(Exception {
            value: Annotated::new(r#"{"unauthorized":true}"#.to_string().into()),
            ..Exception::default()
        });
        process_value(&mut exception, &mut processor, ProcessingState::root()).unwrap();
        let exception = exception.value().unwrap();

        // Don't split a json-serialized value on the colon
        assert_eq!(exception.value.as_str(), Some(r#"{"unauthorized":true}"#));
        assert_eq!(exception.ty.value(), None);
    }

    #[test]
    fn test_exception_invalid() {
        let mut processor = NormalizeProcessor::default();

        let mut exception = Annotated::new(Exception::default());
        process_value(&mut exception, &mut processor, ProcessingState::root()).unwrap();

        let expected = Error::with(ErrorKind::MissingAttribute, |error| {
            error.insert("attribute", "type or value");
        });

        assert_eq!(
            exception.meta().iter_errors().collect_tuple(),
            Some((&expected,))
        );
    }

    #[test]
    fn test_geo_from_ip_address() {
        let lookup = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();
        let mut processor =
            NormalizeProcessor::new(Arc::new(StoreConfig::default()), Some(&lookup));

        let mut user = Annotated::new(User {
            ip_address: Annotated::new(IpAddr("2.125.160.216".to_string())),
            ..User::default()
        });

        process_value(&mut user, &mut processor, ProcessingState::root()).unwrap();

        let expected = Annotated::new(Geo {
            country_code: Annotated::new("GB".to_string()),
            city: Annotated::new("Boxford".to_string()),
            subdivision: Annotated::new("England".to_string()),
            region: Annotated::new("United Kingdom".to_string()),
            ..Geo::default()
        });
        assert_eq!(user.value().unwrap().geo, expected)
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
        let mut processor = NormalizeProcessor::new(Arc::new(config), None);
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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
        let mut processor = NormalizeProcessor::new(Arc::new(config), None);
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::new(Arc::new(config), None);
        let config = LightNormalizationConfig {
            client_ip: Some(&ip_address),
            ..Default::default()
        };
        light_normalize_event(&mut event, config).unwrap();
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
        let config = StoreConfig {
            client_ip: Some(ip_address.clone()),
            ..StoreConfig::default()
        };

        let geo = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();
        let mut processor = NormalizeProcessor::new(Arc::new(config), Some(&geo));
        let config = LightNormalizationConfig {
            client_ip: Some(&ip_address),
            ..Default::default()
        };
        light_normalize_event(&mut event, config).unwrap();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let user = get_value!(event.user!);
        let ip_addr = user.ip_address.value().expect("ip address missing");

        assert_eq!(ip_addr, &IpAddr("2.125.160.216".to_string()));
        assert!(user.geo.value().is_some());
    }

    #[test]
    fn test_user_ip_from_client_ip_without_appropriate_platform() {
        let mut event = Annotated::new(Event::default());

        let ip_address = IpAddr::parse("2.125.160.216").unwrap();
        let config = StoreConfig {
            client_ip: Some(ip_address.clone()),
            ..StoreConfig::default()
        };

        let geo = GeoIpLookup::open("tests/fixtures/GeoIP2-Enterprise-Test.mmdb").unwrap();
        let mut processor = NormalizeProcessor::new(Arc::new(config), Some(&geo));
        let config = LightNormalizationConfig {
            client_ip: Some(&ip_address),
            ..Default::default()
        };
        light_normalize_event(&mut event, config).unwrap();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let user = get_value!(event.user!);
        assert!(user.ip_address.value().is_none());
        assert!(user.geo.value().is_none());
    }

    #[test]
    fn test_event_level_defaulted() {
        let processor = &mut NormalizeProcessor::default();
        let mut event = Annotated::new(Event::default());
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
        process_value(&mut event, processor, ProcessingState::root()).unwrap();
        assert_eq!(get_value!(event.level), Some(&Level::Error));
    }

    #[test]
    fn test_transaction_level_untouched() {
        let processor = &mut NormalizeProcessor::default();
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
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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
        let mut processor = NormalizeProcessor::new(Arc::new(config), None);
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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
        let mut processor = NormalizeProcessor::new(Arc::new(config), None);
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let event = event.value().unwrap();

        let event_trace_context = event.context::<TraceContext>().unwrap();
        assert_eq!(
            event_trace_context.status,
            Annotated::new(SpanStatus::Unknown)
        )
    }

    #[test]
    fn test_user_data_moved() {
        let mut user = Annotated::new(User {
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_owned())),
                );
                map
            },
            ..User::default()
        });

        let mut processor = NormalizeProcessor::default();
        process_value(&mut user, &mut processor, ProcessingState::root()).unwrap();

        let user = user.value().unwrap();

        assert_eq!(user.data, {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_owned())),
            );
            Annotated::new(map)
        });

        assert_eq!(user.other, Object::new());
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        process_value(&mut frame, &mut processor, ProcessingState::root()).unwrap();

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

        let mut processor = NormalizeProcessor::default();
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

        let mut processor = NormalizeProcessor::default();
        process_value(&mut frame, &mut processor, ProcessingState::root()).unwrap();

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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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
        let mut processor = NormalizeProcessor::new(
            Arc::new(StoreConfig {
                client: Some("_fooBar/0.0.0".to_string()),
                ..StoreConfig::default()
            }),
            None,
        );

        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::default();

        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::new(
            Arc::new(StoreConfig {
                grouping_config: Some(json!({
                    "id": "legacy:1234-12-12".to_string(),
                })),
                ..Default::default()
            }),
            None,
        );

        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
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
        let mut event = Annotated::from_json(json).unwrap();

        let mut processor = NormalizeProcessor::default();
        let config = LightNormalizationConfig::default();
        light_normalize_event(&mut event, config).unwrap();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        insta::assert_json_snapshot!(SerializableAnnotated(&event), {".received" => "[received]"}, @r#"
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

        let mut processor = NormalizeProcessor::new(
            Arc::new(StoreConfig {
                received_at,
                max_secs_in_past,
                max_secs_in_future,
                ..Default::default()
            }),
            None,
        );
        let config = LightNormalizationConfig {
            received_at,
            max_secs_in_past,
            max_secs_in_future,
            ..Default::default()
        };
        light_normalize_event(&mut event, config).unwrap();
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

        let mut processor = NormalizeProcessor::new(
            Arc::new(StoreConfig {
                received_at,
                max_secs_in_past,
                max_secs_in_future,
                ..Default::default()
            }),
            None,
        );
        let config = LightNormalizationConfig {
            received_at,
            max_secs_in_past,
            max_secs_in_future,
            ..Default::default()
        };
        light_normalize_event(&mut event, config).unwrap();
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
    fn test_normalize_dist_none() {
        let mut dist = Annotated::default();
        normalize_dist(&mut dist).unwrap();
        assert_eq!(dist.value(), None);
    }

    #[test]
    fn test_normalize_dist_empty() {
        let mut dist = Annotated::new("".to_string());
        normalize_dist(&mut dist).unwrap();
        assert_eq!(dist.value(), None);
    }

    #[test]
    fn test_normalize_dist_trim() {
        let mut dist = Annotated::new(" foo  ".to_string());
        normalize_dist(&mut dist).unwrap();
        assert_eq!(dist.value(), Some(&"foo".to_string()));
    }

    #[test]
    fn test_normalize_dist_whitespace() {
        let mut dist = Annotated::new(" ".to_owned());
        normalize_dist(&mut dist).unwrap();
        assert_eq!(dist.value(), None);
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

        let mut processor = NormalizeProcessor::default();
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

        let mut processor = NormalizeProcessor::default();
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

        let mut processor = NormalizeProcessor::default();
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

        let mut processor = NormalizeProcessor::default();
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

        let mut processor = NormalizeProcessor::default();
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

        let mut processor = NormalizeProcessor::default();
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

        let mut processor = NormalizeProcessor::default();
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

        let mut processor = NormalizeProcessor::default();
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();
        assert_annotated_snapshot!(event);
    }

    #[test]
    fn test_computed_measurements() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "frames_slow": {"value": 1},
                "frames_frozen": {"value": 2},
                "frames_total": {"value": 4},
                "stall_total_time": {"value": 4000, "unit": "millisecond"}
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        normalize_measurements(&mut event, None, None);

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r#"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "measurements": {
            "frames_frozen": {
              "value": 2.0,
              "unit": "none",
            },
            "frames_frozen_rate": {
              "value": 0.5,
              "unit": "ratio",
            },
            "frames_slow": {
              "value": 1.0,
              "unit": "none",
            },
            "frames_slow_rate": {
              "value": 0.25,
              "unit": "ratio",
            },
            "frames_total": {
              "value": 4.0,
              "unit": "none",
            },
            "stall_percentage": {
              "value": 0.8,
              "unit": "ratio",
            },
            "stall_total_time": {
              "value": 4000.0,
              "unit": "millisecond",
            },
          },
        }
        "#);
    }

    #[test]
    fn test_filter_custom_measurements() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "my_custom_measurement_1": {"value": 123},
                "frames_frozen": {"value": 666, "unit": "invalid_unit"},
                "frames_slow": {"value": 1},
                "my_custom_measurement_3": {"value": 456},
                "my_custom_measurement_2": {"value": 789}
            }
        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let project_measurement_config: MeasurementsConfig = serde_json::from_value(json!({
            "builtinMeasurements": [
                {"name": "frames_frozen", "unit": "none"},
                {"name": "frames_slow", "unit": "none"}
            ],
            "maxCustomMeasurements": 2,
            "stray_key": "zzz"
        }))
        .unwrap();

        let dynamic_measurement_config = DynamicMeasurementsConfig {
            project: Some(&project_measurement_config),
            global: None,
        };

        normalize_measurements(&mut event, Some(dynamic_measurement_config), None);

        // Only two custom measurements are retained, in alphabetic order (1 and 2)
        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r#"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "measurements": {
            "frames_slow": {
              "value": 1.0,
              "unit": "none",
            },
            "my_custom_measurement_1": {
              "value": 123.0,
              "unit": "none",
            },
            "my_custom_measurement_2": {
              "value": 789.0,
              "unit": "none",
            },
          },
          "_meta": {
            "measurements": {
              "": Meta(Some(MetaInner(
                err: [
                  [
                    "invalid_data",
                    {
                      "reason": "Too many measurements: my_custom_measurement_3",
                    },
                  ],
                ],
                val: Some({
                  "my_custom_measurement_3": {
                    "unit": "none",
                    "value": 456.0,
                  },
                }),
              ))),
            },
          },
        }
        "#);
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

        let config = LightNormalizationConfig::default();

        fn remove_received_from_event(event: &mut Annotated<Event>) -> &mut Annotated<Event> {
            processor::apply(event, |e, _m| {
                e.received = Annotated::empty();
                Ok(())
            })
            .unwrap();
            event
        }

        light_normalize_event(&mut event, config.clone()).unwrap();
        let first = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        // Expected some fields (such as timestamps) exist after first light normalization.

        light_normalize_event(&mut event, config.clone()).unwrap();
        let second = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        assert_eq!(&first, &second, "idempotency check failed");

        light_normalize_event(&mut event, config).unwrap();
        let third = remove_received_from_event(&mut event.clone())
            .to_json()
            .unwrap();
        assert_eq!(&second, &third, "idempotency check failed");
    }

    #[test]
    fn test_normalize_units() {
        let mut measurements = Annotated::<Measurements>::from_json(
            r#"{
                "fcp": {"value": 1.1},
                "stall_count": {"value": 3.3},
                "foo": {"value": 8.8}
            }"#,
        )
        .unwrap()
        .into_value()
        .unwrap();
        insta::assert_debug_snapshot!(measurements, @r#"
        Measurements(
            {
                "fcp": Measurement {
                    value: 1.1,
                    unit: ~,
                },
                "foo": Measurement {
                    value: 8.8,
                    unit: ~,
                },
                "stall_count": Measurement {
                    value: 3.3,
                    unit: ~,
                },
            },
        )
        "#);
        normalize_units(&mut measurements);
        insta::assert_debug_snapshot!(measurements, @r#"
        Measurements(
            {
                "fcp": Measurement {
                    value: 1.1,
                    unit: Duration(
                        MilliSecond,
                    ),
                },
                "foo": Measurement {
                    value: 8.8,
                    unit: None,
                },
                "stall_count": Measurement {
                    value: 3.3,
                    unit: None,
                },
            },
        )
        "#);
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

            let res = light_normalize_event(&mut modified_event, Default::default());

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

        let result = light_normalize_event(
            &mut event,
            LightNormalizationConfig {
                is_renormalize: true,
                ..Default::default()
            },
        );

        assert!(result.is_ok());

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
        let config = LightNormalizationConfig {
            geoip_lookup: Some(&lookup),
            ..Default::default()
        };

        // Extract user's geo information before normalization.
        let user_geo = event.value().unwrap().user.value().unwrap().geo.value();
        assert!(user_geo.is_none());

        light_normalize_event(&mut event, config).unwrap();

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
    fn test_normalize_security_report() {
        let mut event = Event {
            csp: Annotated::from(Csp::default()),
            ..Default::default()
        };
        let ipaddr = IpAddr("213.164.1.114".to_string());

        let client_ip = Some(&ipaddr);
        let user_agent = RawUserAgentInfo::new_test_dummy();

        // This call should fill the event headers with info from the user_agent which is
        // tested below.
        normalize_security_report(&mut event, client_ip, &user_agent);

        let headers = event
            .request
            .value_mut()
            .get_or_insert_with(Request::default)
            .headers
            .value_mut()
            .get_or_insert_with(Headers::default);

        assert_eq!(
            event.user.value().unwrap().ip_address,
            Annotated::from(ipaddr)
        );
        assert_eq!(
            headers.get_header(RawUserAgentInfo::USER_AGENT),
            user_agent.user_agent
        );
        assert_eq!(
            headers.get_header(ClientHints::SEC_CH_UA),
            user_agent.client_hints.sec_ch_ua,
        );
        assert_eq!(
            headers.get_header(ClientHints::SEC_CH_UA_MODEL),
            user_agent.client_hints.sec_ch_ua_model,
        );
        assert_eq!(
            headers.get_header(ClientHints::SEC_CH_UA_PLATFORM),
            user_agent.client_hints.sec_ch_ua_platform,
        );
        assert_eq!(
            headers.get_header(ClientHints::SEC_CH_UA_PLATFORM_VERSION),
            user_agent.client_hints.sec_ch_ua_platform_version,
        );

        assert!(
            std::mem::size_of_val(&ClientHints::<&str>::default()) == 64,
            "If you add new fields, update the test accordingly"
        );
    }

    #[test]
    fn test_no_device_class() {
        let mut event = Event {
            ..Default::default()
        };
        normalize_device_class(&mut event);
        let tags = &event.tags.value_mut().get_or_insert_with(Tags::default).0;
        assert_eq!(None, tags.get("device_class"));
    }

    #[test]
    fn test_apple_low_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "iPhone".to_string().into(),
                    model: "iPhone8,4".to_string().into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "1",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_apple_medium_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "iPhone".to_string().into(),
                    model: "iPhone12,8".to_string().into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "2",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_apple_high_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "iPhone".to_string().into(),
                    model: "iPhone15,3".to_string().into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "3",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_android_low_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "android".to_string().into(),
                    processor_frequency: 1000.into(),
                    processor_count: 6.into(),
                    memory_size: (2 * 1024 * 1024 * 1024).into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "1",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_android_medium_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "android".to_string().into(),
                    processor_frequency: 2000.into(),
                    processor_count: 8.into(),
                    memory_size: (6 * 1024 * 1024 * 1024).into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "2",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_android_high_device_class() {
        let mut event = Event {
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(DeviceContext {
                    family: "android".to_string().into(),
                    processor_frequency: 2500.into(),
                    processor_count: 8.into(),
                    memory_size: (6 * 1024 * 1024 * 1024).into(),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        };
        normalize_device_class(&mut event);
        assert_debug_snapshot!(event.tags, @r#"
        Tags(
            PairList(
                [
                    TagEntry(
                        "device.class",
                        "3",
                    ),
                ],
            ),
        )
        "#);
    }

    #[test]
    fn test_keeps_valid_measurement() {
        let name = "lcp";
        let measurement = Measurement {
            value: Annotated::new(420.69),
            unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
        };

        assert!(!is_measurement_dropped(name, measurement));
    }

    #[test]
    fn test_drops_too_long_measurement_names() {
        let name = "lcpppppppppppppppppppppppppppp";
        let measurement = Measurement {
            value: Annotated::new(420.69),
            unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
        };

        assert!(is_measurement_dropped(name, measurement));
    }

    #[test]
    fn test_drops_measurements_with_invalid_characters() {
        let name = "i æm frøm nørwåy";
        let measurement = Measurement {
            value: Annotated::new(420.69),
            unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
        };

        assert!(is_measurement_dropped(name, measurement));
    }

    fn is_measurement_dropped(name: &str, measurement: Measurement) -> bool {
        let max_name_and_unit_len = Some(30);

        let mut measurements: BTreeMap<String, Annotated<Measurement>> = Object::new();
        measurements.insert(name.to_string(), Annotated::new(measurement));

        let mut measurements = Measurements(measurements);
        let mut meta = Meta::default();
        let measurements_config = MeasurementsConfig {
            max_custom_measurements: 1,
            ..Default::default()
        };

        let dynamic_config = DynamicMeasurementsConfig {
            project: Some(&measurements_config),
            global: None,
        };

        // Just for clarity.
        // Checks that there is 1 measurement before processing.
        assert_eq!(measurements.len(), 1);

        remove_invalid_measurements(
            &mut measurements,
            &mut meta,
            dynamic_config,
            max_name_and_unit_len,
        );

        // Checks whether the measurement is dropped.
        measurements.len() == 0
    }

    #[test]
    fn test_normalize_app_start_measurements_does_not_add_measurements() {
        let mut measurements = Annotated::<Measurements>::from_json(r###"{}"###)
            .unwrap()
            .into_value()
            .unwrap();
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {},
        )
        "###);
        normalize_app_start_measurements(&mut measurements);
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {},
        )
        "###);
    }

    #[test]
    fn test_normalize_app_start_cold_measurements() {
        let mut measurements =
            Annotated::<Measurements>::from_json(r#"{"app.start.cold": {"value": 1.1}}"#)
                .unwrap()
                .into_value()
                .unwrap();
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "app.start.cold": Measurement {
                    value: 1.1,
                    unit: ~,
                },
            },
        )
        "###);
        normalize_app_start_measurements(&mut measurements);
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "app_start_cold": Measurement {
                    value: 1.1,
                    unit: ~,
                },
            },
        )
        "###);
    }

    #[test]
    fn test_normalize_app_start_warm_measurements() {
        let mut measurements =
            Annotated::<Measurements>::from_json(r#"{"app.start.warm": {"value": 1.1}}"#)
                .unwrap()
                .into_value()
                .unwrap();
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "app.start.warm": Measurement {
                    value: 1.1,
                    unit: ~,
                },
            },
        )
        "###);
        normalize_app_start_measurements(&mut measurements);
        insta::assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "app_start_warm": Measurement {
                    value: 1.1,
                    unit: ~,
                },
            },
        )
        "###);
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
                other: {},
            },
        ]
        "###);
    }
}
