use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;

use std::hash::{Hash, Hasher};
use std::mem;

use chrono::{DateTime, Duration, Utc};
use relay_base_schema::metrics::{is_valid_metric_name, DurationUnit, FractionUnit, MetricUnit};
use relay_event_schema::processor::{
    self, MaxChars, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::{
    AsPair, Context, ContextInner, Contexts, DeviceClass, Event, EventType, Exception, Headers,
    IpAddr, LogEntry, Measurement, Measurements, NelContext, Request, SpanAttribute, SpanStatus,
    Tags, TraceContext, User,
};
use relay_protocol::{Annotated, Empty, Error, ErrorKind, Meta, Object, Value};
use smallvec::SmallVec;

use crate::normalize::{mechanism, stacktrace};
use crate::span::tag_extraction::{self, extract_span_tags};
use crate::timestamp::TimestampProcessor;
use crate::utils::{self, MAX_DURATION_MOBILE_MS};
use crate::{
    breakdowns, schema, span, transactions, trimming, user_agent, BreakdownsConfig,
    ClockDriftProcessor, DynamicMeasurementsConfig, GeoIpLookup, NormalizeProcessorConfig,
    PerformanceScoreConfig, RawUserAgentInfo,
};

pub(crate) fn normalize(
    event: &mut Event,
    meta: &mut Meta,
    config: &NormalizeProcessorConfig,
) -> ProcessingResult {
    if config.is_renormalize {
        return Ok(());
    }

    // Validate and normalize transaction
    // (internally noops for non-transaction events).
    // TODO: Parts of this processor should probably be a filter so we
    // can revert some changes to ProcessingAction)
    let mut transactions_processor = transactions::TransactionsProcessor::new(
        config.transaction_name_config.clone(),
        config.transaction_range.clone(),
    );
    transactions_processor.process_event(event, meta, ProcessingState::root())?;

    // Check for required and non-empty values
    schema::SchemaProcessor.process_event(event, meta, ProcessingState::root())?;

    TimestampProcessor.process_event(event, meta, ProcessingState::root())?;

    // Process security reports first to ensure all props.
    normalize_security_report(event, config.client_ip, &config.user_agent);

    // Process NEL reports to ensure all props.
    normalize_nel_report(event, config.client_ip);

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
    normalize_measurements(
        event,
        config.measurements.clone(),
        config.max_name_and_unit_len,
    ); // Measurements are part of the metric extraction
    normalize_performance_score(event, config.performance_score);
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
        span::attributes::normalize_spans(event, &BTreeSet::from([SpanAttribute::ExclusiveTime]));
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
        trimming::TrimmingProcessor::new().process_event(event, meta, ProcessingState::root())?;
    }

    Ok(())
}

/// Backfills the client IP address on for the NEL reports.
fn normalize_nel_report(event: &mut Event, client_ip: Option<&IpAddr>) {
    if event.context::<NelContext>().is_none() {
        return;
    }

    if let Some(client_ip) = client_ip {
        let user = event.user.value_mut().get_or_insert_with(User::default);
        user.ip_address = Annotated::new(client_ip.to_owned());
    }
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

fn is_security_report(event: &Event) -> bool {
    event.csp.value().is_some()
        || event.expectct.value().is_some()
        || event.expectstaple.value().is_some()
        || event.hpkp.value().is_some()
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

/// Sets the user's GeoIp info based on user's IP address.
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

fn normalize_logentry(logentry: &mut Annotated<LogEntry>, _meta: &mut Meta) -> ProcessingResult {
    processor::apply(logentry, crate::normalize::logentry::normalize_logentry)
}

/// Ensures that the `release` and `dist` fields match up.
fn normalize_release_dist(event: &mut Event) -> ProcessingResult {
    normalize_dist(&mut event.dist)
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

fn normalize_user_agent(_event: &mut Event, normalize_user_agent: Option<bool>) {
    if normalize_user_agent.unwrap_or(false) {
        user_agent::normalize_user_agent(_event);
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
        normalize_mobile_measurements(measurements);
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

/// Computes performance score measurements.
///
/// This computes score from vital measurements, using config options to define how it is
/// calculated.
fn normalize_performance_score(
    event: &mut Event,
    performance_score: Option<&PerformanceScoreConfig>,
) {
    let Some(performance_score) = performance_score else {
        return;
    };
    for profile in &performance_score.profiles {
        if let Some(condition) = &profile.condition {
            if !condition.matches(event) {
                continue;
            }
            if let Some(measurements) = event.measurements.value_mut() {
                let mut should_add_total = false;
                if profile.score_components.iter().any(|c| {
                    !measurements.contains_key(c.measurement.as_str())
                        && c.weight.abs() >= f64::EPSILON
                }) {
                    // All measurements with a profile weight greater than 0 are required to exist
                    // on the event. Skip calculating performance scores if a measurement with
                    // weight is missing.
                    break;
                }
                let mut score_total = 0.0;
                let mut weight_total = 0.0;
                for component in &profile.score_components {
                    weight_total += component.weight;
                }
                for component in &profile.score_components {
                    let normalized_component_weight = component.weight / weight_total;
                    if let Some(value) = measurements.get_value(component.measurement.as_str()) {
                        let cdf = utils::calculate_cdf_score(value, component.p10, component.p50);
                        let component_score = cdf * normalized_component_weight;
                        score_total += component_score;
                        should_add_total = true;

                        measurements.insert(
                            format!("score.{}", component.measurement),
                            Measurement {
                                value: component_score.into(),
                                unit: (MetricUnit::Fraction(FractionUnit::Ratio)).into(),
                            }
                            .into(),
                        );
                    }
                    measurements.insert(
                        format!("score.weight.{}", component.measurement),
                        Measurement {
                            value: normalized_component_weight.into(),
                            unit: (MetricUnit::Fraction(FractionUnit::Ratio)).into(),
                        }
                        .into(),
                    );
                }

                if should_add_total {
                    measurements.insert(
                        "score.total".to_owned(),
                        Measurement {
                            value: score_total.into(),
                            unit: (MetricUnit::Fraction(FractionUnit::Ratio)).into(),
                        }
                        .into(),
                    );
                }
                break; // Measurements have successfully been added, skip any other profiles.
            }
        }
    }
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

/// Emit any breakdowns
fn normalize_breakdowns(event: &mut Event, breakdowns_config: Option<&BreakdownsConfig>) {
    match breakdowns_config {
        None => {}
        Some(config) => breakdowns::normalize_breakdowns(event, config),
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

/// Normalizes incoming contexts for the downstream metric extraction.
fn normalize_contexts(contexts: &mut Contexts, _: &mut Meta) -> ProcessingResult {
    for annotated in &mut contexts.0.values_mut() {
        if let Some(ContextInner(Context::Trace(context))) = annotated.value_mut() {
            normalize_trace_context(context)?
        }
    }

    Ok(())
}

/// New SDKs do not send measurements when they exceed 180 seconds.
///
/// Drop those outlier measurements for older SDKs.
fn filter_mobile_outliers(measurements: &mut Measurements) {
    for key in [
        "app_start_cold",
        "app_start_warm",
        "time_to_initial_display",
        "time_to_full_display",
    ] {
        if let Some(value) = measurements.get_value(key) {
            if value > MAX_DURATION_MOBILE_MS {
                measurements.remove(key);
            }
        }
    }
}

fn normalize_mobile_measurements(measurements: &mut Measurements) {
    normalize_app_start_measurements(measurements);
    filter_mobile_outliers(measurements);
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

fn normalize_trace_context(context: &mut TraceContext) -> ProcessingResult {
    context.status.get_or_insert_with(|| SpanStatus::Unknown);
    Ok(())
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
        "time_to_full_display" => Some(MetricUnit::Duration(DurationUnit::MilliSecond)),

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
