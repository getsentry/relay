//! Event normalization processor.
//!
//! This processor is work in progress. The intention is to have a single
//! processor to deal with all event normalization. Currently, the normalization
//! logic is split across several processing steps running at different times
//! and under different conditions, like light normalization and store
//! processing. Having a single processor will make things simpler.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeSet;

use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::Range;

use chrono::{DateTime, Duration, Utc};
use relay_base_schema::metrics::{is_valid_metric_name, DurationUnit, FractionUnit, MetricUnit};
use relay_common::time::UnixTimestamp;
use relay_event_schema::processor::{
    self, MaxChars, ProcessValue, ProcessingAction, ProcessingResult, ProcessingState, Processor,
};
use relay_event_schema::protocol::{
    AsPair, Context, ContextInner, Contexts, DeviceClass, Event, EventType, Exception, Headers,
    IpAddr, LogEntry, Measurement, Measurements, NelContext, Request, Span, SpanAttribute,
    SpanStatus, Tags, TraceContext, User,
};
use relay_protocol::{Annotated, Array, Empty, Error, ErrorKind, Meta, Object, Value};
use smallvec::SmallVec;

use crate::normalize::utils::validate_span;
use crate::normalize::{mechanism, stacktrace};
use crate::span::tag_extraction::{self, extract_span_tags};
use crate::timestamp::TimestampProcessor;
use crate::utils::{self, MAX_DURATION_MOBILE_MS};
use crate::{
    breakdowns, end_all_spans, normalize_transaction_name, set_default_transaction_source, span,
    trimming, user_agent, validate_transaction, BreakdownsConfig, ClockDriftProcessor,
    DynamicMeasurementsConfig, GeoIpLookup, PerformanceScoreConfig, RawUserAgentInfo,
    SpanDescriptionRule, TransactionNameConfig,
};

/// Configuration for [`NormalizeProcessor`].
#[derive(Clone, Debug)]
pub struct NormalizeProcessorConfig<'a> {
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

    /// Timestamp range in which a transaction must end.
    ///
    /// Transactions that finish outside of this range are considered invalid.
    /// This check is skipped if no range is provided.
    pub transaction_range: Option<Range<UnixTimestamp>>,

    /// The maximum length for names of custom measurements.
    ///
    /// Measurements with longer names are removed from the transaction event and replaced with a
    /// metadata entry.
    pub max_name_and_unit_len: Option<usize>,

    /// Configuration for measurement normalization in transaction events.
    ///
    /// Has an optional [`crate::MeasurementsConfig`] from both the project and the global level.
    /// If at least one is provided, then normalization will truncate custom measurements
    /// and add units of known built-in measurements.
    pub measurements: Option<DynamicMeasurementsConfig<'a>>,

    /// Emit breakdowns based on given configuration.
    pub breakdowns_config: Option<&'a BreakdownsConfig>,

    /// When `Some(true)`, context information is extracted from the user agent.
    pub normalize_user_agent: Option<bool>,

    /// Configuration to apply to transaction names, especially around sanitizing.
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

    /// Configuration for generating performance score measurements for web vitals
    pub performance_score: Option<&'a PerformanceScoreConfig>,

    /// An initialized GeoIP lookup.
    pub geoip_lookup: Option<&'a GeoIpLookup>,

    /// When `Some(true)`, individual parts of the event payload is trimmed to a maximum size.
    ///
    /// See the event schema for size declarations.
    pub enable_trimming: bool,
}

impl<'a> Default for NormalizeProcessorConfig<'a> {
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
            performance_score: Default::default(),
            geoip_lookup: Default::default(),
            enable_trimming: false,
            measurements: None,
        }
    }
}

/// Normalizes an event, rejecting it if necessary.
///
/// The normalization consists of applying a series of transformations on the
/// event payload based on the given configuration.
///
/// The returned [`ProcessingResult`] indicates whether the passed event should
/// be ingested or dropped.
#[derive(Debug, Default)]
pub struct NormalizeProcessor<'a> {
    /// Configuration for the normalization steps.
    config: NormalizeProcessorConfig<'a>,
}

impl<'a> NormalizeProcessor<'a> {
    /// Returns a new [`NormalizeProcessor`] with the given config.
    pub fn new(config: NormalizeProcessorConfig<'a>) -> Self {
        Self { config }
    }
}

impl<'a> Processor for NormalizeProcessor<'a> {
    fn process_event(
        &mut self,
        event: &mut Event,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if self.config.is_renormalize {
            return Ok(());
        }

        if event.ty.value() == Some(&EventType::Transaction) {
            // TODO: Parts of this processor should probably be a filter so we
            // can revert some changes to ProcessingAction)

            validate_transaction(event, self.config.transaction_range.as_ref())?;

            if let Some(trace_context) = event.context_mut::<TraceContext>() {
                trace_context.op.get_or_insert_with(|| "default".to_owned());
            }

            // The transaction name is expected to be non-empty by downstream services (e.g. Snuba), but
            // Relay doesn't reject events missing the transaction name. Instead, a default transaction
            // name is given, similar to how Sentry gives an "<unlabeled event>" title to error events.
            // SDKs should avoid sending empty transaction names, setting a more contextual default
            // value when possible.
            if event.transaction.value().map_or(true, |s| s.is_empty()) {
                event
                    .transaction
                    .set_value(Some("<unlabeled transaction>".to_owned()))
            }
            set_default_transaction_source(event);
            normalize_transaction_name(event, &self.config.transaction_name_config)?;
            end_all_spans(event)?;
        }

        // XXX(iker): processing child values should be the last step. The logic
        // below this call is being moved (WIP) to the processor appropriately.
        event.process_child_values(self, state)?;

        if self.config.is_renormalize {
            return Ok(());
        }

        TimestampProcessor.process_event(event, meta, ProcessingState::root())?;

        // Process security reports first to ensure all props.
        normalize_security_report(event, self.config.client_ip, &self.config.user_agent);

        // Process NEL reports to ensure all props.
        normalize_nel_report(event, self.config.client_ip);

        // Insert IP addrs before recursing, since geo lookup depends on it.
        normalize_ip_addresses(
            &mut event.request,
            &mut event.user,
            event.platform.as_str(),
            self.config.client_ip,
        );

        if let Some(geoip_lookup) = self.config.geoip_lookup {
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
            self.config.received_at,
            self.config.max_secs_in_past,
            self.config.max_secs_in_future,
        )?; // Timestamps are core in the metrics extraction
        normalize_event_tags(event)?; // Tags are added to every metric

        // TODO: Consider moving to store normalization
        if self.config.device_class_synthesis_config {
            normalize_device_class(event);
        }
        light_normalize_stacktraces(event)?;
        normalize_exceptions(event)?; // Browser extension filters look at the stacktrace
        normalize_user_agent(event, self.config.normalize_user_agent); // Legacy browsers filter
        normalize_measurements(
            event,
            self.config.measurements.clone(),
            self.config.max_name_and_unit_len,
        ); // Measurements are part of the metric extraction
        normalize_performance_score(event, self.config.performance_score);
        normalize_breakdowns(event, self.config.breakdowns_config); // Breakdowns are part of the metric extraction too

        // Some contexts need to be normalized before metrics extraction takes place.
        processor::apply(&mut event.contexts, normalize_contexts)?;

        if self.config.light_normalize_spans && event.ty.value() == Some(&EventType::Transaction) {
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

        if self.config.enrich_spans {
            extract_span_tags(
                event,
                &tag_extraction::Config {
                    max_tag_value_size: self.config.max_tag_value_length,
                },
            );
        }

        if self.config.enable_trimming {
            // Trim large strings and databags down
            trimming::TrimmingProcessor::new().process_event(
                event,
                meta,
                ProcessingState::root(),
            )?;
        }

        Ok(())
    }

    fn process_span(
        &mut self,
        span: &mut Span,
        _: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        validate_span(span)?;
        span.op.get_or_insert_with(|| "default".to_owned());

        span.process_child_values(self, state)?;

        Ok(())
    }

    fn before_process<T: ProcessValue>(
        &mut self,
        value: Option<&T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        if value.is_none() && state.attrs().required && !meta.has_errors() {
            meta.add_error(ErrorKind::MissingAttribute);
        }
        Ok(())
    }

    fn process_string(
        &mut self,
        value: &mut String,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult {
        value_trim_whitespace(value, meta, state);
        verify_value_nonempty_string(value, meta, state)?;
        verify_value_characters(value, meta, state)?;
        Ok(())
    }

    fn process_array<T>(
        &mut self,
        value: &mut Array<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        T: ProcessValue,
    {
        value.process_child_values(self, state)?;
        verify_value_nonempty(value, meta, state)?;
        Ok(())
    }

    fn process_object<T>(
        &mut self,
        value: &mut Object<T>,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ProcessingResult
    where
        T: ProcessValue,
    {
        value.process_child_values(self, state)?;
        verify_value_nonempty(value, meta, state)?;
        Ok(())
    }
}

fn value_trim_whitespace(value: &mut String, _meta: &mut Meta, state: &ProcessingState<'_>) {
    if state.attrs().trim_whitespace {
        let new_value = value.trim().to_owned();
        value.clear();
        value.push_str(&new_value);
    }
}

fn verify_value_nonempty<T>(
    value: &T,
    meta: &mut Meta,
    state: &ProcessingState<'_>,
) -> ProcessingResult
where
    T: Empty,
{
    if state.attrs().nonempty && value.is_empty() {
        meta.add_error(Error::nonempty());
        Err(ProcessingAction::DeleteValueHard)
    } else {
        Ok(())
    }
}

fn verify_value_nonempty_string<T>(
    value: &T,
    meta: &mut Meta,
    state: &ProcessingState<'_>,
) -> ProcessingResult
where
    T: Empty,
{
    if state.attrs().nonempty && value.is_empty() {
        meta.add_error(Error::nonempty_string());
        Err(ProcessingAction::DeleteValueHard)
    } else {
        Ok(())
    }
}

fn verify_value_characters(
    value: &str,
    meta: &mut Meta,
    state: &ProcessingState<'_>,
) -> ProcessingResult {
    if let Some(ref character_set) = state.attrs().characters {
        for c in value.chars() {
            if !(character_set.char_is_valid)(c) {
                meta.add_error(Error::invalid(format!("invalid character {c:?}")));
                return Err(ProcessingAction::DeleteValueSoft);
            }
        }
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
                                value: cdf.into(),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::{Duration, TimeZone, Utc};
    use insta::assert_debug_snapshot;
    use itertools::Itertools;
    use relay_base_schema::events::EventType;
    use relay_base_schema::metrics::{DurationUnit, MetricUnit};
    use relay_base_schema::spans::SpanStatus;
    use relay_common::glob2::LazyGlob;
    use relay_common::time::UnixTimestamp;
    use relay_event_schema::processor::{
        self, process_value, ProcessingAction, ProcessingState, Processor,
    };
    use relay_event_schema::protocol::{
        CError, ClientSdkInfo, Contexts, Csp, DeviceContext, Event, Headers, IpAddr, MachException,
        Measurement, Measurements, Mechanism, MechanismMeta, PosixSignal, RawStacktrace, Request,
        Span, SpanId, Tags, TraceContext, TraceId, TransactionSource, User,
    };
    use relay_protocol::{get_value, Annotated, ErrorKind, Meta, Object, SerializableAnnotated};
    use serde_json::json;

    use crate::normalize::processor::{
        filter_mobile_outliers, normalize_app_start_measurements, normalize_device_class,
        normalize_dist, normalize_measurements, normalize_performance_score,
        normalize_security_report, normalize_units, remove_invalid_measurements,
        NormalizeProcessor, NormalizeProcessorConfig,
    };
    use crate::{
        scrub_identifiers, ClientHints, DynamicMeasurementsConfig, MeasurementsConfig,
        PerformanceScoreConfig, RawUserAgentInfo, RedactionRule, TransactionNameConfig,
        TransactionNameRule,
    };

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
    fn test_filter_mobile_outliers() {
        let mut measurements =
            Annotated::<Measurements>::from_json(r#"{"app_start_warm": {"value": 180001}}"#)
                .unwrap()
                .into_value()
                .unwrap();
        assert_eq!(measurements.len(), 1);
        filter_mobile_outliers(&mut measurements);
        assert_eq!(measurements.len(), 0);
    }

    #[test]
    fn test_span_default_op_when_missing() {
        let json = r#"{
  "start_timestamp": 1,
  "timestamp": 2,
  "span_id": "fa90fdead5f74052",
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
}"#;
        let mut span = Annotated::<Span>::from_json(json).unwrap();

        process_value(
            &mut span,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();
        assert_eq!(get_value!(span.op).unwrap(), &"default".to_owned());
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_span_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
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
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing span_id"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
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
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing start_timestamp"
            ))
        );
    }

    #[test]
    fn test_discards_transaction_event_with_span_with_missing_trace_id() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
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
            spans: Annotated::new(vec![Annotated::new(Span {
                timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                start_timestamp: Annotated::new(
                    Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
                ),
                ..Default::default()
            })]),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "span is missing trace_id"
            ))
        );
    }

    #[test]
    fn test_renormalize_spans_is_idempotent() {
        let json = r#"{
  "start_timestamp": 1,
  "timestamp": 2,
  "span_id": "fa90fdead5f74052",
  "trace_id": "4c79f60c11214eb38604f4ae0781bfb2"
}"#;

        let mut processed = Annotated::<Span>::from_json(json).unwrap();
        let processor_config = NormalizeProcessorConfig::default();
        let mut processor = NormalizeProcessor::new(processor_config.clone());
        process_value(&mut processed, &mut processor, ProcessingState::root()).unwrap();
        assert_eq!(get_value!(processed.op!), &"default".to_owned());

        let mut reprocess_config = processor_config.clone();
        reprocess_config.is_renormalize = true;
        let mut processor = NormalizeProcessor::new(processor_config.clone());

        let mut reprocessed = processed.clone();
        process_value(&mut reprocessed, &mut processor, ProcessingState::root()).unwrap();
        assert_eq!(processed, reprocessed);

        let mut reprocessed2 = reprocessed.clone();
        process_value(&mut reprocessed2, &mut processor, ProcessingState::root()).unwrap();
        assert_eq!(reprocessed, reprocessed2);
    }

    #[test]
    fn test_renormalize_transactions_is_idempotent() {
        let json = r#"{
  "event_id": "52df9022835246eeb317dbd739ccd059",
  "type": "transaction",
  "transaction": "test-transaction",
  "start_timestamp": 1,
  "timestamp": 2,
  "contexts": {
    "trace": {
      "trace_id": "ff62a8b040f340bda5d830223def1d81",
      "span_id": "bd429c44b67a3eb4"
    }
  }
}"#;

        let mut processed = Annotated::<Event>::from_json(json).unwrap();
        let processor_config = NormalizeProcessorConfig::default();
        let mut processor = NormalizeProcessor::new(processor_config.clone());
        process_value(&mut processed, &mut processor, ProcessingState::root()).unwrap();
        remove_received_from_event(&mut processed);
        let trace_context = get_value!(processed!).context::<TraceContext>().unwrap();
        assert_eq!(trace_context.op.value().unwrap(), "default");

        let mut reprocess_config = processor_config.clone();
        reprocess_config.is_renormalize = true;
        let mut processor = NormalizeProcessor::new(processor_config.clone());

        let mut reprocessed = processed.clone();
        process_value(&mut reprocessed, &mut processor, ProcessingState::root()).unwrap();
        remove_received_from_event(&mut reprocessed);
        assert_eq!(processed, reprocessed);

        let mut reprocessed2 = reprocessed.clone();
        process_value(&mut reprocessed2, &mut processor, ProcessingState::root()).unwrap();
        remove_received_from_event(&mut reprocessed2);
        assert_eq!(reprocessed, reprocessed2);
    }

    fn remove_received_from_event(event: &mut Annotated<Event>) {
        processor::apply(event, |e, _| {
            e.received = Annotated::empty();
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_computed_performance_score() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "fid": {"value": 213, "unit": "millisecond"},
                "fcp": {"value": 1237, "unit": "millisecond"},
                "lcp": {"value": 6596, "unit": "millisecond"},
                "cls": {"value": 0.11}
            },
            "contexts": {
                "browser": {
                    "name": "Chrome",
                    "version": "120.1.1",
                    "type": "browser"
                }
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let performance_score: PerformanceScoreConfig = serde_json::from_value(json!({
            "profiles": [
                {
                    "name": "Desktop",
                    "scoreComponents": [
                        {
                            "measurement": "fcp",
                            "weight": 0.15,
                            "p10": 900,
                            "p50": 1600
                        },
                        {
                            "measurement": "lcp",
                            "weight": 0.30,
                            "p10": 1200,
                            "p50": 2400
                        },
                        {
                            "measurement": "fid",
                            "weight": 0.30,
                            "p10": 100,
                            "p50": 300
                        },
                        {
                            "measurement": "cls",
                            "weight": 0.25,
                            "p10": 0.1,
                            "p50": 0.25
                        },
                        {
                            "measurement": "ttfb",
                            "weight": 0.0,
                            "p10": 0.2,
                            "p50": 0.4
                        },
                    ],
                    "condition": {
                        "op":"eq",
                        "name": "event.contexts.browser.name",
                        "value": "Chrome"
                    }
                }
            ]
        }))
        .unwrap();

        normalize_performance_score(&mut event, Some(&performance_score));

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "contexts": {
            "browser": {
              "name": "Chrome",
              "version": "120.1.1",
              "type": "browser",
            },
          },
          "measurements": {
            "cls": {
              "value": 0.11,
            },
            "fcp": {
              "value": 1237.0,
              "unit": "millisecond",
            },
            "fid": {
              "value": 213.0,
              "unit": "millisecond",
            },
            "lcp": {
              "value": 6596.0,
              "unit": "millisecond",
            },
            "score.cls": {
              "value": 0.8745668242977945,
              "unit": "ratio",
            },
            "score.fcp": {
              "value": 0.7167236962527221,
              "unit": "ratio",
            },
            "score.fid": {
              "value": 0.6552453782760849,
              "unit": "ratio",
            },
            "score.lcp": {
              "value": 0.03079632190462195,
              "unit": "ratio",
            },
            "score.total": {
              "value": 0.531962770566569,
              "unit": "ratio",
            },
            "score.weight.cls": {
              "value": 0.25,
              "unit": "ratio",
            },
            "score.weight.fcp": {
              "value": 0.15,
              "unit": "ratio",
            },
            "score.weight.fid": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.lcp": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.ttfb": {
              "value": 0.0,
              "unit": "ratio",
            },
          },
        }
        "###);
    }

    // Test performance score is calculated correctly when the sum of weights is under 1.
    // The expected result should normalize the weights to a sum of 1 and scale the weight measurements accordingly.
    #[test]
    fn test_computed_performance_score_with_under_normalized_weights() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "fid": {"value": 213, "unit": "millisecond"},
                "fcp": {"value": 1237, "unit": "millisecond"},
                "lcp": {"value": 6596, "unit": "millisecond"},
                "cls": {"value": 0.11}
            },
            "contexts": {
                "browser": {
                    "name": "Chrome",
                    "version": "120.1.1",
                    "type": "browser"
                }
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let performance_score: PerformanceScoreConfig = serde_json::from_value(json!({
            "profiles": [
                {
                    "name": "Desktop",
                    "scoreComponents": [
                        {
                            "measurement": "fcp",
                            "weight": 0.03,
                            "p10": 900,
                            "p50": 1600
                        },
                        {
                            "measurement": "lcp",
                            "weight": 0.06,
                            "p10": 1200,
                            "p50": 2400
                        },
                        {
                            "measurement": "fid",
                            "weight": 0.06,
                            "p10": 100,
                            "p50": 300
                        },
                        {
                            "measurement": "cls",
                            "weight": 0.05,
                            "p10": 0.1,
                            "p50": 0.25
                        },
                        {
                            "measurement": "ttfb",
                            "weight": 0.0,
                            "p10": 0.2,
                            "p50": 0.4
                        },
                    ],
                    "condition": {
                        "op":"eq",
                        "name": "event.contexts.browser.name",
                        "value": "Chrome"
                    }
                }
            ]
        }))
        .unwrap();

        normalize_performance_score(&mut event, Some(&performance_score));

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "contexts": {
            "browser": {
              "name": "Chrome",
              "version": "120.1.1",
              "type": "browser",
            },
          },
          "measurements": {
            "cls": {
              "value": 0.11,
            },
            "fcp": {
              "value": 1237.0,
              "unit": "millisecond",
            },
            "fid": {
              "value": 213.0,
              "unit": "millisecond",
            },
            "lcp": {
              "value": 6596.0,
              "unit": "millisecond",
            },
            "score.cls": {
              "value": 0.8745668242977945,
              "unit": "ratio",
            },
            "score.fcp": {
              "value": 0.7167236962527221,
              "unit": "ratio",
            },
            "score.fid": {
              "value": 0.6552453782760849,
              "unit": "ratio",
            },
            "score.lcp": {
              "value": 0.03079632190462195,
              "unit": "ratio",
            },
            "score.total": {
              "value": 0.531962770566569,
              "unit": "ratio",
            },
            "score.weight.cls": {
              "value": 0.25,
              "unit": "ratio",
            },
            "score.weight.fcp": {
              "value": 0.15,
              "unit": "ratio",
            },
            "score.weight.fid": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.lcp": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.ttfb": {
              "value": 0.0,
              "unit": "ratio",
            },
          },
        }
        "###);
    }

    // Test performance score is calculated correctly when the sum of weights is over 1.
    // The expected result should normalize the weights to a sum of 1 and scale the weight measurements accordingly.
    #[test]
    fn test_computed_performance_score_with_over_normalized_weights() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "fid": {"value": 213, "unit": "millisecond"},
                "fcp": {"value": 1237, "unit": "millisecond"},
                "lcp": {"value": 6596, "unit": "millisecond"},
                "cls": {"value": 0.11}
            },
            "contexts": {
                "browser": {
                    "name": "Chrome",
                    "version": "120.1.1",
                    "type": "browser"
                }
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let performance_score: PerformanceScoreConfig = serde_json::from_value(json!({
            "profiles": [
                {
                    "name": "Desktop",
                    "scoreComponents": [
                        {
                            "measurement": "fcp",
                            "weight": 0.30,
                            "p10": 900,
                            "p50": 1600
                        },
                        {
                            "measurement": "lcp",
                            "weight": 0.60,
                            "p10": 1200,
                            "p50": 2400
                        },
                        {
                            "measurement": "fid",
                            "weight": 0.60,
                            "p10": 100,
                            "p50": 300
                        },
                        {
                            "measurement": "cls",
                            "weight": 0.50,
                            "p10": 0.1,
                            "p50": 0.25
                        },
                        {
                            "measurement": "ttfb",
                            "weight": 0.0,
                            "p10": 0.2,
                            "p50": 0.4
                        },
                    ],
                    "condition": {
                        "op":"eq",
                        "name": "event.contexts.browser.name",
                        "value": "Chrome"
                    }
                }
            ]
        }))
        .unwrap();

        normalize_performance_score(&mut event, Some(&performance_score));

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "contexts": {
            "browser": {
              "name": "Chrome",
              "version": "120.1.1",
              "type": "browser",
            },
          },
          "measurements": {
            "cls": {
              "value": 0.11,
            },
            "fcp": {
              "value": 1237.0,
              "unit": "millisecond",
            },
            "fid": {
              "value": 213.0,
              "unit": "millisecond",
            },
            "lcp": {
              "value": 6596.0,
              "unit": "millisecond",
            },
            "score.cls": {
              "value": 0.8745668242977945,
              "unit": "ratio",
            },
            "score.fcp": {
              "value": 0.7167236962527221,
              "unit": "ratio",
            },
            "score.fid": {
              "value": 0.6552453782760849,
              "unit": "ratio",
            },
            "score.lcp": {
              "value": 0.03079632190462195,
              "unit": "ratio",
            },
            "score.total": {
              "value": 0.531962770566569,
              "unit": "ratio",
            },
            "score.weight.cls": {
              "value": 0.25,
              "unit": "ratio",
            },
            "score.weight.fcp": {
              "value": 0.15,
              "unit": "ratio",
            },
            "score.weight.fid": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.lcp": {
              "value": 0.3,
              "unit": "ratio",
            },
            "score.weight.ttfb": {
              "value": 0.0,
              "unit": "ratio",
            },
          },
        }
        "###);
    }

    #[test]
    fn test_computed_performance_score_missing_measurement() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "a": {"value": 213, "unit": "millisecond"}
            },
            "contexts": {
                "browser": {
                    "name": "Chrome",
                    "version": "120.1.1",
                    "type": "browser"
                }
            }
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        let performance_score: PerformanceScoreConfig = serde_json::from_value(json!({
            "profiles": [
                {
                    "name": "Desktop",
                    "scoreComponents": [
                        {
                            "measurement": "a",
                            "weight": 0.15,
                            "p10": 900,
                            "p50": 1600
                        },
                        {
                            "measurement": "b",
                            "weight": 0.30,
                            "p10": 1200,
                            "p50": 2400
                        },
                    ],
                    "condition": {
                        "op":"eq",
                        "name": "event.contexts.browser.name",
                        "value": "Chrome"
                    }
                }
            ]
        }))
        .unwrap();

        normalize_performance_score(&mut event, Some(&performance_score));

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "contexts": {
            "browser": {
              "name": "Chrome",
              "version": "120.1.1",
              "type": "browser",
            },
          },
          "measurements": {
            "a": {
              "value": 213.0,
              "unit": "millisecond",
            },
          },
        }
        "###);
    }

    fn new_test_event() -> Annotated<Event> {
        let start = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 10).unwrap();
        Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            transaction: Annotated::new("/".to_owned()),
            start_timestamp: Annotated::new(start.into()),
            timestamp: Annotated::new(end.into()),
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
                start_timestamp: Annotated::new(start.into()),
                timestamp: Annotated::new(end.into()),
                trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
                op: Annotated::new("db.statement".to_owned()),
                ..Default::default()
            })]),
            ..Default::default()
        })
    }

    #[test]
    fn test_skips_non_transaction_events() {
        let mut event = Annotated::new(Event::default());
        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();
        assert!(event.value().is_some());
    }

    #[test]
    fn test_discards_when_missing_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "timestamp hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_when_timestamp_out_of_range() {
        let mut event = new_test_event();

        let processor = &mut NormalizeProcessor::new(NormalizeProcessorConfig {
            transaction_range: Some(UnixTimestamp::now()..UnixTimestamp::now()),
            ..Default::default()
        });

        assert!(matches!(
            process_value(&mut event, processor, ProcessingState::root()),
            Err(ProcessingAction::InvalidTransaction(
                "timestamp is out of the valid range for metrics"
            ))
        ));
    }

    #[test]
    fn test_replace_missing_timestamp() {
        let span = Span {
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 1).unwrap().into(),
            ),
            trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
            span_id: Annotated::new(SpanId("fa90fdead5f74053".into())),
            ..Default::default()
        };

        let mut event = new_test_event().0.unwrap();
        event.spans = Annotated::new(vec![Annotated::new(span)]);

        NormalizeProcessor::default()
            .process_event(
                &mut event,
                &mut Meta::default(),
                &ProcessingState::default(),
            )
            .unwrap();

        let spans = event.spans;
        let span = get_value!(spans[0]!);

        assert_eq!(span.timestamp, event.timestamp);
        assert_eq!(span.status.value().unwrap(), &SpanStatus::DeadlineExceeded);
    }

    #[test]
    fn test_discards_when_missing_start_timestamp() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "start_timestamp hard-required for transaction events"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_contexts_map() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: Annotated::new(Contexts::new()),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
            ))
        );
    }

    #[test]
    fn test_discards_on_null_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: Annotated::new(Contexts({
                let mut contexts = Object::new();
                contexts.insert("trace".to_owned(), Annotated::empty());
                contexts
            })),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "missing valid trace context"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_trace_id_in_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext::default());
                Annotated::new(contexts)
            },
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "trace context is missing trace_id"
            ))
        );
    }

    #[test]
    fn test_discards_on_missing_span_id_in_context() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
            ),
            contexts: {
                let mut contexts = Contexts::new();
                contexts.add(TraceContext {
                    trace_id: Annotated::new(TraceId("4c79f60c11214eb38604f4ae0781bfb2".into())),
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "trace context is missing span_id"
            ))
        );
    }

    #[test]
    fn test_defaults_missing_op_in_context() {
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
                    ..Default::default()
                });
                Annotated::new(contexts)
            },
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        let trace_context = get_value!(event.contexts)
            .unwrap()
            .get::<TraceContext>()
            .unwrap();
        let trace_op = trace_context.op.value().unwrap();
        assert_eq!(trace_op, "default");
    }

    #[test]
    fn test_allows_transaction_event_without_span_list() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
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
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();
        assert!(event.value().is_some());
    }

    #[test]
    fn test_allows_transaction_event_with_empty_span_list() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
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
            spans: Annotated::new(vec![]),
            ..Default::default()
        });

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();
        assert!(event.value().is_some());
    }

    #[test]
    fn test_allows_transaction_event_with_null_span_list() {
        let mut event = new_test_event();

        processor::apply(&mut event, |event, _| {
            event.spans.set_value(None);
            Ok(())
        })
        .unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();
        assert!(get_value!(event.spans).unwrap().is_empty());
    }

    #[test]
    fn test_discards_transaction_event_with_nulled_out_span() {
        let mut event = Annotated::new(Event {
            ty: Annotated::new(EventType::Transaction),
            timestamp: Annotated::new(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into()),
            start_timestamp: Annotated::new(
                Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().into(),
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
            spans: Annotated::new(vec![Annotated::empty()]),
            ..Default::default()
        });

        assert_eq!(
            process_value(
                &mut event,
                &mut NormalizeProcessor::default(),
                ProcessingState::root()
            ),
            Err(ProcessingAction::InvalidTransaction(
                "spans must be valid in transaction event"
            ))
        );
    }

    #[test]
    fn test_default_transaction_source_unknown() {
        let mut event = Annotated::<Event>::from_json(
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
                "sdk": {
                    "name": "sentry.dart.flutter"
                },
                "spans": []
            }
            "#,
        )
        .unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        let source = event
            .value()
            .unwrap()
            .transaction_info
            .value()
            .and_then(|info| info.source.value())
            .unwrap();

        assert_eq!(source, &TransactionSource::Unknown);
    }

    #[test]
    fn test_allows_valid_transaction_event_with_spans() {
        let mut event = new_test_event();

        assert!(process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .is_ok());
    }

    #[test]
    fn test_defaults_transaction_name_when_missing() {
        let mut event = new_test_event();

        processor::apply(&mut event, |event, _| {
            event.transaction.set_value(None);
            Ok(())
        })
        .unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "<unlabeled transaction>");
    }

    #[test]
    fn test_defaults_transaction_name_when_empty() {
        let mut event = new_test_event();

        processor::apply(&mut event, |event, _| {
            event.transaction.set_value(Some("".to_owned()));
            Ok(())
        })
        .unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "<unlabeled transaction>");
    }

    #[test]
    fn test_transaction_name_normalize() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0",
            "transaction_info": {
              "source": "url"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            },
            "sdk": {"name": "sentry.ruby"},
            "modules": {"rack": "1.2.3"}
        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                5,
                45,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                51,
                54,
            ),
        ),
    },
]"#);
    }

    /// When no identifiers are scrubbed, we should not set an original value in _meta.
    #[test]
    fn test_transaction_name_skip_original_value() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/static/page",
            "transaction_info": {
              "source": "url"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            },
            "sdk": {"name": "sentry.ruby"},
            "modules": {"rack": "1.2.3"}
        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert!(event.meta().is_empty());
    }

    #[test]
    fn test_transaction_name_normalize_mark_as_sanitized() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0",
            "transaction_info": {
              "source": "url"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            }

        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::default(),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
    }

    #[test]
    fn test_transaction_name_rename_with_rules() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/rule-target/user/123/0/",
            "transaction_info": {
              "source": "url"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            },
            "sdk": {"name": "sentry.ruby"},
            "modules": {"rack": "1.2.3"}
        }
        "#;

        let rule1 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/user/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };
        let rule2 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };
        // This should not happend, such rules shouldn't be sent to relay at all.
        let rule3 = TransactionNameRule {
            pattern: LazyGlob::new("/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::new(NormalizeProcessorConfig {
                transaction_name_config: TransactionNameConfig {
                    rules: &[rule1, rule2, rule3],
                },
                ..Default::default()
            }),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                22,
                25,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/user/*/**",
        range: None,
    },
]"#);
    }

    #[test]
    fn test_transaction_name_rules_skip_expired() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/rule-target/user/123/0/",
            "transaction_info": {
              "source": "url"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            },
            "sdk": {"name": "sentry.ruby"},
            "modules": {"rack": "1.2.3"}
        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();

        let rule1 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/user/*/**".to_string()),
            expiry: Utc::now() - Duration::hours(1), // Expired rule
            redaction: Default::default(),
        };
        let rule2 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };
        // This should not happend, such rules shouldn't be sent to relay at all.
        let rule3 = TransactionNameRule {
            pattern: LazyGlob::new("/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };

        process_value(
            &mut event,
            &mut NormalizeProcessor::new(NormalizeProcessorConfig {
                transaction_name_config: TransactionNameConfig {
                    rules: &[rule1, rule2, rule3],
                },
                ..Default::default()
            }),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                22,
                25,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/**",
        range: None,
    },
]"#);
    }

    #[test]
    fn test_normalize_twice() {
        // Simulate going through a chain of relays.
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/rule-target/user/123/0/",
            "transaction_info": {
              "source": "url"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request"
                }
            }
        }
        "#;

        let rules = vec![TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/user/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        }];

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        let mut processor = NormalizeProcessor::new(NormalizeProcessorConfig {
            transaction_name_config: TransactionNameConfig { rules: &rules },
            ..Default::default()
        });
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                22,
                25,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/user/*/**",
        range: None,
    },
]"#);

        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        // Process again:
        process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user/*/0/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                22,
                25,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/user/*/**",
        range: None,
    },
]"#);

        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
    }

    #[test]
    fn test_transaction_name_unsupported_source() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0",
            "transaction_info": {
              "source": "foobar"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            }
        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();
        let rule1 = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };
        // This should not happend, such rules shouldn't be sent to relay at all.
        let rule2 = TransactionNameRule {
            pattern: LazyGlob::new("/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };
        let rules = vec![rule1, rule2];

        // This must not normalize transaction name, since it's disabled.
        process_value(
            &mut event,
            &mut NormalizeProcessor::new(NormalizeProcessorConfig {
                transaction_name_config: TransactionNameConfig { rules: &rules },
                ..Default::default()
            }),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(
            get_value!(event.transaction!),
            "/foo/2fd4e1c67a2d28fced849ee1bb76e7391b93eb12/user/123/0"
        );
        assert!(get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .next()
            .is_none());
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "foobar"
        );
    }

    fn run_with_unknown_source(sdk: &str) -> Annotated<Event> {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/user/jane/blog/",
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            }
        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();
        event
            .value_mut()
            .as_mut()
            .unwrap()
            .client_sdk
            .set_value(Some(ClientSdkInfo {
                name: sdk.to_owned().into(),
                ..Default::default()
            }));
        let rules: Vec<TransactionNameRule> = serde_json::from_value(serde_json::json!([
            {"pattern": "/user/*/**", "expiry": "3021-04-26T07:59:01+0100", "redaction": {"method": "replace"}}
        ]))
        .unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::new(NormalizeProcessorConfig {
                transaction_name_config: TransactionNameConfig { rules: &rules },
                ..Default::default()
            }),
            ProcessingState::root(),
        )
        .unwrap();
        event
    }

    #[test]
    fn test_normalize_legacy_javascript() {
        // Javascript without source annotation gets sanitized.
        let event = run_with_unknown_source("sentry.javascript.browser");

        assert_eq!(get_value!(event.transaction!), "/user/*/blog/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "/user/*/**",
        range: None,
    },
]"#);

        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
    }

    #[test]
    fn test_normalize_legacy_python() {
        // Python without source annotation does not get sanitized, because we assume it to be
        // low cardinality.
        let event = run_with_unknown_source("sentry.python");
        assert_eq!(get_value!(event.transaction!), "/user/jane/blog/");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "unknown"
        );
    }

    #[test]
    fn test_transaction_name_rename_end_slash() {
        let json = r#"
        {
            "type": "transaction",
            "transaction": "/foo/rule-target/user",
            "transaction_info": {
              "source": "url"
            },
            "timestamp": "2021-04-26T08:00:00+0100",
            "start_timestamp": "2021-04-26T07:59:01+0100",
            "contexts": {
                "trace": {
                    "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                    "span_id": "fa90fdead5f74053",
                    "op": "rails.request",
                    "status": "ok"
                }
            },
            "sdk": {"name": "sentry.ruby"},
            "modules": {"rack": "1.2.3"}
        }
        "#;

        let rule = TransactionNameRule {
            pattern: LazyGlob::new("/foo/*/**".to_string()),
            expiry: Utc::now() + Duration::hours(1),
            redaction: Default::default(),
        };

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::new(NormalizeProcessorConfig {
                transaction_name_config: TransactionNameConfig { rules: &[rule] },
                ..Default::default()
            }),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/foo/*/user");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "/foo/*/**",
        range: None,
    },
]"#);

        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
    }

    #[test]
    fn test_normalize_transaction_names() {
        let should_be_replaced = [
            "/aaa11111-aa11-11a1-a11a-1aaa1111a111",
            "/1aa111aa-11a1-11aa-a111-a1a11111aa11",
            "/00a00000-0000-0000-0000-000000000001",
            "/test/b25feeaa-ed2d-4132-bcbd-6232b7922add/url",
        ];
        let replaced = should_be_replaced.map(|s| {
            let mut s = Annotated::new(s.to_owned());
            scrub_identifiers(&mut s).unwrap();
            s.0.unwrap()
        });
        assert_eq!(
            replaced,
            ["/*", "/*", "/*", "/test/*/url",].map(str::to_owned)
        )
    }

    macro_rules! transaction_name_test {
        ($name:ident, $input:literal, $output:literal) => {
            #[test]
            fn $name() {
                let json = format!(
                    r#"
                    {{
                        "type": "transaction",
                        "transaction": "{}",
                        "transaction_info": {{
                          "source": "url"
                        }},
                        "timestamp": "2021-04-26T08:00:00+0100",
                        "start_timestamp": "2021-04-26T07:59:01+0100",
                        "contexts": {{
                            "trace": {{
                                "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                                "span_id": "fa90fdead5f74053",
                                "op": "rails.request",
                                "status": "ok"
                            }}
                        }}
                    }}
                "#,
                    $input
                );

                let mut event = Annotated::<Event>::from_json(&json).unwrap();

                process_value(
                    &mut event,
                    &mut NormalizeProcessor::default(),
                    ProcessingState::root(),
                )
                .unwrap();

                assert_eq!($output, event.value().unwrap().transaction.value().unwrap());
            }
        };
    }

    transaction_name_test!(test_transaction_name_normalize_id, "/1234", "/*");
    transaction_name_test!(
        test_transaction_name_normalize_in_segments_1,
        "/user/path-with-1234/",
        "/user/*/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_in_segments_2,
        "/testing/open-19-close/1",
        "/testing/*/1"
    );
    transaction_name_test!(
        test_transaction_name_normalize_in_segments_3,
        "/testing/open19close/1",
        "/testing/*/1"
    );
    transaction_name_test!(
        test_transaction_name_normalize_in_segments_4,
        "/testing/asdf012/asdf034/asdf056",
        "/testing/*/*/*"
    );
    transaction_name_test!(
        test_transaction_name_normalize_in_segments_5,
        "/foo/test%A33/1234",
        "/foo/test%A33/*"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_1,
        "/%2Ftest%2Fopen%20and%20help%2F1%0A",
        "/%2Ftest%2Fopen%20and%20help%2F1%0A"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_2,
        "/this/1234/%E2%9C%85/foo/bar/098123908213",
        "/this/*/%E2%9C%85/foo/bar/*"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_3,
        "/foo/hello%20world-4711/",
        "/foo/*/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_4,
        "/foo/hello%20world-0xdeadbeef/",
        "/foo/*/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_5,
        "/foo/hello%20world-4711/",
        "/foo/*/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_6,
        "/foo/hello%2Fworld/",
        "/foo/hello%2Fworld/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_url_encode_7,
        "/foo/hello%201/",
        "/foo/hello%201/"
    );
    transaction_name_test!(
        test_transaction_name_normalize_sha,
        "/hash/4c79f60c11214eb38604f4ae0781bfb2/diff",
        "/hash/*/diff"
    );
    transaction_name_test!(
        test_transaction_name_normalize_uuid,
        "/u/7b25feea-ed2d-4132-bcbd-6232b7922add/edit",
        "/u/*/edit"
    );
    transaction_name_test!(
        test_transaction_name_normalize_hex,
        "/u/0x3707344A4093822299F31D008/profile/123123213",
        "/u/*/profile/*"
    );
    transaction_name_test!(
        test_transaction_name_normalize_windows_path,
        r"C:\\\\Program Files\\1234\\Files",
        r"C:\\Program Files\*\Files"
    );
    transaction_name_test!(test_transaction_name_skip_replace_all, "12345", "12345");
    transaction_name_test!(
        test_transaction_name_skip_replace_all2,
        "open-12345-close",
        "open-12345-close"
    );

    #[test]
    fn test_scrub_identifiers_before_rules() {
        // There's a rule matching the transaction name. However, the UUID
        // should be scrubbed first. Scrubbing the UUID makes the rule to not
        // match the transformed transaction name anymore.

        let mut event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "transaction": "/remains/rule-target/1234567890",
                "transaction_info": {
                    "source": "url"
                },
                "timestamp": "2021-04-26T08:00:00+0100",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053"
                    }
                }
            }"#,
        )
        .unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::new(NormalizeProcessorConfig {
                transaction_name_config: TransactionNameConfig {
                    rules: &[TransactionNameRule {
                        pattern: LazyGlob::new("/remains/*/1234567890/".to_owned()),
                        expiry: Utc.with_ymd_and_hms(3000, 1, 1, 1, 1, 1).unwrap(),
                        redaction: RedactionRule::default(),
                    }],
                },
                ..Default::default()
            }),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/remains/rule-target/*");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                21,
                31,
            ),
        ),
    },
]"#);
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );
    }

    #[test]
    fn test_scrub_identifiers_and_apply_rules() {
        // Ensure rules are applied after scrubbing identifiers. Rules are only
        // applied when `transaction.source="url"`, so this test ensures this
        // value isn't set as part of identifier scrubbing.
        let mut event = Annotated::<Event>::from_json(
            r#"{
                "type": "transaction",
                "transaction": "/remains/rule-target/1234567890",
                "transaction_info": {
                    "source": "url"
                },
                "timestamp": "2021-04-26T08:00:00+0100",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "contexts": {
                    "trace": {
                        "trace_id": "4c79f60c11214eb38604f4ae0781bfb2",
                        "span_id": "fa90fdead5f74053"
                    }
                }
            }"#,
        )
        .unwrap();

        process_value(
            &mut event,
            &mut NormalizeProcessor::new(NormalizeProcessorConfig {
                transaction_name_config: TransactionNameConfig {
                    rules: &[TransactionNameRule {
                        pattern: LazyGlob::new("/remains/*/**".to_owned()),
                        expiry: Utc.with_ymd_and_hms(3000, 1, 1, 1, 1, 1).unwrap(),
                        redaction: RedactionRule::default(),
                    }],
                },
                ..Default::default()
            }),
            ProcessingState::root(),
        )
        .unwrap();

        assert_eq!(get_value!(event.transaction!), "/remains/*/*");
        assert_eq!(
            get_value!(event.transaction_info.source!).as_str(),
            "sanitized"
        );

        let remarks = get_value!(event!)
            .transaction
            .meta()
            .iter_remarks()
            .collect_vec();
        assert_debug_snapshot!(remarks, @r#"[
    Remark {
        ty: Substituted,
        rule_id: "int",
        range: Some(
            (
                21,
                31,
            ),
        ),
    },
    Remark {
        ty: Substituted,
        rule_id: "/remains/*/**",
        range: None,
    },
]"#);
    }

    // TODO(ja): Enable this test
    // fn assert_nonempty_base<T>(expected_error: &str)
    // where
    //     T: Default + PartialEq + ProcessValue,
    // {
    //     #[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
    //     struct Foo<T> {
    //         #[metastructure(required = "true", nonempty = "true")]
    //         bar: Annotated<T>,
    //         bar2: Annotated<T>,
    //     }

    //     let mut wrapper = Annotated::new(Foo {
    //         bar: Annotated::new(T::default()),
    //         bar2: Annotated::new(T::default()),
    //     });
    //     process_value(&mut wrapper, &mut SchemaProcessor, ProcessingState::root()).unwrap();

    //     assert_eq!(
    //         wrapper,
    //         Annotated::new(Foo {
    //             bar: Annotated::from_error(Error::expected(expected_error), None),
    //             bar2: Annotated::new(T::default())
    //         })
    //     );
    // }

    // #[test]
    // fn test_nonempty_string() {
    //     assert_nonempty_base::<String>("a non-empty string");
    // }

    // #[test]
    // fn test_nonempty_array() {
    //     assert_nonempty_base::<Array<u64>>("a non-empty value");
    // }

    // #[test]
    // fn test_nonempty_object() {
    //     assert_nonempty_base::<Object<u64>>("a non-empty value");
    // }

    #[test]
    fn test_invalid_email() {
        let mut user = Annotated::new(User {
            email: Annotated::new("bananabread".to_owned()),
            ..Default::default()
        });

        let expected = user.clone();
        let mut processor = NormalizeProcessor::new(NormalizeProcessorConfig::default());
        processor::process_value(&mut user, &mut processor, ProcessingState::root()).unwrap();

        assert_eq!(user, expected);
    }

    #[test]
    fn test_client_sdk_missing_attribute() {
        let mut info = Annotated::new(ClientSdkInfo {
            name: Annotated::new("sentry.rust".to_string()),
            ..Default::default()
        });

        let mut processor = NormalizeProcessor::new(NormalizeProcessorConfig::default());
        processor::process_value(&mut info, &mut processor, ProcessingState::root()).unwrap();

        let expected = Annotated::new(ClientSdkInfo {
            name: Annotated::new("sentry.rust".to_string()),
            version: Annotated::from_error(ErrorKind::MissingAttribute, None),
            ..Default::default()
        });

        assert_eq!(info, expected);
    }

    #[test]
    fn test_mechanism_missing_attributes() {
        let mut mechanism = Annotated::new(Mechanism {
            ty: Annotated::new("mytype".to_string()),
            meta: Annotated::new(MechanismMeta {
                errno: Annotated::new(CError {
                    name: Annotated::new("ENOENT".to_string()),
                    ..Default::default()
                }),
                mach_exception: Annotated::new(MachException {
                    name: Annotated::new("EXC_BAD_ACCESS".to_string()),
                    ..Default::default()
                }),
                signal: Annotated::new(PosixSignal {
                    name: Annotated::new("SIGSEGV".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        let mut processor = NormalizeProcessor::new(NormalizeProcessorConfig::default());
        processor::process_value(&mut mechanism, &mut processor, ProcessingState::root()).unwrap();

        let expected = Annotated::new(Mechanism {
            ty: Annotated::new("mytype".to_string()),
            meta: Annotated::new(MechanismMeta {
                errno: Annotated::new(CError {
                    number: Annotated::empty(),
                    name: Annotated::new("ENOENT".to_string()),
                }),
                mach_exception: Annotated::new(MachException {
                    ty: Annotated::empty(),
                    code: Annotated::empty(),
                    subcode: Annotated::empty(),
                    name: Annotated::new("EXC_BAD_ACCESS".to_string()),
                }),
                signal: Annotated::new(PosixSignal {
                    number: Annotated::empty(),
                    code: Annotated::empty(),
                    name: Annotated::new("SIGSEGV".to_string()),
                    code_name: Annotated::empty(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        });

        assert_eq!(mechanism, expected);
    }

    #[test]
    fn test_stacktrace_missing_attribute() {
        let mut stack = Annotated::new(RawStacktrace::default());

        let mut processor = NormalizeProcessor::new(NormalizeProcessorConfig::default());
        processor::process_value(&mut stack, &mut processor, ProcessingState::root()).unwrap();

        let expected = Annotated::new(RawStacktrace {
            frames: Annotated::from_error(ErrorKind::MissingAttribute, None),
            ..Default::default()
        });

        assert_eq!(stack, expected);
    }

    #[test]
    fn test_newlines_release() {
        let mut event = Annotated::new(Event {
            release: Annotated::new("42\n".to_string().into()),
            ..Default::default()
        });

        let mut processor = NormalizeProcessor::new(NormalizeProcessorConfig::default());
        processor::process_value(&mut event, &mut processor, ProcessingState::root()).unwrap();

        let expected = Annotated::new(Event {
            release: Annotated::new("42".to_string().into()),
            ..Default::default()
        });

        assert_eq!(get_value!(expected.release!), get_value!(event.release!));
    }
}
