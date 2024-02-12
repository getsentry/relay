//! Event normalization.
//!
//! This module provides a function to normalize events.

use std::collections::hash_map::DefaultHasher;

use std::hash::{Hash, Hasher};
use std::mem;

use itertools::Itertools;
use once_cell::sync::OnceCell;
use regex::Regex;
use relay_base_schema::metrics::{
    can_be_valid_metric_name, DurationUnit, FractionUnit, MetricResourceIdentifier, MetricUnit,
};
use relay_event_schema::processor::{self, MaxChars, ProcessingAction, ProcessingState, Processor};
use relay_event_schema::protocol::{
    AsPair, ClientSdkInfo, Context, ContextInner, Contexts, DebugImage, DeviceClass, Event,
    EventId, EventType, Exception, Headers, IpAddr, Level, LogEntry, Measurement, Measurements,
    MetricSummaryMapping, NelContext, ReplayContext, Request, SpanStatus, Tags, Timestamp,
    TraceContext, User, VALID_PLATFORMS,
};
use relay_protocol::{
    Annotated, Empty, Error, ErrorKind, FromValue, IntoValue, Meta, Object, Remark, RemarkType,
    Value,
};
use smallvec::SmallVec;
use uuid::Uuid;

use crate::normalize::request;
use crate::span::tag_extraction::{self, extract_span_tags};
use crate::utils::{self, MAX_DURATION_MOBILE_MS};
use crate::{
    breakdowns, legacy, mechanism, schema, span, stacktrace, transactions, trimming, user_agent,
    BreakdownsConfig, DynamicMeasurementsConfig, GeoIpLookup, PerformanceScoreConfig,
    RawUserAgentInfo, SpanDescriptionRule, TransactionNameConfig,
};

/// Configuration for [`normalize_event`].
#[derive(Clone, Debug)]
pub struct NormalizationConfig<'a> {
    /// The identifier of the target project, which gets added to the payload.
    pub project_id: Option<u64>,

    /// The name and version of the SDK that sent the event.
    pub client: Option<String>,

    /// The internal identifier of the DSN, which gets added to the payload.
    ///
    /// Note that this is different from the DSN's public key. The ID is usually numeric.
    pub key_id: Option<String>,

    /// The version of the protocol.
    ///
    /// This is a deprecated field, as there is no more versioning of Relay event payloads.
    pub protocol_version: Option<String>,

    /// Configuration for issue grouping.
    ///
    /// This configuration is persisted into the event payload to achieve idempotency in the
    /// processing pipeline and for reprocessing.
    pub grouping_config: Option<serde_json::Value>,

    /// The IP address of the SDK that sent the event.
    ///
    /// When `{{auto}}` is specified and there is no other IP address in the payload, such as in the
    /// `request` context, this IP address gets added to the `user` context.
    pub client_ip: Option<&'a IpAddr>,

    /// The SDK's sample rate as communicated via envelope headers.
    ///
    /// It is persisted into the event payload.
    pub client_sample_rate: Option<f64>,

    /// The user-agent and client hints obtained from the submission request headers.
    ///
    /// Client hints are the preferred way to infer device, operating system, and browser
    /// information should the event payload contain no such data. If no client hints are present,
    /// normalization falls back to the user agent.
    pub user_agent: RawUserAgentInfo<&'a str>,

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

    /// The identifier of the Replay running while this event was created.
    ///
    /// It is persisted into the event payload for correlation.
    pub replay_id: Option<Uuid>,
}

impl<'a> Default for NormalizationConfig<'a> {
    fn default() -> Self {
        Self {
            project_id: Default::default(),
            client: Default::default(),
            key_id: Default::default(),
            protocol_version: Default::default(),
            grouping_config: Default::default(),
            client_ip: Default::default(),
            client_sample_rate: Default::default(),
            user_agent: Default::default(),
            max_name_and_unit_len: Default::default(),
            breakdowns_config: Default::default(),
            normalize_user_agent: Default::default(),
            transaction_name_config: Default::default(),
            is_renormalize: Default::default(),
            device_class_synthesis_config: Default::default(),
            enrich_spans: Default::default(),
            max_tag_value_length: usize::MAX,
            span_description_rules: Default::default(),
            performance_score: Default::default(),
            geoip_lookup: Default::default(),
            enable_trimming: false,
            measurements: None,
            replay_id: Default::default(),
        }
    }
}

/// Normalizes an event.
///
/// Normalization consists of applying a series of transformations on the event
/// payload based on the given configuration.
pub fn normalize_event(event: &mut Annotated<Event>, config: &NormalizationConfig) {
    let Annotated(Some(ref mut event), ref mut meta) = event else {
        return;
    };

    let is_renormalize = config.is_renormalize;

    // Convert legacy data structures to current format
    let _ = legacy::LegacyProcessor.process_event(event, meta, ProcessingState::root());

    if !is_renormalize {
        // Check for required and non-empty values
        let _ = schema::SchemaProcessor.process_event(event, meta, ProcessingState::root());

        normalize(event, meta, config);
    }

    if config.enable_trimming {
        // Trim large strings and databags down
        let _ =
            trimming::TrimmingProcessor::new().process_event(event, meta, ProcessingState::root());
    }
}

/// Normalizes the given event based on the given config.
fn normalize(event: &mut Event, meta: &mut Meta, config: &NormalizationConfig) {
    // Normalize the transaction.
    // (internally noops for non-transaction events).
    // TODO: Parts of this processor should probably be a filter so we
    // can revert some changes to ProcessingAction)
    let mut transactions_processor =
        transactions::TransactionsProcessor::new(config.transaction_name_config.clone());
    let _ = transactions_processor.process_event(event, meta, ProcessingState::root());

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
            normalize_user_geoinfo(geoip_lookup, user);
        }
    }

    // Validate the basic attributes we extract metrics from
    let _ = processor::apply(&mut event.release, |release, meta| {
        if crate::validate_release(release).is_ok() {
            Ok(())
        } else {
            meta.add_error(ErrorKind::InvalidData);
            Err(ProcessingAction::DeleteValueSoft)
        }
    });
    let _ = processor::apply(&mut event.environment, |environment, meta| {
        if crate::validate_environment(environment).is_ok() {
            Ok(())
        } else {
            meta.add_error(ErrorKind::InvalidData);
            Err(ProcessingAction::DeleteValueSoft)
        }
    });

    // Default required attributes, even if they have errors
    normalize_user(&mut event.user);
    normalize_logentry(&mut event.logentry, meta);
    normalize_debug_meta(event);
    normalize_breadcrumbs(event);
    normalize_release_dist(event); // dist is a tag extracted along with other metrics from transactions
    normalize_event_tags(event); // Tags are added to every metric
    normalize_platform_and_level(event);

    // TODO: Consider moving to store normalization
    if config.device_class_synthesis_config {
        normalize_device_class(event);
    }
    normalize_stacktraces(event);
    normalize_exceptions(event); // Browser extension filters look at the stacktrace
    normalize_user_agent(event, config.normalize_user_agent); // Legacy browsers filter
    normalize_event_measurements(
        event,
        config.measurements.clone(),
        config.max_name_and_unit_len,
    ); // Measurements are part of the metric extraction
    normalize_performance_score(event, config.performance_score);
    normalize_breakdowns(event, config.breakdowns_config); // Breakdowns are part of the metric extraction too
    normalize_default_attributes(event, meta, config);

    let _ = processor::apply(&mut event.request, |request, _| {
        request::normalize_request(request);
        Ok(())
    });

    // Some contexts need to be normalized before metrics extraction takes place.
    normalize_contexts(&mut event.contexts);

    if event.ty.value() == Some(&EventType::Transaction) {
        crate::normalize::normalize_app_start_spans(event);
        span::exclusive_time::compute_span_exclusive_time(event);
        normalize_all_metrics_summaries(event);
    }

    if config.enrich_spans {
        extract_span_tags(
            event,
            &tag_extraction::Config {
                max_tag_value_size: config.max_tag_value_length,
            },
        );
    }

    if let Some(context) = event.context_mut::<TraceContext>() {
        context.client_sample_rate = Annotated::from(config.client_sample_rate);
    }
    normalize_replay_context(event, config.replay_id);
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

fn normalize_replay_context(event: &mut Event, replay_id: Option<Uuid>) {
    if let Some(ref mut contexts) = event.contexts.value_mut() {
        if let Some(replay_id) = replay_id {
            contexts.add(ReplayContext {
                replay_id: Annotated::new(EventId(replay_id)),
                other: Object::default(),
            });
        }
    }
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
pub fn normalize_user_geoinfo(geoip_lookup: &GeoIpLookup, user: &mut User) {
    // Infer user.geo from user.ip_address
    if user.geo.value().is_none() {
        if let Some(ip_address) = user.ip_address.value() {
            if let Ok(Some(geo)) = geoip_lookup.lookup(ip_address.as_str()) {
                user.geo.set_value(Some(geo));
            }
        }
    }
}

fn normalize_user(user: &mut Annotated<User>) {
    let Annotated(Some(user), _) = user else {
        return;
    };
    if !user.other.is_empty() {
        let data = user.data.value_mut().get_or_insert_with(Object::new);
        data.extend(std::mem::take(&mut user.other));
    }
}

fn normalize_logentry(logentry: &mut Annotated<LogEntry>, _meta: &mut Meta) {
    let _ = processor::apply(logentry, |logentry, meta| {
        crate::logentry::normalize_logentry(logentry, meta)
    });
}

/// Normalizes the debug images in the event's debug meta.
fn normalize_debug_meta(event: &mut Event) {
    let Annotated(Some(debug_meta), _) = &mut event.debug_meta else {
        return;
    };
    let Annotated(Some(debug_images), _) = &mut debug_meta.images else {
        return;
    };

    for annotated_image in debug_images {
        let _ = processor::apply(annotated_image, |image, meta| match image {
            DebugImage::Other(_) => {
                meta.add_error(Error::invalid("unsupported debug image type"));
                Err(ProcessingAction::DeleteValueSoft)
            }
            _ => Ok(()),
        });
    }
}

fn normalize_breadcrumbs(event: &mut Event) {
    let Annotated(Some(breadcrumbs), _) = &mut event.breadcrumbs else {
        return;
    };
    let Some(breadcrumbs) = breadcrumbs.values.value_mut() else {
        return;
    };

    for annotated_breadcrumb in breadcrumbs {
        let Annotated(Some(breadcrumb), _) = annotated_breadcrumb else {
            continue;
        };

        if breadcrumb.ty.value().is_empty() {
            breadcrumb.ty.set_value(Some("default".to_string()));
        }
        if breadcrumb.level.value().is_none() {
            breadcrumb.level.set_value(Some(Level::Info));
        }
    }
}

/// Ensures that the `release` and `dist` fields match up.
fn normalize_release_dist(event: &mut Event) {
    normalize_dist(&mut event.dist);
}

fn normalize_dist(distribution: &mut Annotated<String>) {
    let _ = processor::apply(distribution, |dist, meta| {
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
    });
}

/// Defaults the `platform` and `level` required attributes.
fn normalize_platform_and_level(event: &mut Event) {
    // The defaulting behavior, was inherited from `StoreNormalizeProcessor` and it's put here since only light
    // normalization happens before metrics extraction and we want the metrics extraction pipeline to already work
    // on some normalized data.
    event.platform.get_or_insert_with(|| "other".to_string());
    event.level.get_or_insert_with(|| match event.ty.value() {
        Some(EventType::Transaction) => Level::Info,
        _ => Level::Error,
    });
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
fn normalize_event_tags(event: &mut Event) {
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
        let _ = processor::apply(tag, |tag, _| {
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
        });
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

/// Normalizes all the stack traces in the given event.
///
/// Normalized stack traces are `event.stacktrace`, `event.exceptions.stacktrace`, and
/// `event.thread.stacktrace`. Raw stack traces are not normalized.
fn normalize_stacktraces(event: &mut Event) {
    normalize_event_stacktrace(event);
    normalize_exception_stacktraces(event);
    normalize_thread_stacktraces(event);
}

/// Normalizes an event's stack trace, in `event.stacktrace`.
fn normalize_event_stacktrace(event: &mut Event) {
    let Annotated(Some(stacktrace), meta) = &mut event.stacktrace else {
        return;
    };
    stacktrace::normalize_stacktrace(&mut stacktrace.0, meta);
}

/// Normalizes the stack traces in an event's exceptions, in `event.exceptions.stacktraces`.
///
/// Note: the raw stack traces, in `event.exceptions.raw_stacktraces` is not normalized.
fn normalize_exception_stacktraces(event: &mut Event) {
    let Some(event_exception) = event.exceptions.value_mut() else {
        return;
    };
    let Some(exceptions) = event_exception.values.value_mut() else {
        return;
    };
    for annotated_exception in exceptions {
        let Some(exception) = annotated_exception.value_mut() else {
            continue;
        };
        if let Annotated(Some(stacktrace), meta) = &mut exception.stacktrace {
            stacktrace::normalize_stacktrace(&mut stacktrace.0, meta);
        }
    }
}

/// Normalizes the stack traces in an event's threads, in `event.threads.stacktraces`.
///
/// Note: the raw stack traces, in `event.threads.raw_stacktraces`, is not normalized.
fn normalize_thread_stacktraces(event: &mut Event) {
    let Some(event_threads) = event.threads.value_mut() else {
        return;
    };
    let Some(threads) = event_threads.values.value_mut() else {
        return;
    };
    for annotated_thread in threads {
        let Some(thread) = annotated_thread.value_mut() else {
            continue;
        };
        if let Annotated(Some(stacktrace), meta) = &mut thread.stacktrace {
            stacktrace::normalize_stacktrace(&mut stacktrace.0, meta);
        }
    }
}

fn normalize_exceptions(event: &mut Event) {
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
                normalize_exception(exception);
                if let Some(exception) = exception.value_mut() {
                    if let Some(mechanism) = exception.mechanism.value_mut() {
                        mechanism::normalize_mechanism(mechanism, os_hint);
                    }
                }
            }
        }
    }
}

fn normalize_exception(exception: &mut Annotated<Exception>) {
    static TYPE_VALUE_RE: OnceCell<Regex> = OnceCell::new();
    let regex = TYPE_VALUE_RE.get_or_init(|| Regex::new(r"^(\w+):(.*)$").unwrap());

    let _ = processor::apply(exception, |exception, meta| {
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
    });
}

fn normalize_user_agent(_event: &mut Event, normalize_user_agent: Option<bool>) {
    if normalize_user_agent.unwrap_or(false) {
        user_agent::normalize_user_agent(_event);
    }
}

/// Ensures measurements interface is only present for transaction events.
fn normalize_event_measurements(
    event: &mut Event,
    measurements_config: Option<DynamicMeasurementsConfig>,
    max_mri_len: Option<usize>,
) {
    if event.ty.value() != Some(&EventType::Transaction) {
        // Only transaction events may have a measurements interface
        event.measurements = Annotated::empty();
    } else if let Annotated(Some(ref mut measurements), ref mut meta) = event.measurements {
        normalize_measurements(
            measurements,
            meta,
            measurements_config,
            max_mri_len,
            event.start_timestamp.0,
            event.timestamp.0,
        );
    }
}

/// Ensure only valid measurements are ingested.
pub fn normalize_measurements(
    measurements: &mut Measurements,
    meta: &mut Meta,
    measurements_config: Option<DynamicMeasurementsConfig>,
    max_mri_len: Option<usize>,
    start_timestamp: Option<Timestamp>,
    end_timestamp: Option<Timestamp>,
) {
    normalize_mobile_measurements(measurements);
    normalize_units(measurements);

    let duration_millis = match (start_timestamp, end_timestamp) {
        (Some(start), Some(end)) => relay_common::time::chrono_to_positive_millis(end - start),
        _ => 0.0,
    };

    compute_measurements(duration_millis, measurements);
    if let Some(measurements_config) = measurements_config {
        remove_invalid_measurements(measurements, meta, measurements_config, max_mri_len);
    }
}

/// Computes performance score measurements for an event.
///
/// This computes score from vital measurements, using config options to define how it is
/// calculated.
pub fn normalize_performance_score(
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
                        && !c.optional
                }) {
                    // All non-optional measurements with a profile weight greater than 0 are
                    // required to exist on the event. Skip calculating performance scores if
                    // a measurement with weight is missing.
                    break;
                }
                let mut score_total = 0.0f64;
                let mut weight_total = 0.0f64;
                for component in &profile.score_components {
                    // Skip optional components if they are not present on the event.
                    if component.optional
                        && !measurements.contains_key(component.measurement.as_str())
                    {
                        continue;
                    }
                    weight_total += component.weight;
                }
                if weight_total.abs() < f64::EPSILON {
                    // All components are optional or have a weight of `0`. We cannot compute
                    // component weights, so we bail.
                    break;
                }
                for component in &profile.score_components {
                    // Optional measurements that are not present are given a weight of 0.
                    let mut normalized_component_weight = 0.0;
                    if let Some(value) = measurements.get_value(component.measurement.as_str()) {
                        normalized_component_weight = component.weight / weight_total;
                        let cdf = utils::calculate_cdf_score(
                            value.max(0.0), // Webvitals can't be negative, but we need to clamp in case of bad data.
                            component.p10,
                            component.p50,
                        );
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

fn normalize_default_attributes(event: &mut Event, meta: &mut Meta, config: &NormalizationConfig) {
    let event_type = infer_event_type(event);
    event.ty = Annotated::from(event_type);
    event.project = Annotated::from(config.project_id);
    event.key_id = Annotated::from(config.key_id.clone());
    event.version = Annotated::from(config.protocol_version.clone());
    event.grouping_config = config
        .grouping_config
        .clone()
        .map_or(Annotated::empty(), |x| {
            FromValue::from_value(Annotated::<Value>::from(x))
        });

    let _ = relay_event_schema::processor::apply(&mut event.platform, |platform, _| {
        if is_valid_platform(platform) {
            Ok(())
        } else {
            Err(ProcessingAction::DeleteValueSoft)
        }
    });

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
        event.client_sdk.set_value(get_sdk_info(config));
    }

    if event.platform.as_str() == Some("java") {
        if let Some(event_logger) = event.logger.value_mut().take() {
            let shortened = shorten_logger(event_logger, meta);
            event.logger.set_value(Some(shortened));
        }
    }
}

/// Returns `true` if the given platform string is a known platform identifier.
///
/// See [`VALID_PLATFORMS`] for a list of all known platforms.
pub fn is_valid_platform(platform: &str) -> bool {
    VALID_PLATFORMS.contains(&platform)
}

/// Infers the `EventType` from the event's interfaces.
fn infer_event_type(event: &Event) -> EventType {
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

/// Returns the SDK info from the config.
fn get_sdk_info(config: &NormalizationConfig) -> Option<ClientSdkInfo> {
    config.client.as_ref().and_then(|client| {
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

/// Normalizes incoming contexts for the downstream metric extraction.
fn normalize_contexts(contexts: &mut Annotated<Contexts>) {
    let _ = processor::apply(contexts, |contexts, _meta| {
        // Reprocessing context sent from SDKs must not be accepted, it is a Sentry-internal
        // construct.
        // [`normalize`] does not run on renormalization anyway.
        contexts.0.remove("reprocessing");

        for annotated in &mut contexts.0.values_mut() {
            if let Some(ContextInner(Context::Trace(context))) = annotated.value_mut() {
                context.status.get_or_insert_with(|| SpanStatus::Unknown);
            }
            if let Some(context_inner) = annotated.value_mut() {
                crate::normalize::contexts::normalize_context(&mut context_inner.0);
            }
        }

        Ok(())
    });
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

        if !can_be_valid_metric_name(name) {
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
            if builtin_measurement.name() == name {
                let value = measurement.value.value().unwrap_or(&0.0);
                // Drop negative values if the builtin measurement does not allow them.
                if !builtin_measurement.allow_negative() && *value < 0.0 {
                    meta.add_error(Error::invalid(format!(
                        "Negative value for measurement {name} not allowed: {value}",
                    )));
                    removed_measurements
                        .insert(name.clone(), Annotated::new(std::mem::take(measurement)));
                    return false;
                }
                // If the unit matches a built-in measurement, we allow it.
                // If the name matches but the unit is wrong, we do not even accept it as a custom measurement,
                // and just drop it instead.
                return builtin_measurement.unit() == unit;
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

    use insta::{assert_debug_snapshot, assert_json_snapshot};
    use itertools::Itertools;
    use relay_event_schema::protocol::{
        Breadcrumb, Contexts, Csp, DebugMeta, DeviceContext, Event, Headers, IpAddr, Measurements,
        MetricSummary, MetricsSummary, Request, Span, Tags, Values,
    };
    use relay_protocol::{get_value, Annotated, SerializableAnnotated};
    use serde_json::json;

    use super::*;
    use crate::{
        ClientHints, DynamicMeasurementsConfig, MeasurementsConfig, PerformanceScoreConfig,
        RawUserAgentInfo,
    };

    #[test]
    fn test_normalize_dist_none() {
        let mut dist = Annotated::default();
        normalize_dist(&mut dist);
        assert_eq!(dist.value(), None);
    }

    #[test]
    fn test_normalize_dist_empty() {
        let mut dist = Annotated::new("".to_string());
        normalize_dist(&mut dist);
        assert_eq!(dist.value(), None);
    }

    #[test]
    fn test_normalize_dist_trim() {
        let mut dist = Annotated::new(" foo  ".to_string());
        normalize_dist(&mut dist);
        assert_eq!(dist.value(), Some(&"foo".to_string()));
    }

    #[test]
    fn test_normalize_dist_whitespace() {
        let mut dist = Annotated::new(" ".to_owned());
        normalize_dist(&mut dist);
        assert_eq!(dist.value(), None);
    }

    #[test]
    fn test_normalize_platform_and_level_with_transaction_event() {
        let json = r#"
        {
            "type": "transaction"
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        normalize_platform_and_level(&mut event);

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "level": "info",
          "type": "transaction",
          "platform": "other",
        }
        "###);
    }

    #[test]
    fn test_normalize_platform_and_level_with_error_event() {
        let json = r#"
        {
            "type": "error"
        }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        normalize_platform_and_level(&mut event);

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "level": "error",
          "type": "error",
          "platform": "other",
        }
        "###);
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

        normalize_event_measurements(&mut event, None, None);

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

        let dynamic_measurement_config =
            DynamicMeasurementsConfig::new(Some(&project_measurement_config), None);

        normalize_event_measurements(&mut event, Some(dynamic_measurement_config), None);

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

        let dynamic_config = DynamicMeasurementsConfig::new(Some(&measurements_config), None);

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
              "value": 0.21864170607444863,
              "unit": "ratio",
            },
            "score.fcp": {
              "value": 0.10750855443790831,
              "unit": "ratio",
            },
            "score.fid": {
              "value": 0.19657361348282545,
              "unit": "ratio",
            },
            "score.lcp": {
              "value": 0.009238896571386584,
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
              "value": 0.21864170607444863,
              "unit": "ratio",
            },
            "score.fcp": {
              "value": 0.10750855443790831,
              "unit": "ratio",
            },
            "score.fid": {
              "value": 0.19657361348282545,
              "unit": "ratio",
            },
            "score.lcp": {
              "value": 0.009238896571386584,
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
              "value": 0.21864170607444863,
              "unit": "ratio",
            },
            "score.fcp": {
              "value": 0.10750855443790831,
              "unit": "ratio",
            },
            "score.fid": {
              "value": 0.19657361348282545,
              "unit": "ratio",
            },
            "score.lcp": {
              "value": 0.009238896571386584,
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

    #[test]
    fn test_computed_performance_score_optional_measurement() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "a": {"value": 213, "unit": "millisecond"},
                "b": {"value": 213, "unit": "millisecond"}
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
                            "p50": 1600,
                        },
                        {
                            "measurement": "b",
                            "weight": 0.30,
                            "p10": 1200,
                            "p50": 2400,
                            "optional": true
                        },
                        {
                            "measurement": "c",
                            "weight": 0.55,
                            "p10": 1200,
                            "p50": 2400,
                            "optional": true
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
            "b": {
              "value": 213.0,
              "unit": "millisecond",
            },
            "score.a": {
              "value": 0.33333215313291975,
              "unit": "ratio",
            },
            "score.b": {
              "value": 0.66666415149198,
              "unit": "ratio",
            },
            "score.total": {
              "value": 0.9999963046248997,
              "unit": "ratio",
            },
            "score.weight.a": {
              "value": 0.33333333333333337,
              "unit": "ratio",
            },
            "score.weight.b": {
              "value": 0.6666666666666667,
              "unit": "ratio",
            },
            "score.weight.c": {
              "value": 0.0,
              "unit": "ratio",
            },
          },
        }
        "###);
    }

    #[test]
    fn test_computed_performance_score_weight_0() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "cls": {"value": 0.11}
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
                            "measurement": "cls",
                            "weight": 0,
                            "p10": 0.1,
                            "p50": 0.25
                        },
                    ],
                    "condition": {
                        "op":"and",
                        "inner": []
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
          "measurements": {
            "cls": {
              "value": 0.11,
            },
          },
        }
        "###);
    }

    #[test]
    fn test_computed_performance_score_negative_value() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "ttfb": {"value": -100, "unit": "millisecond"}
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
                            "measurement": "ttfb",
                            "weight": 1.0,
                            "p10": 100.0,
                            "p50": 250.0
                        },
                    ],
                    "condition": {
                        "op":"and",
                        "inner": []
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
          "measurements": {
            "score.total": {
              "value": 1.0,
              "unit": "ratio",
            },
            "score.ttfb": {
              "value": 1.0,
              "unit": "ratio",
            },
            "score.weight.ttfb": {
              "value": 1.0,
              "unit": "ratio",
            },
            "ttfb": {
              "value": -100.0,
              "unit": "millisecond",
            },
          },
        }
        "###);
    }

    #[test]
    fn test_filter_negative_web_vital_measurements() {
        let json = r#"
        {
            "type": "transaction",
            "timestamp": "2021-04-26T08:00:05+0100",
            "start_timestamp": "2021-04-26T08:00:00+0100",
            "measurements": {
                "ttfb": {"value": -100, "unit": "millisecond"}
            }
        }
        "#;
        let mut event = Annotated::<Event>::from_json(json).unwrap().0.unwrap();

        // Allow ttfb as a builtinMeasurement with allow_negative defaulted to false.
        let project_measurement_config: MeasurementsConfig = serde_json::from_value(json!({
            "builtinMeasurements": [
                {"name": "ttfb", "unit": "millisecond"},
            ],
        }))
        .unwrap();

        let dynamic_measurement_config =
            DynamicMeasurementsConfig::new(Some(&project_measurement_config), None);

        normalize_event_measurements(&mut event, Some(dynamic_measurement_config), None);

        insta::assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(event)), {}, @r###"
        {
          "type": "transaction",
          "timestamp": 1619420405.0,
          "start_timestamp": 1619420400.0,
          "measurements": {},
          "_meta": {
            "measurements": {
              "": Meta(Some(MetaInner(
                err: [
                  [
                    "invalid_data",
                    {
                      "reason": "Negative value for measurement ttfb not allowed: -100",
                    },
                  ],
                ],
                val: Some({
                  "ttfb": {
                    "unit": "millisecond",
                    "value": -100.0,
                  },
                }),
              ))),
            },
          },
        }
        "###);
    }

    #[test]
    fn test_normalization_removes_reprocessing_context() {
        let json = r#"{
            "contexts": {
                "reprocessing": {}
            }
        }"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();
        assert!(get_value!(event.contexts!).contains_key("reprocessing"));
        normalize_event(&mut event, &NormalizationConfig::default());
        assert!(!get_value!(event.contexts!).contains_key("reprocessing"));
    }

    #[test]
    fn test_renormalization_does_not_remove_reprocessing_context() {
        let json = r#"{
            "contexts": {
                "reprocessing": {}
            }
        }"#;
        let mut event = Annotated::<Event>::from_json(json).unwrap();
        assert!(get_value!(event.contexts!).contains_key("reprocessing"));
        normalize_event(
            &mut event,
            &NormalizationConfig {
                is_renormalize: true,
                ..Default::default()
            },
        );
        assert!(get_value!(event.contexts!).contains_key("reprocessing"));
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

        normalize_user(&mut user);

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
    fn test_handle_types_in_spaced_exception_values() {
        let mut exception = Annotated::new(Exception {
            value: Annotated::new("ValueError: unauthorized".to_string().into()),
            ..Exception::default()
        });
        normalize_exception(&mut exception);

        let exception = exception.value().unwrap();
        assert_eq!(exception.value.as_str(), Some("unauthorized"));
        assert_eq!(exception.ty.as_str(), Some("ValueError"));
    }

    #[test]
    fn test_handle_types_in_non_spaced_excepton_values() {
        let mut exception = Annotated::new(Exception {
            value: Annotated::new("ValueError:unauthorized".to_string().into()),
            ..Exception::default()
        });
        normalize_exception(&mut exception);

        let exception = exception.value().unwrap();
        assert_eq!(exception.value.as_str(), Some("unauthorized"));
        assert_eq!(exception.ty.as_str(), Some("ValueError"));
    }

    #[test]
    fn test_rejects_empty_exception_fields() {
        let mut exception = Annotated::new(Exception {
            value: Annotated::new("".to_string().into()),
            ty: Annotated::new("".to_string()),
            ..Default::default()
        });

        normalize_exception(&mut exception);

        assert!(exception.value().is_none());
        assert!(exception.meta().has_errors());
    }

    #[test]
    fn test_json_value() {
        let mut exception = Annotated::new(Exception {
            value: Annotated::new(r#"{"unauthorized":true}"#.to_string().into()),
            ..Exception::default()
        });

        normalize_exception(&mut exception);

        let exception = exception.value().unwrap();

        // Don't split a json-serialized value on the colon
        assert_eq!(exception.value.as_str(), Some(r#"{"unauthorized":true}"#));
        assert_eq!(exception.ty.value(), None);
    }

    #[test]
    fn test_exception_invalid() {
        let mut exception = Annotated::new(Exception::default());

        normalize_exception(&mut exception);

        let expected = Error::with(ErrorKind::MissingAttribute, |error| {
            error.insert("attribute", "type or value");
        });
        assert_eq!(
            exception.meta().iter_errors().collect_tuple(),
            Some((&expected,))
        );
    }

    #[test]
    fn test_normalize_exception() {
        let mut event = Annotated::new(Event {
            exceptions: Annotated::new(Values::new(vec![Annotated::new(Exception {
                // Exception with missing type and value
                ty: Annotated::empty(),
                value: Annotated::empty(),
                ..Default::default()
            })])),
            ..Default::default()
        });

        normalize_event(&mut event, &NormalizationConfig::default());

        let exception = event
            .value()
            .unwrap()
            .exceptions
            .value()
            .unwrap()
            .values
            .value()
            .unwrap()
            .first()
            .unwrap();

        assert_debug_snapshot!(exception.meta(), @r#"
        Meta {
            remarks: [],
            errors: [
                Error {
                    kind: MissingAttribute,
                    data: {
                        "attribute": String(
                            "type or value",
                        ),
                    },
                },
            ],
            original_length: None,
            original_value: Some(
                Object(
                    {
                        "mechanism": ~,
                        "module": ~,
                        "raw_stacktrace": ~,
                        "stacktrace": ~,
                        "thread_id": ~,
                        "type": ~,
                        "value": ~,
                    },
                ),
            ),
        }"#);
    }

    #[test]
    fn test_normalize_breadcrumbs() {
        let mut event = Event {
            breadcrumbs: Annotated::new(Values {
                values: Annotated::new(vec![Annotated::new(Breadcrumb::default())]),
                ..Default::default()
            }),
            ..Default::default()
        };
        normalize_breadcrumbs(&mut event);

        let breadcrumb = event
            .breadcrumbs
            .value()
            .unwrap()
            .values
            .value()
            .unwrap()
            .first()
            .unwrap()
            .value()
            .unwrap();
        assert_eq!(breadcrumb.ty.value().unwrap(), "default");
        assert_eq!(&breadcrumb.level.value().unwrap().to_string(), "info");
    }

    #[test]
    fn test_other_debug_images_have_meta_errors() {
        let mut event = Event {
            debug_meta: Annotated::new(DebugMeta {
                images: Annotated::new(vec![Annotated::new(
                    DebugImage::Other(BTreeMap::default()),
                )]),
                ..Default::default()
            }),
            ..Default::default()
        };
        normalize_debug_meta(&mut event);

        let debug_image_meta = event
            .debug_meta
            .value()
            .unwrap()
            .images
            .value()
            .unwrap()
            .first()
            .unwrap()
            .meta();
        assert_debug_snapshot!(debug_image_meta, @r#"
        Meta {
            remarks: [],
            errors: [
                Error {
                    kind: InvalidData,
                    data: {
                        "reason": String(
                            "unsupported debug image type",
                        ),
                    },
                },
            ],
            original_length: None,
            original_value: Some(
                Object(
                    {},
                ),
            ),
        }"#);
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
