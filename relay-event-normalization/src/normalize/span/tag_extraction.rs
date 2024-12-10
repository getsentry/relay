//! Logic for persisting items into `span.sentry_tags` and `span.measurements` fields.
//! These are then used for metrics extraction.
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::net::IpAddr;
use std::ops::ControlFlow;

use once_cell::sync::Lazy;
use regex::Regex;
use relay_base_schema::metrics::{DurationUnit, InformationUnit, MetricUnit};
use relay_event_schema::protocol::{
    AppContext, BrowserContext, Event, Measurement, OsContext, ProfileContext, SentryTags, Span,
    Timestamp, TraceContext,
};
use relay_protocol::{Annotated, Value};
use sqlparser::ast::Visit;
use sqlparser::ast::{ObjectName, Visitor};
use url::Url;

use crate::span::country_subregion::Subregion;
use crate::span::description::{
    concatenate_host_and_port, scrub_domain_name, scrub_span_description,
};
use crate::span::TABLE_NAME_REGEX;
use crate::utils::{
    extract_transaction_op, http_status_code_from_span, MAIN_THREAD_NAME, MOBILE_SDKS,
};

/// Render-blocking resources are static files, such as fonts, CSS, and JavaScript that block or
/// delay the browser from rendering page content to the screen.
///
/// See <https://developer.mozilla.org/en-US/docs/Web/API/PerformanceResourceTiming/renderBlockingStatus>.
enum RenderBlockingStatus {
    Blocking,
    NonBlocking,
}

impl<'a> TryFrom<&'a str> for RenderBlockingStatus {
    type Error = &'a str;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        Ok(match value {
            "blocking" => Self::Blocking,
            "non-blocking" => Self::NonBlocking,
            other => return Err(other),
        })
    }
}

impl std::fmt::Display for RenderBlockingStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Blocking => "blocking",
            Self::NonBlocking => "non-blocking",
        })
    }
}

/// Wrapper for [`extract_span_tags`].
///
/// Tags longer than `max_tag_value_size` bytes will be truncated.
pub(crate) fn extract_span_tags_from_event(
    event: &mut Event,
    max_tag_value_size: usize,
    http_scrubbing_allow_list: &[String],
) {
    // Temporarily take ownership to pass both an event reference and a mutable span reference to `extract_span_tags`.
    let mut spans = std::mem::take(&mut event.spans);
    let Some(spans_vec) = spans.value_mut() else {
        return;
    };
    extract_span_tags(
        event,
        spans_vec.as_mut_slice(),
        max_tag_value_size,
        http_scrubbing_allow_list,
    );

    event.spans = spans;
}

/// Extracts tags and measurements from event and spans and materializes them into the spans.
///
/// Tags longer than `max_tag_value_size` bytes will be truncated.
pub fn extract_span_tags(
    event: &Event,
    spans: &mut [Annotated<Span>],
    max_tag_value_size: usize,
    span_allowed_hosts: &[String],
) {
    // TODO: To prevent differences between metrics and payloads, we should not extract tags here
    // when they have already been extracted by a downstream relay.
    let shared_tags = extract_shared_tags(event);
    let is_mobile = shared_tags
        .mobile
        .value()
        .is_some_and(|v| v.as_str() == "true");
    let start_type = is_mobile.then(|| get_event_start_type(event)).flatten();

    let ttid = timestamp_by_op(spans, "ui.load.initial_display");
    let ttfd = timestamp_by_op(spans, "ui.load.full_display");

    for span in spans {
        let Some(span) = span.value_mut() else {
            continue;
        };

        let mut tags = extract_tags(
            span,
            max_tag_value_size,
            ttid,
            ttfd,
            is_mobile,
            start_type,
            span_allowed_hosts,
        );

        shared_tags.copy_into(&mut tags);
        span.sentry_tags = tags.into();

        extract_measurements(span, is_mobile);
    }
}

/// Extract segment span specific tags and measurements from the event and materialize them into the spans.
pub fn extract_segment_span_tags(event: &Event, spans: &mut [Annotated<Span>]) {
    let segment_tags = extract_segment_tags(event);
    let segment_measurements = extract_segment_measurements(event);

    for span in spans {
        let Some(span) = span.value_mut() else {
            continue;
        };

        if !segment_measurements.is_empty() {
            span.measurements
                .get_or_insert_with(Default::default)
                .extend(
                    segment_measurements
                        .iter()
                        .map(|(k, v)| (k.clone(), Annotated::new(v.clone()))),
                );
        }
        segment_tags.copy_into(span.sentry_tags.get_or_insert_with(Default::default))
    }
}

#[derive(Clone, Default)]
struct SharedTags {
    browser_name: Annotated<String>,
    device_class: Annotated<String>,
    environment: Annotated<String>,
    mobile: Annotated<String>,
    os_name: Annotated<String>,
    platform: Annotated<String>,
    profiler_id: Annotated<String>,
    release: Annotated<String>,
    sdk_name: Annotated<String>,
    sdk_version: Annotated<String>,
    thread_id: Annotated<String>,
    thread_name: Annotated<String>,
    trace_status: Annotated<String>,
    transaction_method: Annotated<String>,
    transaction_op: Annotated<String>,
    transaction: Annotated<String>,
    user_country_code: Annotated<String>,
    user_email: Annotated<String>,
    user_id: Annotated<String>,
    user_ip: Annotated<String>,
    user_subregion: Annotated<String>,
    user_username: Annotated<String>,
    user: Annotated<String>,
}

impl SharedTags {
    fn copy_into(&self, tags: &mut SentryTags) {
        let Self {
            browser_name,
            device_class,
            environment,
            mobile,
            os_name,
            platform,
            profiler_id,
            release,
            sdk_name,
            sdk_version,
            thread_id,
            thread_name,
            trace_status,
            transaction_method,
            transaction_op,
            transaction,
            user_country_code,
            user_email,
            user_id,
            user_ip,
            user_subregion,
            user_username,
            user,
        } = self.clone();
        tags.browser_name = browser_name;
        tags.device_class = device_class;
        tags.environment = environment;
        tags.mobile = mobile;
        tags.os_name = os_name;
        tags.platform = platform;
        tags.profiler_id = profiler_id;
        tags.release = release;
        tags.sdk_name = sdk_name;
        tags.sdk_version = sdk_version;
        tags.thread_id = thread_id;
        tags.thread_name = thread_name;
        tags.trace_status = trace_status;
        tags.transaction_method = transaction_method;
        tags.transaction_op = transaction_op;
        tags.transaction = transaction;
        tags.user_country_code = user_country_code;
        tags.user_email = user_email;
        tags.user_id = user_id;
        tags.user_ip = user_ip;
        tags.user_subregion = user_subregion;
        tags.user_username = user_username;
        tags.user = user;
    }
}

/// Extracts tags shared by every span.
fn extract_shared_tags(event: &Event) -> SharedTags {
    let mut tags = SharedTags::default();

    if let Some(release) = event.release.as_str() {
        tags.release = release.to_owned().into();
    }

    if let Some(user) = event.user.value() {
        if let Some(sentry_user) = user.sentry_user.value() {
            tags.user = sentry_user.clone().into();
        }
        if let Some(user_id) = user.id.value() {
            tags.user_id = user_id.as_str().to_owned().into();
        }
        if let Some(user_ip) = user.ip_address.value() {
            tags.user_ip = user_ip.as_str().to_owned().into();
        }
        if let Some(user_username) = user.username.value() {
            tags.user_username = user_username.as_str().to_owned().into();
        }
        if let Some(user_email) = user.email.value() {
            tags.user_email = user_email.clone().into();
        }

        // We only want this on frontend or mobile modules.
        let should_extract_geo = (event.context::<BrowserContext>().is_some()
            && event.platform.as_str() == Some("javascript"))
            || MOBILE_SDKS.contains(&event.sdk_name());

        if should_extract_geo {
            if let Some(country_code) = user.geo.value().and_then(|geo| geo.country_code.value()) {
                tags.user_country_code = country_code.to_owned().into();
                if let Some(subregion) = Subregion::from_iso2(country_code.as_str()) {
                    let numerical_subregion = subregion as u8;
                    tags.user_subregion = numerical_subregion.to_string().into();
                }
            }
        }
    }

    if let Some(environment) = event.environment.as_str() {
        tags.environment = environment.to_owned().into();
    }

    if let Some(transaction_name) = event.transaction.value() {
        tags.transaction = transaction_name.clone().into();

        let transaction_method_from_request = event
            .request
            .value()
            .and_then(|r| r.method.value())
            .map(|m| m.to_uppercase());

        if let Some(transaction_method) = transaction_method_from_request.or_else(|| {
            http_method_from_transaction_name(transaction_name).map(|m| m.to_uppercase())
        }) {
            tags.transaction_method = transaction_method.into();
        }
    }

    if let Some(trace_context) = event.context::<TraceContext>() {
        if let Some(op) = extract_transaction_op(trace_context) {
            tags.transaction_op = op.to_lowercase().into();
        }

        if let Some(status) = trace_context.status.value() {
            tags.trace_status = status.to_string().into();
        }
    }

    if MOBILE_SDKS.contains(&event.sdk_name()) {
        tags.mobile = "true".to_owned().into();

        // Check if app context exists. This tells us if the span originated from
        // an app (as opposed to mobile browser) since we are currently focused on
        // app use cases for mobile.
        if event.context::<AppContext>().is_some() {
            if let Some(os_context) = event.context::<OsContext>() {
                if let Some(os_name) = os_context.name.value() {
                    tags.os_name = os_name.to_string().into();
                }
            }
        }
    }

    if let Some(device_class) = event.tag_value("device.class") {
        tags.device_class = device_class.to_owned().into();
    }

    if let Some(browser_name) = event
        .context::<BrowserContext>()
        .and_then(|v| v.name.value())
    {
        tags.browser_name = browser_name.to_owned().into();
    }

    if let Some(profiler_id) = event
        .context::<ProfileContext>()
        .and_then(|profile_context| profile_context.profiler_id.value())
    {
        tags.profiler_id = profiler_id.to_string().into();
    }

    tags.sdk_name = event.sdk_name().to_owned().into();
    tags.sdk_version = event.sdk_version().to_owned().into();
    tags.platform = event.platform.as_str().unwrap_or("other").to_owned().into();

    if let Some(data) = event
        .context::<TraceContext>()
        .and_then(|trace_context| trace_context.data.value())
    {
        if let Some(thread_id) = data.thread_id.value() {
            tags.thread_id = thread_id.to_string().into();
        }

        if let Some(thread_name) = data.thread_name.value() {
            tags.thread_name = thread_name.to_string().into();
        }
    }

    tags
}

/// Extracts measurements that should only be saved on segment spans.
fn extract_segment_measurements(event: &Event) -> BTreeMap<String, Measurement> {
    let mut measurements = BTreeMap::new();

    if let Some(trace_context) = event.context::<TraceContext>() {
        if let Some(op) = extract_transaction_op(trace_context) {
            if op == "queue.publish" || op == "queue.process" {
                if let Some(data) = trace_context.data.value() {
                    for (field, key, unit) in [
                        (
                            &data.messaging_message_retry_count,
                            "messaging.message.retry.count",
                            MetricUnit::None,
                        ),
                        (
                            &data.messaging_message_receive_latency,
                            "messaging.message.receive.latency",
                            MetricUnit::Duration(DurationUnit::MilliSecond),
                        ),
                        (
                            &data.messaging_message_body_size,
                            "messaging.message.body.size",
                            MetricUnit::Information(InformationUnit::Byte),
                        ),
                    ] {
                        if let Some(value) = value_to_f64(field.value()) {
                            measurements.insert(
                                key.into(),
                                Measurement {
                                    value: value.into(),
                                    unit: unit.into(),
                                },
                            );
                        }
                    }
                }
            }
        }
    }

    measurements
}

#[derive(Clone, Debug, Default)]
struct SegmentTags {
    messaging_destination_name: Annotated<String>,
    messaging_message_id: Annotated<String>,
}

impl SegmentTags {
    fn copy_into(&self, tags: &mut SentryTags) {
        let Self {
            messaging_destination_name,
            messaging_message_id,
        } = self.clone();
        tags.messaging_destination_name = messaging_destination_name;
        tags.messaging_message_id = messaging_message_id;
    }
}

/// Extract tags that should only be saved on segment spans.
fn extract_segment_tags(event: &Event) -> SegmentTags {
    let mut tags = SegmentTags::default();

    if let Some(trace_context) = event.context::<TraceContext>() {
        if let Some(op) = extract_transaction_op(trace_context) {
            if op == "queue.publish" || op == "queue.process" {
                if let Some(data) = trace_context.data.value() {
                    tags.messaging_destination_name = data.messaging_destination_name.clone();
                    tags.messaging_message_id = data.messaging_message_id.clone();
                }
            }
        }
    }

    tags
}

/// Writes fields into [`Span::sentry_tags`].
///
/// Generating new span data fields is based on a combination of looking at
/// [span operations](https://develop.sentry.dev/sdk/performance/span-operations/) and
/// existing [span data](https://develop.sentry.dev/sdk/performance/span-data-conventions/) fields,
/// and rely on Sentry conventions and heuristics.
#[allow(clippy::too_many_arguments)]
pub fn extract_tags(
    span: &Span,
    max_tag_value_size: usize,
    initial_display: Option<Timestamp>,
    full_display: Option<Timestamp>,
    is_mobile: bool,
    start_type: Option<&str>,
    span_allowed_hosts: &[String],
) -> SentryTags {
    let mut span_tags = SentryTags::default();

    let system = span
        .data
        .value()
        .and_then(|data| data.db_system.value())
        .and_then(|system| system.as_str());
    if let Some(sys) = system {
        span_tags.system = sys.to_lowercase().into();
    }

    if let Some(status) = span.status.value() {
        span_tags.span_status = status.as_str().to_owned().into();
    }

    if let Some(unsanitized_span_op) = span.op.value() {
        let span_op = unsanitized_span_op.to_lowercase();

        span_tags.span_op = span_op.to_owned().into();

        let category = span_op_to_category(&span_op);
        if let Some(category) = category {
            span_tags.category = category.to_owned().into();
        }

        let (scrubbed_description, parsed_sql) = scrub_span_description(span, span_allowed_hosts);

        let action = match (category, span_op.as_str(), &scrubbed_description) {
            (Some("http"), _, _) => span
                .data
                .value()
                .and_then(|data| data.http_request_method.value())
                .and_then(|method| method.as_str())
                .map(|s| s.to_uppercase()),
            (_, "db.redis", Some(desc)) => {
                // This only works as long as redis span descriptions contain the command + " *"
                let command = desc.replace(" *", "");
                if command.is_empty() {
                    None
                } else {
                    Some(command)
                }
            }
            (Some("db"), _, _) => {
                let action_from_data = span
                    .data
                    .value()
                    .and_then(|data| data.db_operation.value())
                    .and_then(|db_op| db_op.as_str())
                    .map(|s| s.to_uppercase());
                action_from_data.or_else(|| {
                    span.description
                        .value()
                        .and_then(|d| sql_action_from_query(d))
                        .map(|a| a.to_uppercase())
                })
            }
            _ => None,
        };

        if let Some(act) = action {
            span_tags.action = act.into();
        }

        let domain = if span_op == "http.client" || span_op.starts_with("resource.") {
            // HACK: Parse the normalized description to get the normalized domain.
            if let Some(scrubbed) = scrubbed_description.as_deref() {
                let url = if let Some((_, url)) = scrubbed.split_once(' ') {
                    url
                } else {
                    scrubbed
                };
                if let Some(domain) = Url::parse(url).ok().and_then(|url| {
                    url.host_str().map(|h| {
                        let mut domain = h.to_lowercase();
                        if let Some(port) = url.port() {
                            domain = format!("{domain}:{port}");
                        }
                        domain
                    })
                }) {
                    Some(domain)
                } else if let Some(server_address) = span
                    .data
                    .value()
                    .and_then(|data| data.server_address.value())
                    .and_then(|value| value.as_str())
                {
                    let lowercase_address = server_address.to_lowercase();

                    // According to OTel semantic conventions the server port should be in a separate property, called `server.port`, but incoming data sometimes disagrees
                    let (domain, port) = match lowercase_address.split_once(':') {
                        Some((domain, port)) => (domain, port.parse::<u16>().ok()),
                        None => (server_address, None),
                    };

                    // Leave IP addresses alone. Scrub qualified domain names
                    let domain = if domain.parse::<IpAddr>().is_ok() {
                        Cow::Borrowed(domain)
                    } else {
                        scrub_domain_name(domain)
                    };

                    if let Some(url_scheme) = span
                        .data
                        .value()
                        .and_then(|data| data.url_scheme.value())
                        .and_then(|value| value.as_str())
                    {
                        span_tags.raw_domain = format!("{url_scheme}://{lowercase_address}").into();
                    }

                    Some(concatenate_host_and_port(Some(domain.as_ref()), port).into_owned())
                } else {
                    None
                }
            } else {
                None
            }
        } else if span.origin.as_str() == Some("auto.db.supabase") {
            scrubbed_description
                .as_deref()
                .and_then(|s| s.strip_prefix("from("))
                .and_then(|s| s.strip_suffix(')'))
                .map(String::from)
        } else if span_op.starts_with("db") {
            let system = span
                .data
                .value()
                .and_then(|data| data.db_system.value())
                .and_then(|db_op| db_op.as_str());
            if system == Some("mongodb") {
                span.data
                    .value()
                    .and_then(|data| data.db_collection_name.value())
                    .and_then(|db_collection| db_collection.as_str())
                    .map(|db_collection| {
                        if let Cow::Owned(s) = TABLE_NAME_REGEX.replace_all(db_collection, "{%s}") {
                            s
                        } else {
                            db_collection.to_owned()
                        }
                    })
            } else {
                span.description
                    .value()
                    .and_then(|query| sql_tables_from_query(query, &parsed_sql))
            }
        } else {
            None
        };

        if !span_op.starts_with("db.redis") {
            if let Some(dom) = domain {
                span_tags.domain = dom.into();
            }
        }

        if span_op.starts_with("cache.") {
            if let Some(Value::Bool(cache_hit)) =
                span.data.value().and_then(|data| data.cache_hit.value())
            {
                let tag_value = if *cache_hit { "true" } else { "false" };
                span_tags.cache_hit = tag_value.to_owned().into();
            }
            if let Some(cache_keys) = span.data.value().and_then(|data| data.cache_key.value()) {
                if let Ok(cache_keys) = serde_json::to_string(cache_keys) {
                    span_tags.cache_key = cache_keys.into();
                }
            }
        }

        if span_op.starts_with("queue.") {
            if let Some(destination) = span
                .data
                .value()
                .and_then(|data| data.messaging_destination_name.as_str())
            {
                span_tags.messaging_destination_name = destination.to_owned().into();
            }
            if let Some(message_id) = span
                .data
                .value()
                .and_then(|data| data.messaging_message_id.as_str())
            {
                span_tags.messaging_message_id = message_id.to_owned().into();
            }
        }

        if let Some(scrubbed_desc) = scrubbed_description {
            // Truncating the span description's tag value is, for now,
            // a temporary solution to not get large descriptions dropped. The
            // group tag mustn't be affected by this, and still be
            // computed from the full, untruncated description.

            let mut span_group = format!("{:?}", md5::compute(&scrubbed_desc));
            span_group.truncate(16);
            span_tags.group = span_group.into();

            let truncated = truncate_string(scrubbed_desc, max_tag_value_size);
            if span_op.starts_with("resource.") {
                if let Some(ext) = truncated
                    .rsplit('/')
                    .next()
                    .and_then(|last_segment| last_segment.rsplit_once('.'))
                    .map(|(_, extension)| extension)
                {
                    span_tags.file_extension = ext.to_lowercase().into();
                }
            }

            span_tags.description = truncated.into();
        }

        if category == Some("ai") {
            if let Some(ai_pipeline_name) = span
                .data
                .value()
                .and_then(|data| data.ai_pipeline_name.value())
                .and_then(|val| val.as_str())
            {
                let mut ai_pipeline_group = format!("{:?}", md5::compute(ai_pipeline_name));
                ai_pipeline_group.truncate(16);
                span_tags.ai_pipeline_group = ai_pipeline_group.into();
            }
        }

        if span_op.starts_with("resource.") {
            // TODO: Remove response size tags once product uses measurements instead.
            if let Some(data) = span.data.value() {
                if let Some(value) = data
                    .http_response_content_length
                    .value()
                    .and_then(|v| String::try_from(v).ok())
                {
                    span_tags.http_response_content_length = value.into();
                }

                if let Some(value) = data
                    .http_decoded_response_content_length
                    .value()
                    .and_then(|v| String::try_from(v).ok())
                {
                    span_tags.http_decoded_response_content_length = value.into();
                }

                if let Some(value) = data
                    .http_response_transfer_size
                    .value()
                    .and_then(|v| String::try_from(v).ok())
                {
                    span_tags.http_response_transfer_size = value.into();
                }
            }

            if let Some(resource_render_blocking_status) = span
                .data
                .value()
                .and_then(|data| data.resource_render_blocking_status.value())
                .and_then(|value| value.as_str())
            {
                // Validate that it's a valid status:
                if let Ok(status) = RenderBlockingStatus::try_from(resource_render_blocking_status)
                {
                    span_tags.resource_render_blocking_status = status.to_string().into();
                }
            }
        }
        if let Some(measurements) = span.measurements.value() {
            if (span_op.starts_with("ui.interaction.") && measurements.contains_key("inp"))
                || span_op.starts_with("ui.webvital.")
            {
                if let Some(transaction) = span
                    .data
                    .value()
                    .and_then(|data| data.segment_name.as_str())
                {
                    span_tags.transaction = transaction.to_owned().into();
                }
                if let Some(user) = span.data.value().and_then(|data| data.user.as_str()) {
                    span_tags.user = user.to_owned().into();
                }
                if let Some(replay_id) = span.data.value().and_then(|data| data.replay_id.as_str())
                {
                    span_tags.replay_id = replay_id.to_owned().into();
                }
                if let Some(environment) =
                    span.data.value().and_then(|data| data.environment.as_str())
                {
                    span_tags.environment = environment.to_owned().into();
                }
                if let Some(release) = span.data.value().and_then(|data| data.release.as_str()) {
                    span_tags.release = release.to_owned().into();
                }
            }
        }
    }

    if let Some(status_code) = http_status_code_from_span(span) {
        span_tags.status_code = status_code.into();
    }

    if is_mobile {
        if let Some(thread_name) = span.data.value().and_then(|data| data.thread_name.as_str()) {
            if thread_name == MAIN_THREAD_NAME {
                span_tags.main_thread = "true".to_owned().into();
            }
        }

        // Attempt to read the start type from span.data if it exists, else
        // pass along the start_type from the event.
        if let Some(span_data_start_type) = span
            .data
            .value()
            .and_then(|data| data.app_start_type.value())
            .and_then(|value| value.as_str())
        {
            span_tags.app_start_type = span_data_start_type.to_owned().into();
        } else if let Some(start_type) = start_type {
            span_tags.app_start_type = start_type.to_owned().into();
        }
    }

    if let Some(end_time) = span.timestamp.value() {
        if let Some(initial_display) = initial_display {
            if end_time <= &initial_display {
                span_tags.time_to_initial_display = "ttid".to_owned().into();
            }
        }
        if let Some(full_display) = full_display {
            if end_time <= &full_display {
                span_tags.time_to_full_display = "ttfd".to_owned().into();
            }
        }
    }

    if let Some(browser_name) = span.data.value().and_then(|data| data.browser_name.value()) {
        span_tags.browser_name = browser_name.clone().into();
    }

    if let Some(data) = span.data.value() {
        if let Some(thread_id) = data.thread_id.value() {
            span_tags.thread_id = thread_id.to_string().into();
        }

        if let Some(thread_name) = data.thread_name.as_str() {
            span_tags.thread_name = thread_name.to_owned().into();
        }
    }

    span_tags
}

fn value_to_f64(val: Option<&Value>) -> Option<f64> {
    match val {
        Some(Value::F64(f)) => Some(*f),
        Some(Value::I64(i)) => Some(*i as f64),
        Some(Value::U64(u)) => Some(*u as f64),
        _ => None,
    }
}

/// Copies specific numeric values from span data to span measurements.
pub fn extract_measurements(span: &mut Span, is_mobile: bool) {
    let Some(span_op) = span.op.as_str() else {
        return;
    };

    if span_op.starts_with("ai.") {
        if let Some(data) = span.data.value() {
            for (field, key) in [
                (&data.ai_total_tokens_used, "ai_total_tokens_used"),
                (&data.ai_completion_tokens_used, "ai_completion_tokens_used"),
                (&data.ai_prompt_tokens_used, "ai_prompt_tokens_used"),
            ] {
                if let Some(value) = value_to_f64(field.value()) {
                    let measurements = span.measurements.get_or_insert_with(Default::default);
                    measurements.insert(
                        key.into(),
                        Measurement {
                            value: value.into(),
                            unit: MetricUnit::None.into(),
                        }
                        .into(),
                    );
                }
            }
        }
    }

    if span_op.starts_with("cache.") {
        if let Some(data) = span.data.value() {
            if let Some(value) = value_to_f64(data.cache_item_size.value()) {
                let measurements = span.measurements.get_or_insert_with(Default::default);
                measurements.insert(
                    "cache.item_size".to_owned(),
                    Measurement {
                        value: value.into(),
                        unit: MetricUnit::Information(InformationUnit::Byte).into(),
                    }
                    .into(),
                );
            }
        }
    }

    if span_op.starts_with("resource.") {
        if let Some(data) = span.data.value() {
            for (field, key) in [
                (
                    &data.http_decoded_response_content_length,
                    "http.decoded_response_content_length",
                ),
                (
                    &data.http_response_content_length,
                    "http.response_content_length",
                ),
                (
                    &data.http_response_transfer_size,
                    "http.response_transfer_size",
                ),
            ] {
                if let Some(value) = value_to_f64(field.value()) {
                    let measurements = span.measurements.get_or_insert_with(Default::default);
                    measurements.insert(
                        key.into(),
                        Measurement {
                            value: value.into(),
                            unit: MetricUnit::Information(InformationUnit::Byte).into(),
                        }
                        .into(),
                    );
                }
            }
        }
    }

    if span_op.starts_with("queue.") {
        if let Some(data) = span.data.value() {
            for (field, key, unit) in [
                (
                    &data.messaging_message_retry_count,
                    "messaging.message.retry.count",
                    MetricUnit::None,
                ),
                (
                    &data.messaging_message_receive_latency,
                    "messaging.message.receive.latency",
                    MetricUnit::Duration(DurationUnit::MilliSecond),
                ),
                (
                    &data.messaging_message_body_size,
                    "messaging.message.body.size",
                    MetricUnit::Information(InformationUnit::Byte),
                ),
            ] {
                if let Some(value) = value_to_f64(field.value()) {
                    let measurements = span.measurements.get_or_insert_with(Default::default);
                    measurements.insert(
                        key.into(),
                        Measurement {
                            value: value.into(),
                            unit: unit.into(),
                        }
                        .into(),
                    );
                }
            }
        }
    }

    if is_mobile {
        if let Some(data) = span.data.value() {
            for (field, key, unit) in [
                (&data.frames_frozen, "frames.frozen", MetricUnit::None),
                (&data.frames_slow, "frames.slow", MetricUnit::None),
                (&data.frames_total, "frames.total", MetricUnit::None),
                (
                    &data.frames_delay,
                    "frames.delay",
                    MetricUnit::Duration(DurationUnit::Second),
                ),
            ] {
                if let Some(value) = value_to_f64(field.value()) {
                    let measurements = span.measurements.get_or_insert_with(Default::default);
                    measurements.insert(
                        key.into(),
                        Measurement {
                            value: value.into(),
                            unit: unit.into(),
                        }
                        .into(),
                    );
                }
            }
        }
    }
}

/// Finds first matching span and get its timestamp.
///
/// Used to get time-to-initial/full-display times.
fn timestamp_by_op(spans: &[Annotated<Span>], op: &str) -> Option<Timestamp> {
    spans
        .iter()
        .filter_map(Annotated::value)
        .find(|span| span.op.as_str() == Some(op))
        .and_then(|span| span.timestamp.value().copied())
}

/// Trims the given string with the given maximum bytes. Splitting only happens
/// on char boundaries.
///
/// If the string is short, it remains unchanged. If it's long, this method
/// truncates it to the maximum allowed size and sets the last character to
/// `*`.
fn truncate_string(mut string: String, max_bytes: usize) -> String {
    if string.len() <= max_bytes {
        return string;
    }

    if max_bytes == 0 {
        return String::new();
    }

    let mut cutoff = max_bytes - 1; // Leave space for `*`

    while cutoff > 0 && !string.is_char_boundary(cutoff) {
        cutoff -= 1;
    }

    string.truncate(cutoff);
    string.push('*');
    string
}

/// Regex with a capture group to extract the database action from a query.
///
/// Currently we have an explicit allow-list of database actions considered important.
static SQL_ACTION_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(?P<action>(SELECT|INSERT|DELETE|UPDATE|SET|SAVEPOINT|RELEASE SAVEPOINT|ROLLBACK TO SAVEPOINT))"#).unwrap()
});

fn sql_action_from_query(query: &str) -> Option<&str> {
    extract_captured_substring(query, &SQL_ACTION_EXTRACTOR_REGEX)
}

/// Regex with a capture group to extract the table from a database query,
/// based on `FROM`, `INTO` and `UPDATE` keywords.
static SQL_TABLE_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)(from|into|update)(\s|")+(?P<table>(\w+(\.\w+)*))(\s|")+"#).unwrap()
});

/// Returns a sorted, comma-separated list of SQL tables, if any.
///
/// HACK: When there is a single table, add comma separation so that the
/// backend can understand the difference between tables and their subsets
/// for example: table `,users,` and table `,users_config,` should be considered different
fn sql_tables_from_query(
    query: &str,
    ast: &Option<Vec<sqlparser::ast::Statement>>,
) -> Option<String> {
    match ast {
        Some(ast) => {
            let mut visitor = SqlTableNameVisitor {
                table_names: Default::default(),
            };
            ast.visit(&mut visitor);
            let comma_size: usize = 1;
            let mut s = String::with_capacity(
                visitor
                    .table_names
                    .iter()
                    .map(|name| String::len(name) + comma_size)
                    .sum::<usize>()
                    + comma_size,
            );
            if !visitor.table_names.is_empty() {
                s.push(',');
            }
            for name in visitor.table_names.into_iter() {
                write!(&mut s, "{name},").ok();
            }
            (!s.is_empty()).then_some(s)
        }
        None => {
            relay_log::debug!("Failed to parse SQL");
            extract_captured_substring(query, &SQL_TABLE_EXTRACTOR_REGEX).map(str::to_lowercase)
        }
    }
}

/// Visitor that finds table names in parsed SQL queries.
struct SqlTableNameVisitor {
    /// maintains sorted list of unique table names.
    /// Having a defined order reduces cardinality in the resulting tag (see [`sql_tables_from_query`]).
    table_names: BTreeSet<String>,
}

impl Visitor for SqlTableNameVisitor {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if let Some(name) = relation.0.last() {
            let last = name.value.split('.').last().unwrap_or(&name.value);
            self.table_names.insert(last.to_lowercase());
        }
        ControlFlow::Continue(())
    }
}

/// Regex with a capture group to extract the HTTP method from a string.
pub static HTTP_METHOD_EXTRACTOR_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^(?P<method>(GET|HEAD|POST|PUT|DELETE|CONNECT|OPTIONS|TRACE|PATCH))\b")
        .unwrap()
});

fn http_method_from_transaction_name(name: &str) -> Option<&str> {
    extract_captured_substring(name, &HTTP_METHOD_EXTRACTOR_REGEX)
}

/// Returns the captured substring in `string` with the capture group in `pattern`.
///
/// It assumes there's only one capture group in `pattern`, and only returns the first one.
fn extract_captured_substring<'a>(string: &'a str, pattern: &'a Lazy<Regex>) -> Option<&'a str> {
    let capture_names: Vec<_> = pattern.capture_names().flatten().collect();

    for captures in pattern.captures_iter(string) {
        for name in &capture_names {
            if let Some(capture) = captures.name(name) {
                return Some(&string[capture.start()..capture.end()]);
            }
        }
    }

    None
}

/// Returns the category of a span from its operation. The mapping is available in:
/// <https://develop.sentry.dev/sdk/performance/span-operations/>
fn span_op_to_category(op: &str) -> Option<&str> {
    let mut it = op.split('.'); // e.g. "ui.react.render"
    match (it.next(), it.next()) {
        // Known categories with prefixes:
        (
            Some(prefix @ "ui"),
            Some(category @ ("react" | "vue" | "svelte" | "angular" | "ember")),
        )
        | (Some(prefix @ "ai"), Some(category @ "pipeline"))
        | (
            Some(prefix @ "function"),
            Some(category @ ("nextjs" | "remix" | "gpc" | "aws" | "azure")),
        ) => op.get(..prefix.len() + 1 + category.len()),
        // Main categories (only keep first part):
        (
            category @ Some(
                "ai" | "app" | "browser" | "cache" | "console" | "db" | "event" | "file"
                | "graphql" | "grpc" | "http" | "measure" | "middleware" | "navigation"
                | "pageload" | "queue" | "resource" | "rpc" | "serialize" | "subprocess"
                | "template" | "topic" | "view" | "websocket",
            ),
            _,
        ) => category,
        // Map everything else to unknown:
        _ => None,
    }
}

/// Reads the event measurements to determine the start type of the event.
fn get_event_start_type(event: &Event) -> Option<&'static str> {
    // Check the measurements on the event to determine what kind of start type the event is.
    if event.measurement("app_start_cold").is_some() {
        Some("cold")
    } else if event.measurement("app_start_warm").is_some() {
        Some("warm")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use insta::assert_debug_snapshot;
    use relay_event_schema::protocol::Request;
    use relay_protocol::{get_value, Getter};

    use super::*;
    use crate::span::description::{scrub_queries, Mode};
    use crate::{normalize_event, NormalizationConfig};

    #[test]
    fn test_truncate_string_no_panic() {
        let string = "ÆÆ".to_owned();

        let truncated = truncate_string(string.clone(), 0);
        assert_eq!(truncated, "");

        let truncated = truncate_string(string.clone(), 1);
        assert_eq!(truncated, "*");

        let truncated = truncate_string(string.clone(), 2);
        assert_eq!(truncated, "*");

        let truncated = truncate_string(string.clone(), 3);
        assert_eq!(truncated, "Æ*");

        let truncated = truncate_string(string.clone(), 4);
        assert_eq!(truncated, "ÆÆ");

        let truncated = truncate_string(string, 5);
        assert_eq!(truncated, "ÆÆ");
    }

    macro_rules! span_transaction_method_test {
        // Tests transaction.method is picked from the right place.
        ($name:ident, $transaction_name:literal, $request_method:literal, $expected_method:literal) => {
            #[test]
            fn $name() {
                let json = format!(
                    r#"
                    {{
                        "type": "transaction",
                        "platform": "javascript",
                        "start_timestamp": "2021-04-26T07:59:01+0100",
                        "timestamp": "2021-04-26T08:00:00+0100",
                        "transaction": "{}",
                        "contexts": {{
                            "trace": {{
                                "trace_id": "ff62a8b040f340bda5d830223def1d81",
                                "span_id": "bd429c44b67a3eb4"
                            }}
                        }},
                        "spans": [
                            {{
                                "span_id": "bd429c44b67a3eb4",
                                "start_timestamp": 1597976300.0000000,
                                "timestamp": 1597976302.0000000,
                                "trace_id": "ff62a8b040f340bda5d830223def1d81"
                            }}
                        ]
                    }}
                "#,
                    $transaction_name
                );

                let mut event = Annotated::<Event>::from_json(&json).unwrap();

                if !$request_method.is_empty() {
                    if let Some(e) = event.value_mut() {
                        e.request = Annotated::new(Request {
                            method: Annotated::new(format!("{}", $request_method)),
                            ..Default::default()
                        });
                    }
                }

                normalize_event(
                    &mut event,
                    &NormalizationConfig {
                        enrich_spans: true,
                        ..Default::default()
                    },
                );

                assert_eq!(
                    $expected_method,
                    event
                        .value()
                        .and_then(|e| e.spans.value())
                        .and_then(|spans| spans[0].value())
                        .and_then(|s| s.sentry_tags.value())
                        .and_then(|d| d.transaction_method.value())
                        .map(|v| v.as_str())
                        .unwrap()
                );
            }
        };
    }

    span_transaction_method_test!(
        test_http_method_txname,
        "get /api/:version/users/",
        "",
        "GET"
    );

    span_transaction_method_test!(
        test_http_method_context,
        "/api/:version/users/",
        "post",
        "POST"
    );

    span_transaction_method_test!(
        test_http_method_request_prioritized,
        "get /api/:version/users/",
        "post",
        "POST"
    );

    fn sql_tables_from_parsed_query(dialect: Option<&str>, query: &str) -> String {
        let Mode::Parsed(ast) = scrub_queries(dialect, query).1 else {
            panic!()
        };
        sql_tables_from_query(query, &Some(ast)).unwrap()
    }

    #[test]
    fn extract_table_select() {
        let query = r#"SELECT * FROM "a.b" WHERE "x" = 1"#;

        assert_eq!(
            sql_tables_from_parsed_query(Some("postgresql"), query),
            ",b,"
        );
    }

    #[test]
    fn extract_table_select_nested() {
        let query = r#"SELECT * FROM (SELECT * FROM "a.b") s WHERE "x" = 1"#;
        assert_eq!(sql_tables_from_parsed_query(None, query), ",b,");
    }

    #[test]
    fn extract_table_multiple() {
        let query = r#"SELECT * FROM a JOIN t.c ON c_id = c.id JOIN b ON b_id = b.id"#;
        assert_eq!(
            sql_tables_from_parsed_query(Some("postgresql"), query),
            ",a,b,c,"
        );
    }

    #[test]
    fn extract_table_multiple_mysql() {
        let query =
            r#"SELECT * FROM a JOIN `t.c` ON /* hello */ c_id = c.id JOIN b ON b_id = b.id"#;
        assert_eq!(
            sql_tables_from_parsed_query(Some("mysql"), query),
            ",a,b,c,"
        );
    }

    #[test]
    fn extract_table_multiple_advanced() {
        let query = r#"
SELECT "sentry_grouprelease"."id", "sentry_grouprelease"."project_id",
  "sentry_grouprelease"."group_id", "sentry_grouprelease"."release_id",
  "sentry_grouprelease"."environment", "sentry_grouprelease"."first_seen",
  "sentry_grouprelease"."last_seen"
FROM "sentry_grouprelease"
WHERE (
  "sentry_grouprelease"."group_id" = %s AND "sentry_grouprelease"."release_id" IN (
    SELECT V0."release_id"
    FROM "sentry_environmentrelease" V0
    WHERE (
      V0."organization_id" = %s AND V0."release_id" IN (
        SELECT U0."release_id"
        FROM "sentry_release_project" U0
        WHERE U0."project_id" = %s
      )
    )
    ORDER BY V0."first_seen" DESC
    LIMIT 1
  )
)
LIMIT 1
            "#;
        assert_eq!(
            sql_tables_from_parsed_query(Some("postgresql"), query),
            ",sentry_environmentrelease,sentry_grouprelease,sentry_release_project,"
        );
    }

    #[test]
    fn extract_table_delete() {
        let query = r#"DELETE FROM "a.b" WHERE "x" = 1"#;
        assert_eq!(sql_tables_from_parsed_query(None, query), ",b,");
    }

    #[test]
    fn extract_table_insert() {
        let query = r#"INSERT INTO "a" ("x", "y") VALUES (%s, %s)"#;
        assert_eq!(
            sql_tables_from_parsed_query(Some("postgresql"), query),
            ",a,"
        );
    }

    #[test]
    fn extract_table_update() {
        let query = r#"UPDATE "a" SET "x" = %s, "y" = %s WHERE "z" = %s"#;
        assert_eq!(
            sql_tables_from_parsed_query(Some("postgresql"), query),
            ",a,"
        );
    }

    #[test]
    fn extract_sql_action() {
        let test_cases = vec![
            (
                r#"SELECT "sentry_organization"."id" FROM "sentry_organization" WHERE "sentry_organization"."id" = %s"#,
                "SELECT",
            ),
            (
                r#"INSERT INTO "sentry_groupseen" ("project_id", "group_id", "user_id", "last_seen") VALUES (%s, %s, %s, %s) RETURNING "sentry_groupseen"."id"#,
                "INSERT",
            ),
            (
                r#"UPDATE sentry_release SET date_released = %s WHERE id = %s"#,
                "UPDATE",
            ),
            (
                r#"DELETE FROM "sentry_groupinbox" WHERE "sentry_groupinbox"."id" IN (%s)"#,
                "DELETE",
            ),
            (r#"SET search_path TO my_schema, public"#, "SET"),
            (r#"SAVEPOINT %s"#, "SAVEPOINT"),
            (r#"RELEASE SAVEPOINT %s"#, "RELEASE SAVEPOINT"),
            (r#"ROLLBACK TO SAVEPOINT %s"#, "ROLLBACK TO SAVEPOINT"),
        ];

        for (query, expected) in test_cases {
            assert_eq!(sql_action_from_query(query).unwrap(), expected)
        }
    }

    #[test]
    fn test_display_times() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "javascript",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    }
                },
                "spans": [
                    {
                        "op": "before_first_display",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    },
                    {
                        "op": "ui.load.initial_display",
                        "span_id": "bd429c44b67a3eb2",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976303.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    },
                    {
                        "span_id": "bd429c44b67a3eb2",
                        "start_timestamp": 1597976303.0000000,
                        "timestamp": 1597976305.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    },
                    {
                        "op": "ui.load.full_display",
                        "span_id": "bd429c44b67a3eb2",
                        "start_timestamp": 1597976304.0000000,
                        "timestamp": 1597976306.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    },
                    {
                        "op": "after_full_display",
                        "span_id": "bd429c44b67a3eb2",
                        "start_timestamp": 1597976307.0000000,
                        "timestamp": 1597976308.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let spans = event.spans.value().unwrap();

        // First two spans contribute to initial display & full display:
        for span in &spans[..2] {
            let tags = span.value().unwrap().sentry_tags.value().unwrap();
            assert_eq!(tags.get_value("ttid").unwrap().as_str(), Some("ttid"));
            assert_eq!(tags.get_value("ttfd").unwrap().as_str(), Some("ttfd"));
        }

        // First four spans contribute to full display:
        for span in &spans[2..4] {
            let tags = span.value().unwrap().sentry_tags.value().unwrap();
            assert_eq!(tags.get_value("ttid"), None);
            assert_eq!(tags.get_value("ttfd").unwrap().as_str(), Some("ttfd"));
        }

        for span in &spans[4..] {
            let tags = span.value().unwrap().sentry_tags.value().unwrap();
            assert_eq!(tags.get_value("ttid"), None);
            assert_eq!(tags.get_value("ttfd"), None);
        }
    }

    #[test]
    fn test_resource_sizes() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "javascript",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    }
                },
                "spans": [
                    {
                        "op": "resource.script",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "data": {
                            "http.response_content_length": 1,
                            "http.decoded_response_content_length": 2.0,
                            "http.response_transfer_size": 3.3
                        }
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let span = &event.spans.value().unwrap()[0];

        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        assert_eq!(
            tags.get_value("http.response_content_length")
                .unwrap()
                .as_str(),
            Some("1"),
        );
        assert_eq!(
            tags.get_value("http.decoded_response_content_length")
                .unwrap()
                .as_str(),
            Some("2"),
        );
        assert_eq!(
            tags.get_value("http.response_transfer_size")
                .unwrap()
                .as_str(),
            Some("3.3"),
        );

        let measurements = span.value().unwrap().measurements.value().unwrap();
        assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "http.decoded_response_content_length": Measurement {
                    value: 2.0,
                    unit: Information(
                        Byte,
                    ),
                },
                "http.response_content_length": Measurement {
                    value: 1.0,
                    unit: Information(
                        Byte,
                    ),
                },
                "http.response_transfer_size": Measurement {
                    value: 3.3,
                    unit: Information(
                        Byte,
                    ),
                },
            },
        )
        "###);
    }

    #[test]
    fn test_resource_raw_domain() {
        let json = r#"
            {
                "spans": [
                    {
                    "timestamp": 1694732408.3145,
                    "start_timestamp": 1694732407.8367,
                    "exclusive_time": 477.800131,
                    "description": "/static/myscript-v1.9.23.js",
                    "op": "resource.script",
                    "span_id": "97c0ef9770a02f9d",
                    "parent_span_id": "9756d8d7b2b364ff",
                    "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                    "data": {
                        "http.decoded_response_content_length": 128950,
                        "http.response_content_length": 36170,
                        "http.response_transfer_size": 36470,
                        "resource.render_blocking_status": "blocking",
                        "server.address": "subdomain.example.com:5688",
                        "url.same_origin": true,
                        "url.scheme": "https"
                    },
                    "hash": "e2fae740cccd3789"
                },
                {
                    "timestamp": 1694732408.3145,
                    "start_timestamp": 1694732407.8367,
                    "exclusive_time": 477.800131,
                    "description": "/static/myscript-v1.9.23.js",
                    "op": "resource.script",
                    "span_id": "97c0ef9770a02f9d",
                    "parent_span_id": "9756d8d7b2b364ff",
                    "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                    "data": {
                        "http.decoded_response_content_length": 128950,
                        "http.response_content_length": 36170,
                        "http.response_transfer_size": 36470,
                        "resource.render_blocking_status": "blocking",
                        "server.address": "example.com",
                        "url.same_origin": true,
                        "url.scheme": "http"
                    },
                    "hash": "e2fae740cccd3781"
                },
                {
                    "timestamp": 1694732408.3145,
                    "start_timestamp": 1694732407.8367,
                    "exclusive_time": 477.800131,
                    "description": "/static/myscript-v1.9.24.js",
                    "op": "resource.script",
                    "span_id": "97c0ef9770a02f9d",
                    "parent_span_id": "9756d8d7b2b364ff",
                    "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                    "data": {
                        "http.decoded_response_content_length": 128950,
                        "http.response_content_length": 36170,
                        "http.response_transfer_size": 36470,
                        "resource.render_blocking_status": "blocking"
                    },
                    "hash": "e2fae740cccd3788"
                }
            ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let span_1 = &event.spans.value().unwrap()[0];
        let span_2 = &event.spans.value().unwrap()[1];
        let span_3 = &event.spans.value().unwrap()[2];

        let tags_1 = get_value!(span_1.sentry_tags).unwrap();
        let tags_2 = get_value!(span_2.sentry_tags).unwrap();
        let tags_3 = get_value!(span_3.sentry_tags).unwrap();

        assert_eq!(
            tags_1.raw_domain.as_str(),
            Some("https://subdomain.example.com:5688")
        );
        assert_eq!(tags_2.raw_domain.as_str(), Some("http://example.com"));
        assert!(!tags_3.raw_domain.value().is_some());
    }

    #[test]
    fn test_ai_extraction() {
        let json = r#"
            {
                "spans": [
                    {
                        "timestamp": 1694732408.3145,
                        "start_timestamp": 1694732407.8367,
                        "exclusive_time": 477.800131,
                        "description": "OpenAI Chat Completion",
                        "op": "ai.chat_completions.openai",
                        "span_id": "97c0ef9770a02f9d",
                        "parent_span_id": "9756d8d7b2b364ff",
                        "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                        "data": {
                            "ai.total_tokens.used": 300,
                            "ai.completion_tokens.used": 200,
                            "ai.prompt_tokens.used": 100,
                            "ai.streaming": true,
                            "ai.pipeline.name": "My AI pipeline"
                        },
                        "hash": "e2fae740cccd3781"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let span = &event
            .spans
            .value()
            .unwrap()
            .first()
            .unwrap()
            .value()
            .unwrap();
        let tags = span.sentry_tags.value().unwrap();
        let measurements = span.measurements.value().unwrap();

        assert_eq!(
            tags.get_value("ai_pipeline_group").unwrap().as_str(),
            Some("68e6cafc5b68d276")
        );
        assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "ai_completion_tokens_used": Measurement {
                    value: 200.0,
                    unit: None,
                },
                "ai_prompt_tokens_used": Measurement {
                    value: 100.0,
                    unit: None,
                },
                "ai_total_tokens_used": Measurement {
                    value: 300.0,
                    unit: None,
                },
            },
        )
        "###);
    }

    #[test]
    fn test_cache_extraction() {
        let json = r#"
            {
                "spans": [
                    {
                        "timestamp": 1694732408.3145,
                        "start_timestamp": 1694732407.8367,
                        "exclusive_time": 477.800131,
                        "description": "get my_key",
                        "op": "cache.get_item",
                        "span_id": "97c0ef9770a02f9d",
                        "parent_span_id": "9756d8d7b2b364ff",
                        "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                        "data": {
                            "cache.hit": true,
                            "cache.key": ["my_key"],
                            "cache.item_size": 8,
                            "thread.id": "6286962688",
                            "thread.name": "Thread-4 (process_request_thread)"

                        },
                        "hash": "e2fae740cccd3781"
                    },
                    {
                        "timestamp": 1694732409.3145,
                        "start_timestamp": 1694732408.8367,
                        "exclusive_time": 477.800131,
                        "description": "mget my_key my_key_2",
                        "op": "cache.get_item",
                        "span_id": "97c0ef9770a02f9d",
                        "parent_span_id": "9756d8d7b2b364ff",
                        "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                        "data": {
                            "cache.hit": false,
                            "cache.key": ["my_key", "my_key_2"],
                            "cache.item_size": 8,
                            "thread.id": "6286962688",
                            "thread.name": "Thread-4 (process_request_thread)"

                        },
                        "hash": "e2fae740cccd3781"
                    },
                    {
                        "timestamp": 1694732409.3145,
                        "start_timestamp": 1694732408.8367,
                        "exclusive_time": 477.800131,
                        "description": "get my_key_2",
                        "op": "cache.get",
                        "span_id": "97c0ef9770a02f9d",
                        "parent_span_id": "9756d8d7b2b364ff",
                        "trace_id": "77aeb1c16bb544a4a39b8d42944947a3",
                        "data": {
                            "cache.hit": false,
                            "cache.key": ["my_key_2"],
                            "cache.item_size": 8,
                            "thread.id": "6286962688",
                            "thread.name": "Thread-4 (process_request_thread)"

                        },
                        "hash": "e2fae740cccd3781"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let span_1 = &event.spans.value().unwrap()[0];
        let span_2 = &event.spans.value().unwrap()[1];
        let span_3 = &event.spans.value().unwrap()[2];

        let tags_1 = get_value!(span_1.sentry_tags).unwrap();
        let tags_2 = get_value!(span_2.sentry_tags).unwrap();
        let tags_3 = get_value!(span_3.sentry_tags).unwrap();

        let measurements_1 = span_1.value().unwrap().measurements.value().unwrap();

        assert_eq!(tags_1.cache_hit.as_str(), Some("true"));
        assert_eq!(tags_2.cache_hit.as_str(), Some("false"));
        assert_eq!(tags_3.cache_hit.as_str(), Some("false"));

        let keys_1 = Value::Array(vec![Annotated::new(Value::String("my_key".to_string()))]);
        let keys_2 = Value::Array(vec![
            Annotated::new(Value::String("my_key".to_string())),
            Annotated::new(Value::String("my_key_2".to_string())),
        ]);
        let keys_3 = Value::Array(vec![Annotated::new(Value::String("my_key_2".to_string()))]);
        assert_eq!(
            tags_1.cache_key.as_str(),
            serde_json::to_string(&keys_1).ok().as_deref()
        );
        assert_eq!(
            tags_2.cache_key.as_str(),
            serde_json::to_string(&keys_2).ok().as_deref()
        );
        assert_eq!(
            tags_3.cache_key.as_str(),
            serde_json::to_string(&keys_3).ok().as_deref()
        );

        assert_debug_snapshot!(measurements_1, @r###"
        Measurements(
            {
                "cache.item_size": Measurement {
                    value: 8.0,
                    unit: Information(
                        Byte,
                    ),
                },
            },
        )
        "###);
    }

    #[test]
    fn test_http_client_domain() {
        let json = r#"
            {
                "spans": [
                    {
                        "timestamp": 1711007391.89278,
                        "start_timestamp": 1711007391.891537,
                        "exclusive_time": 1.243114,
                        "description": "POST http://127.0.0.1:10007/data",
                        "op": "http.client",
                        "span_id": "8e635823db6a742a",
                        "parent_span_id": "a1bdf3c7d2afe10e",
                        "trace_id": "2920522dedff493ebe5d84da7be4319f",
                        "data": {
                            "http.request_method": "POST",
                            "http.response.status_code": 200,
                            "http.fragment": "",
                            "http.query": "",
                            "reason": "OK",
                            "url": "http://127.0.0.1:10007/data"
                        },
                        "hash": "8e7b6caca435801d",
                        "same_process_as_parent": true
                    },
                    {
                        "timestamp": 1711007391.036243,
                        "start_timestamp": 1711007391.034472,
                        "exclusive_time": 1.770973,
                        "description": "GET http://8.8.8.8/",
                        "op": "http.client",
                        "span_id": "872834c747983b2f",
                        "parent_span_id": "a1bdf3c7d2afe10e",
                        "trace_id": "2920522dedff493ebe5d84da7be4319f",
                        "data": {
                            "http.request_method": "GET",
                            "http.response.status_code": 200,
                            "http.fragment": "",
                            "http.query": "",
                            "reason": "OK",
                            "url": "http://8.8.8.8/"
                        },
                        "hash": "8e7b6caca435801d",
                        "same_process_as_parent": true
                    },
                    {
                        "timestamp": 1711007391.034472,
                        "start_timestamp": 1711007391.217212,
                        "exclusive_time": 0.18274,
                        "description": "GET http://data.application.co.uk/feed.json",
                        "op": "http.client",
                        "span_id": "37983b2fc748728f",
                        "parent_span_id": "a1bdf3c7d2afe10e",
                        "trace_id": "2920522dedff493ebe5d84da7be4319f",
                        "data": {
                            "http.request_method": "GET",
                            "http.response.status_code": 200,
                            "http.fragment": "",
                            "http.query": "",
                            "reason": "OK",
                            "url": "http://data.application.co.uk/feed.json"
                        },
                        "hash": "6a4358018e7bdcac",
                        "same_process_as_parent": true
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let span_1 = &event.spans.value().unwrap()[0];
        let span_2 = &event.spans.value().unwrap()[1];
        let span_3 = &event.spans.value().unwrap()[2];

        let tags_1 = get_value!(span_1.sentry_tags).unwrap();
        let tags_2 = get_value!(span_2.sentry_tags).unwrap();
        let tags_3 = get_value!(span_3.sentry_tags).unwrap();

        // Allow loopback IPs
        assert_eq!(
            tags_1.description.as_str(),
            Some("POST http://127.0.0.1:10007")
        );
        assert_eq!(tags_1.domain.as_str(), Some("127.0.0.1:10007"));

        // Scrub other IPs
        assert_eq!(tags_2.description.as_str(), Some("GET http://*.*.*.*"));
        assert_eq!(tags_2.domain.as_str(), Some("*.*.*.*"));

        // Parse ccTLDs
        assert_eq!(
            tags_3.description.as_str(),
            Some("GET http://*.application.co.uk")
        );
        assert_eq!(tags_3.domain.as_str(), Some("*.application.co.uk"));
    }

    #[test]
    fn test_mobile_specific_tags() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "android",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "sdk": {"name": "sentry.java.android"},
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    },
                    "app": {
                        "app_identifier": "io.sentry.samples.android",
                        "app_name": "sentry_android_example"
                    },
                    "os": {
                        "name": "Android",
                        "version": "8.1.0"
                    }
                },
                "measurements": {
                    "app_start_warm": {
                        "value": 1.0,
                        "unit": "millisecond"
                    }
                },
                "spans": [
                    {
                        "op": "ui.load",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "data": {
                            "thread.id": 1,
                            "thread.name": "main",
                            "app_start_type": "cold"
                        }
                    },
                    {
                        "op": "ui.load",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "data": {
                            "thread.id": 2,
                            "thread.name": "not main"
                        }
                    },
                    {
                        "op": "file.write",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let span = &event.spans.value().unwrap()[0];

        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        assert_eq!(
            tags.get_value("main_thread").unwrap().as_str(),
            Some("true")
        );
        assert_eq!(tags.get_value("os.name").unwrap().as_str(), Some("Android"));
        assert_eq!(
            tags.get_value("app_start_type").unwrap().as_str(),
            Some("cold")
        );

        let span = &event.spans.value().unwrap()[1];

        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        assert_eq!(tags.get_value("main_thread"), None);
        assert_eq!(
            tags.get_value("app_start_type").unwrap().as_str(),
            Some("warm")
        );

        let span = &event.spans.value().unwrap()[2];

        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        assert_eq!(tags.get_value("main_thread"), None);
        assert_eq!(
            tags.get_value("app_start_type").unwrap().as_str(),
            Some("warm")
        );
    }

    #[test]
    fn test_span_tags_extraction_from_event_browser_name() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "javascript",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    },
                    "browser": {
                        "name": "Chrome"
                    }
                },
                "spans": [
                    {
                        "op": "resource.script",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let span = &event.spans.value().unwrap()[0];
        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        assert_eq!(tags.browser_name.as_str(), Some("Chrome"));
    }

    #[test]
    fn test_span_tags_extraction_from_span_browser_name() {
        let json = r#"
            {
                "op": "resource.script",
                "span_id": "bd429c44b67a3eb1",
                "start_timestamp": 1597976300.0000000,
                "timestamp": 1597976302.0000000,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
                "data": {
                    "browser.name": "Chrome"
                }
            }
        "#;
        let span: Span = Annotated::<Span>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();
        let tags = extract_tags(&span, 200, None, None, false, None, &[]);

        assert_eq!(tags.browser_name.value(), Some(&"Chrome".to_string()));
    }

    #[test]
    fn test_extract_trace_status() {
        let json = r#"

            {
                "type": "transaction",
                "platform": "python",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "status": "ok"
                    }
                },
                "spans": [
                    {
                        "op": "resource.script",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let span = &event.spans.value().unwrap()[0];
        let tags = span.value().unwrap().sentry_tags.value().unwrap();

        assert_eq!(tags.trace_status.as_str(), Some("ok"));
    }

    #[test]
    fn test_queue_tags() {
        let json = r#"
            {
                "op": "queue.task",
                "span_id": "bd429c44b67a3eb1",
                "start_timestamp": 1597976300.0000000,
                "timestamp": 1597976302.0000000,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
                "data": {
                    "messaging.destination.name": "default",
                    "messaging.message.id": "abc123",
                    "messaging.message.body.size": 100
                }
            }
        "#;
        let span: Span = Annotated::<Span>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();
        let tags = extract_tags(&span, 200, None, None, false, None, &[]);

        assert_eq!(
            tags.messaging_destination_name.value(),
            Some(&"default".to_string())
        );
        assert_eq!(
            tags.messaging_message_id.value(),
            Some(&"abc123".to_string())
        );
    }

    #[test]
    fn test_extract_segment_queue_tags_and_measurement_from_transaction() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "python",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "op": "queue.process",
                        "status": "ok",
                        "data": {
                            "messaging.destination.name": "default",
                            "messaging.message.id": "abc123",
                            "messaging.message.receive.latency": 456,
                            "messaging.message.body.size": 100,
                            "messaging.message.retry.count": 3
                        }
                    }
                }
            }
        "#;

        let event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();
        let mut spans = [Span::from(&event).into()];

        extract_segment_span_tags(&event, &mut spans);

        let segment_span: &Annotated<Span> = &spans[0];
        let tags = segment_span.value().unwrap().sentry_tags.value().unwrap();
        let measurements = segment_span.value().unwrap().measurements.value().unwrap();

        assert_eq!(tags.messaging_destination_name.as_str(), Some("default"));

        assert_eq!(tags.messaging_message_id.as_str(), Some("abc123"));

        assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "messaging.message.body.size": Measurement {
                    value: 100.0,
                    unit: Information(
                        Byte,
                    ),
                },
                "messaging.message.receive.latency": Measurement {
                    value: 456.0,
                    unit: Duration(
                        MilliSecond,
                    ),
                },
                "messaging.message.retry.count": Measurement {
                    value: 3.0,
                    unit: None,
                },
            },
        )
        "###);
    }

    #[test]
    fn test_does_not_extract_segment_tags_and_measurements_on_child_spans() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "python",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "op": "queue.process",
                        "status": "ok",
                        "data": {
                            "messaging.destination.name": "default",
                            "messaging.message.id": "abc123",
                            "messaging.message.receive.latency": 456,
                            "messaging.message.body.size": 100,
                            "messaging.message.retry.count": 3
                        }
                    }
                },
                "spans": [
                    {
                        "op": "queue.process",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "data": {
                            "messaging.message.body.size": 200
                        }
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();

        extract_span_tags_from_event(&mut event, 200, &[]);

        let span = &event.spans.value().unwrap()[0];
        let tags = span.value().unwrap().sentry_tags.value().unwrap();
        let measurements = span.value().unwrap().measurements.value().unwrap();

        assert_eq!(tags.get_value("messaging.destination.name"), None);
        assert_eq!(tags.get_value("messaging.message.id"), None);

        assert_debug_snapshot!(measurements, @r###"
        Measurements(
            {
                "messaging.message.body.size": Measurement {
                    value: 200.0,
                    unit: Information(
                        Byte,
                    ),
                },
            },
        )
        "###);
    }

    #[test]
    fn extract_span_status_into_sentry_tags() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "javascript",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    }
                },
                "spans": [
                    {
                        "op": "before_first_display",
                        "span_id": "bd429c44b67a3eb1",
                        "status": "success",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    },
                    {
                        "op": "before_first_display",
                        "span_id": "bd429c44b67a3eb1",
                        "status": "invalid_argument",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                enrich_spans: true,
                ..Default::default()
            },
        );

        let spans = get_value!(event.spans!);

        let statuses: Vec<_> = spans
            .iter()
            .map(|span| get_value!(span.sentry_tags.status_code!))
            .collect();

        assert_eq!(statuses, vec!["ok", "invalid_argument"]);
    }

    fn extract_tags_supabase(description: impl Into<String>) -> SentryTags {
        let json = r#"{
            "description": "from(my_table)",
            "op": "db.select",
            "origin": "auto.db.supabase",
            "data": {
                "query": [
                    "select(*,other(*))",
                    "in(something, (value1,value2))"
                ]
            }
        }"#;

        let mut span = Annotated::<Span>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();
        span.description.set_value(Some(description.into()));

        extract_tags(&span, 200, None, None, false, None, &[])
    }

    #[test]
    fn supabase() {
        let tags = extract_tags_supabase("from(mytable)");
        assert_eq!(
            tags.description.value().map(String::as_str),
            Some("from(mytable)")
        );
        assert_eq!(tags.domain.value().map(String::as_str), Some("mytable"));
    }

    #[test]
    fn supabase_with_identifiers() {
        let tags = extract_tags_supabase("from(my_table00)");

        assert_eq!(
            tags.description.value().map(String::as_str),
            Some("from(my_table{%s})")
        );
        assert_eq!(
            tags.domain.value().map(String::as_str),
            Some("my_table{%s}")
        );
    }

    #[test]
    fn supabase_unsupported() {
        let tags = extract_tags_supabase("something else");

        assert_eq!(tags.description.value(), None);
        assert_eq!(tags.domain.value(), None);
    }

    #[test]
    fn mongodb() {
        let json = r#"
            {
                "op": "db",
                "span_id": "bd429c44b67a3eb1",
                "start_timestamp": 1597976300.0000000,
                "timestamp": 1597976302.0000000,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
                "data": {
                    "db.operation": "find",
                    "db.collection.name": "documents",
                    "db.system": "mongodb"
                }
            }
        "#;
        let span: Span = Annotated::<Span>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();
        let tags = extract_tags(&span, 200, None, None, false, None, &[]);

        assert_eq!(tags.action.value(), Some(&"FIND".to_string()));

        assert_eq!(tags.domain.value(), Some(&"documents".to_string()));
    }

    #[test]
    fn mongodb_collection_name_scrubbing() {
        let json = r#"
            {
                "op": "db",
                "span_id": "bd429c44b67a3eb1",
                "start_timestamp": 1597976300.0000000,
                "timestamp": 1597976302.0000000,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
                "data": {
                    "db.operation": "find",
                    "db.collection.name": "documents_a1b2c3d4",
                    "db.system": "mongodb"
                }
            }
        "#;
        let span: Span = Annotated::<Span>::from_json(json)
            .unwrap()
            .into_value()
            .unwrap();
        let tags = extract_tags(&span, 200, None, None, false, None, &[]);

        assert_eq!(tags.domain.value(), Some(&"documents_{%s}".to_string()));
    }

    #[test]
    fn extract_profiler_id_into_sentry_tags() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "javascript",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "profile": {
                        "profiler_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                },
                "spans": [
                    {
                        "op": "before_first_display",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                enrich_spans: true,
                ..Default::default()
            },
        );

        let spans = get_value!(event.spans!);
        let span = &spans[0];

        assert_eq!(
            get_value!(span.sentry_tags.profiler_id!),
            "ff62a8b040f340bda5d830223def1d81",
        );
    }

    #[test]
    fn extract_user_into_sentry_tags() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "javascript",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    },
                    "browser": {
                        "name": "Chrome"
                    }
                },
                "user": {
                    "id": "1",
                    "ip_address": "127.0.0.1",
                    "email": "admin@sentry.io",
                    "username": "admin",
                    "geo": {
                        "country_code": "US"
                    }
                },
                "spans": [
                    {
                        "op": "before_first_display",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                enrich_spans: true,
                ..Default::default()
            },
        );

        let spans = get_value!(event.spans!);
        let span = &spans[0];

        assert_eq!(get_value!(span.sentry_tags.user!), "id:1");
        assert_eq!(get_value!(span.sentry_tags.user_id!), "1");
        assert_eq!(get_value!(span.sentry_tags.user_ip!), "127.0.0.1");
        assert_eq!(get_value!(span.sentry_tags.user_username!), "admin");
        assert_eq!(get_value!(span.sentry_tags.user_email!), "admin@sentry.io");
        assert_eq!(get_value!(span.sentry_tags.user_country_code!), "US");
        assert_eq!(get_value!(span.sentry_tags.user_subregion!), "21");
    }

    #[test]
    fn not_extract_geo_location_if_not_browser() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "python",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    },
                    "browser": {
                        "name": "Chrome"
                    }
                },
                "user": {
                    "id": "1",
                    "email": "admin@sentry.io",
                    "username": "admin",
                    "geo": {
                        "country_code": "US"
                    }
                },
                "spans": [
                    {
                        "op": "http.client",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                enrich_spans: true,
                ..Default::default()
            },
        );

        let spans = get_value!(event.spans!);
        let span = &spans[0];

        let tags = span.value().unwrap().sentry_tags.value().unwrap();

        assert_eq!(tags.get_value("user.geo.subregion"), None);
        assert_eq!(tags.get_value("user.geo.country_code"), None);
    }

    #[test]
    fn extract_thread_id_name_from_span_data_into_sentry_tags() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "javascript",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "span_id": "bd429c44b67a3eb4"
                    }
                },
                "spans": [
                    {
                        "op": "before_first_display",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81",
                        "data": {
                            "thread.name": "main",
                            "thread.id": 42
                        }
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                enrich_spans: true,
                ..Default::default()
            },
        );

        let spans = get_value!(event.spans!);
        let span = &spans[0];

        assert_eq!(get_value!(span.sentry_tags.thread_id!), "42",);
        assert_eq!(get_value!(span.sentry_tags.thread_name!), "main",);
    }

    #[test]
    fn extract_thread_id_name_from_trace_context_into_sentry_tags() {
        let json = r#"
            {
                "type": "transaction",
                "platform": "python",
                "start_timestamp": "2021-04-26T07:59:01+0100",
                "timestamp": "2021-04-26T08:00:00+0100",
                "transaction": "foo",
                "contexts": {
                    "trace": {
                        "op": "queue.process",
                        "status": "ok",
                        "data": {
                            "thread.name": "main",
                            "thread.id": 42
                        }
                    }
                },
                "spans": [
                    {
                        "op": "before_first_display",
                        "span_id": "bd429c44b67a3eb1",
                        "start_timestamp": 1597976300.0000000,
                        "timestamp": 1597976302.0000000,
                        "trace_id": "ff62a8b040f340bda5d830223def1d81"
                    }
                ]
            }
        "#;

        let mut event = Annotated::<Event>::from_json(json).unwrap();

        normalize_event(
            &mut event,
            &NormalizationConfig {
                enrich_spans: true,
                ..Default::default()
            },
        );

        let spans = get_value!(event.spans!);
        let span = &spans[0];

        assert_eq!(get_value!(span.sentry_tags.thread_id!), "42",);
        assert_eq!(get_value!(span.sentry_tags.thread_name!), "main",);
    }
}
