//! Event processor related code.

use std::error::Error;
use std::sync::{Arc, OnceLock};

use chrono::Duration as SignedDuration;
use relay_auth::RelayVersion;
use relay_base_schema::events::EventType;
use relay_config::Config;
use relay_dynamic_config::GlobalConfig;
use relay_event_normalization::ClockDriftProcessor;
use relay_event_schema::processor::{self, ProcessingState};
use relay_event_schema::protocol::{
    Breadcrumb, Csp, Event, ExpectCt, ExpectStaple, Hpkp, LenientString, Metrics, OtelContext,
    RelayInfo, SecurityReportType, Values,
};
use relay_pii::PiiProcessor;
use relay_protocol::{Annotated, Array, Empty, Object, Value};
use relay_quotas::DataCategory;
use relay_statsd::metric;
use serde_json::Value as SerdeValue;

use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::extractors::RequestMeta;
use crate::managed::TypedEnvelope;
use crate::services::outcome::Outcome;
use crate::services::processor::{
    EventFullyNormalized, EventMetricsExtracted, EventProcessing, ExtractedEvent,
    MINIMUM_CLOCK_DRIFT, ProcessingError, SpansExtracted, event_category, event_type,
};
use crate::services::projects::project::ProjectInfo;
use crate::statsd::{PlatformTag, RelayCounters, RelayHistograms, RelayTimers};
use crate::utils::{self, ChunkedFormDataAggregator, FormDataIter};

/// Result of the extraction of the primary event payload from an envelope.
#[derive(Debug)]
pub struct ExtractionResult {
    pub event: Annotated<Event>,
    pub event_metrics_extracted: Option<EventMetricsExtracted>,
    pub spans_extracted: Option<SpansExtracted>,
}

/// Extracts the primary event payload from an envelope.
///
/// The event is obtained from only one source in the following precedence:
///  1. An explicit event item. This is also the case for JSON uploads.
///  2. A security report item.
///  3. Attachments `__sentry-event` and `__sentry-breadcrumb1/2`.
///  4. A multipart form data body.
///  5. If none match, `Annotated::empty()`.
pub fn extract<Group: EventProcessing>(
    managed_envelope: &mut TypedEnvelope<Group>,
    metrics: &mut Metrics,
    event_fully_normalized: EventFullyNormalized,
    config: &Config,
) -> Result<ExtractionResult, ProcessingError> {
    let envelope = managed_envelope.envelope_mut();

    // Remove all items first, and then process them. After this function returns, only
    // attachments can remain in the envelope. The event will be added again at the end of
    // `process_event`.
    let event_item = envelope.take_item_by(|item| item.ty() == &ItemType::Event);
    let transaction_item = envelope.take_item_by(|item| item.ty() == &ItemType::Transaction);
    let security_item = envelope.take_item_by(|item| item.ty() == &ItemType::Security);
    let raw_security_item = envelope.take_item_by(|item| item.ty() == &ItemType::RawSecurity);
    let user_report_v2_item = envelope.take_item_by(|item| item.ty() == &ItemType::UserReportV2);
    let form_item = envelope.take_item_by(|item| item.ty() == &ItemType::FormData);
    let attachment_item =
        envelope.take_item_by(|item| item.attachment_type() == Some(&AttachmentType::EventPayload));
    let breadcrumbs1 =
        envelope.take_item_by(|item| item.attachment_type() == Some(&AttachmentType::Breadcrumbs));
    let breadcrumbs2 =
        envelope.take_item_by(|item| item.attachment_type() == Some(&AttachmentType::Breadcrumbs));

    // Event items can never occur twice in an envelope.
    if let Some(duplicate) =
        envelope.get_item_by(|item| is_duplicate(item, config.processing_enabled()))
    {
        return Err(ProcessingError::DuplicateItem(duplicate.ty().clone()));
    }

    let skip_normalization = config.processing_enabled() && event_fully_normalized.0;

    let mut event_metrics_extracted = None;
    let mut spans_extracted = None;
    let (event, event_len) = if let Some(item) = event_item.or(security_item) {
        relay_log::trace!("processing json event");
        metric!(timer(RelayTimers::EventProcessingDeserialize), {
            let (mut annotated_event, len) = event_from_json_payload(item, None)?;
            // Event items can never include transactions, so retain the event type and let
            // inference deal with this during normalization.
            if let Some(event) = annotated_event.value_mut() {
                if !skip_normalization {
                    event.ty.set_value(None);
                }
            }
            (annotated_event, len)
        })
    } else if let Some(item) = transaction_item {
        relay_log::trace!("processing json transaction");

        event_metrics_extracted = Some(EventMetricsExtracted(item.metrics_extracted()));
        spans_extracted = Some(SpansExtracted(item.spans_extracted()));

        metric!(timer(RelayTimers::EventProcessingDeserialize), {
            // Transaction items can only contain transaction events. Force the event type to
            // hint to normalization that we're dealing with a transaction now.
            event_from_json_payload(item, Some(EventType::Transaction))?
        })
    } else if let Some(item) = user_report_v2_item {
        relay_log::trace!("processing user_report_v2");
        event_from_json_payload(item, Some(EventType::UserReportV2))?
    } else if let Some(item) = raw_security_item {
        relay_log::trace!("processing security report");
        event_from_security_report(item, envelope.meta()).map_err(|error| {
            if !matches!(error, ProcessingError::UnsupportedSecurityType) {
                relay_log::error!(
                    error = &error as &dyn Error,
                    "failed to extract security report"
                );
            }
            error
        })?
    } else if attachment_item.is_some() || breadcrumbs1.is_some() || breadcrumbs2.is_some() {
        relay_log::trace!("extracting attached event data");
        event_from_attachments(config, attachment_item, breadcrumbs1, breadcrumbs2)?
    } else if let Some(item) = form_item {
        relay_log::trace!("extracting form data");
        let len = item.len();

        let mut value = SerdeValue::Object(Default::default());
        merge_formdata(&mut value, item);
        let event = Annotated::deserialize_with_meta(value).unwrap_or_default();

        (event, len)
    } else {
        relay_log::trace!("no event in envelope");
        (Annotated::empty(), 0)
    };

    metrics.bytes_ingested_event = Annotated::new(event_len as u64);

    Ok(ExtractionResult {
        event,
        event_metrics_extracted,
        spans_extracted,
    })
}

pub fn finalize<Group: EventProcessing>(
    managed_envelope: &mut TypedEnvelope<Group>,
    event: &mut Annotated<Event>,
    metrics: &mut Metrics,
    config: &Config,
) -> Result<(), ProcessingError> {
    let envelope = managed_envelope.envelope_mut();

    let inner_event = match event.value_mut() {
        Some(event) => event,
        None if !config.processing_enabled() => return Ok(()),
        None => return Err(ProcessingError::NoEventPayload),
    };

    if !config.processing_enabled() {
        static MY_VERSION_STRING: OnceLock<String> = OnceLock::new();
        let my_version = MY_VERSION_STRING.get_or_init(|| RelayVersion::current().to_string());

        inner_event
            .ingest_path
            .get_or_insert_with(Default::default)
            .push(Annotated::new(RelayInfo {
                version: Annotated::new(my_version.clone()),
                public_key: config
                    .public_key()
                    .map_or(Annotated::empty(), |pk| Annotated::new(pk.to_string())),
                other: Default::default(),
            }));
    }

    // Event id is set statically in the ingest path.
    let event_id = envelope.event_id().unwrap_or_default();
    debug_assert!(!event_id.is_nil());

    // Ensure that the event id in the payload is consistent with the envelope. If an event
    // id was ingested, this will already be the case. Otherwise, this will insert a new
    // event id. To be defensive, we always overwrite to ensure consistency.
    inner_event.id = Annotated::new(event_id);

    // In processing mode, also write metrics into the event. Most metrics have already been
    // collected at this state, except for the combined size of all attachments.
    if config.processing_enabled() {
        let mut metrics = std::mem::take(metrics);

        let attachment_size = envelope
            .items()
            .filter(|item| item.attachment_type() == Some(&AttachmentType::Attachment))
            .map(|item| item.len() as u64)
            .sum::<u64>();

        if attachment_size > 0 {
            metrics.bytes_ingested_event_attachment = Annotated::new(attachment_size);
        }

        inner_event._metrics = Annotated::new(metrics);

        if inner_event.ty.value() == Some(&EventType::Transaction) {
            metric!(
                counter(RelayCounters::EventTransaction) += 1,
                source = utils::transaction_source_tag(inner_event),
                platform =
                    PlatformTag::from(inner_event.platform.as_str().unwrap_or("other")).name(),
                contains_slashes = if inner_event
                    .transaction
                    .as_str()
                    .unwrap_or_default()
                    .contains('/')
                {
                    "true"
                } else {
                    "false"
                }
            );

            let span_count = inner_event.spans.value().map(Vec::len).unwrap_or(0) as u64;
            metric!(
                histogram(RelayHistograms::EventSpans) = span_count,
                sdk = envelope.meta().client_name().name(),
                platform = inner_event.platform.as_str().unwrap_or("other"),
            );

            let has_otel = inner_event
                .contexts
                .value()
                .is_some_and(|contexts| contexts.contains::<OtelContext>());

            if has_otel {
                metric!(
                    counter(RelayCounters::OpenTelemetryEvent) += 1,
                    sdk = envelope.meta().client_name().name(),
                    platform = inner_event.platform.as_str().unwrap_or("other"),
                );
            }
        }

        if let Some(dsc) = envelope.dsc() {
            if let Ok(Some(value)) = relay_protocol::to_value(dsc) {
                inner_event._dsc = Annotated::new(value);
            }
        }
    }

    let mut processor =
        ClockDriftProcessor::new(envelope.sent_at(), managed_envelope.received_at())
            .at_least(MINIMUM_CLOCK_DRIFT);
    processor::process_value(event, &mut processor, ProcessingState::root())
        .map_err(|_| ProcessingError::InvalidTransaction)?;

    // Log timestamp delays for all events after clock drift correction. This happens before
    // store processing, which could modify the timestamp if it exceeds a threshold. We are
    // interested in the actual delay before this correction.
    if let Some(timestamp) = event.value().and_then(|e| e.timestamp.value()) {
        let event_delay = managed_envelope.received_at() - timestamp.into_inner();
        if event_delay > SignedDuration::minutes(1) {
            let category = event_category(event).unwrap_or(DataCategory::Unknown);
            metric!(
                timer(RelayTimers::TimestampDelay) = event_delay.to_std().unwrap(),
                category = category.name(),
            );
        }
    }

    Ok(())
}

/// Status for applying some filters that don't drop the event.
///
/// The enum represents either the success of running all filters and keeping
/// the event, [`FiltersStatus::Ok`], or not running all the filters because
/// some are unsupported, [`FiltersStatus::Unsupported`].
///
/// If there are unsuppported filters, Relay should forward the event upstream
/// so that a more up-to-date Relay can apply filters appropriately. Actions
/// that depend on the outcome of event filtering, such as metric extraction,
/// should be postponed until a filtering decision is made.
#[must_use]
pub enum FiltersStatus {
    /// All filters have been applied and the event should be kept.
    Ok,
    /// Some filters are not supported and were not applied.
    ///
    /// Relay should forward events upstream for a more up-to-date Relay to apply these filters.
    /// Supported filters were applied and they don't reject the event.
    Unsupported,
}

pub fn filter<Group: EventProcessing>(
    managed_envelope: &mut TypedEnvelope<Group>,
    event: &mut Annotated<Event>,
    project_info: Arc<ProjectInfo>,
    global_config: &GlobalConfig,
) -> Result<FiltersStatus, ProcessingError> {
    let event = match event.value_mut() {
        Some(event) => event,
        // Some events are created by processing relays (e.g. unreal), so they do not yet
        // exist at this point in non-processing relays.
        None => return Ok(FiltersStatus::Ok),
    };

    let client_ip = managed_envelope.envelope().meta().client_addr();
    let filter_settings = &project_info.config.filter_settings;

    metric!(timer(RelayTimers::EventProcessingFiltering), {
        relay_filter::should_filter(event, client_ip, filter_settings, global_config.filters())
            .map_err(|err| {
                managed_envelope.reject(Outcome::Filtered(err.clone()));
                ProcessingError::EventFiltered(err)
            })
    })?;

    // Don't extract metrics if relay can't apply generic filters.  A filter
    // applied in another up-to-date relay in chain may need to drop the event,
    // and there should not be metrics from dropped events.
    // In processing relays, always extract metrics to avoid losing them.
    let supported_generic_filters = global_config.filters.is_ok()
        && relay_filter::are_generic_filters_supported(
            global_config.filters().map(|f| f.version),
            project_info.config.filter_settings.generic.version,
        );
    if supported_generic_filters {
        Ok(FiltersStatus::Ok)
    } else {
        Ok(FiltersStatus::Unsupported)
    }
}

/// Apply data privacy rules to the event payload.
///
/// This uses both the general `datascrubbing_settings`, as well as the the PII rules.
pub fn scrub(
    event: &mut Annotated<Event>,
    project_info: Arc<ProjectInfo>,
) -> Result<(), ProcessingError> {
    let config = &project_info.config;

    if config.datascrubbing_settings.scrub_data {
        if let Some(event) = event.value_mut() {
            relay_pii::scrub_graphql(event);
        }
    }

    metric!(timer(RelayTimers::EventProcessingPii), {
        if let Some(ref config) = config.pii_config {
            let mut processor = PiiProcessor::new(config.compiled());
            processor::process_value(event, &mut processor, ProcessingState::root())?;
        }
        let pii_config = config
            .datascrubbing_settings
            .pii_config()
            .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?;
        if let Some(config) = pii_config {
            let mut processor = PiiProcessor::new(config.compiled());
            processor::process_value(event, &mut processor, ProcessingState::root())?;
        }
    });

    Ok(())
}

pub fn serialize<Group: EventProcessing>(
    managed_envelope: &mut TypedEnvelope<Group>,
    event: &mut Annotated<Event>,
    event_fully_normalized: EventFullyNormalized,
    event_metrics_extracted: EventMetricsExtracted,
    spans_extracted: SpansExtracted,
) -> Result<(), ProcessingError> {
    if event.is_empty() {
        relay_log::error!("Cannot serialize empty event");
        return Ok(());
    }

    let data = metric!(timer(RelayTimers::EventProcessingSerialization), {
        event.to_json().map_err(ProcessingError::SerializeFailed)?
    });

    let event_type = event_type(event).unwrap_or_default();
    let mut event_item = Item::new(ItemType::from_event_type(event_type));
    event_item.set_payload(ContentType::Json, data);

    // TODO: The state should simply maintain & update an `ItemHeaders` object.
    // If transaction metrics were extracted, set the corresponding item header
    event_item.set_metrics_extracted(event_metrics_extracted.0);
    event_item.set_spans_extracted(spans_extracted.0);
    event_item.set_fully_normalized(event_fully_normalized.0);

    managed_envelope.envelope_mut().add_item(event_item);

    Ok(())
}

/// Checks if the Event includes unprintable fields.
pub fn has_unprintable_fields(event: &Annotated<Event>) -> bool {
    fn is_unprintable(value: &&str) -> bool {
        value.chars().any(|c| {
            c == '\u{fffd}' // unicode replacement character
                || (c.is_control() && !c.is_whitespace()) // non-whitespace control characters
        })
    }
    if let Some(event) = event.value() {
        let env = event.environment.as_str().filter(is_unprintable);
        let release = event.release.as_str().filter(is_unprintable);
        env.is_some() || release.is_some()
    } else {
        false
    }
}

/// Computes and emits metrics for monitoring user feedback (UserReportV2) ingest
pub fn emit_feedback_metrics(envelope: &Envelope) {
    let mut has_feedback = false;
    let mut num_attachments = 0;
    for item in envelope.items() {
        match item.ty() {
            ItemType::UserReportV2 => has_feedback = true,
            ItemType::Attachment => num_attachments += 1,
            _ => (),
        }
    }
    if has_feedback {
        metric!(counter(RelayCounters::FeedbackAttachments) += num_attachments);
    }
}

/// Checks for duplicate items in an envelope.
///
/// An item is considered duplicate if it was not removed by sanitation in `process_event` and
/// `extract_event`. This partially depends on the `processing_enabled` flag.
fn is_duplicate(item: &Item, processing_enabled: bool) -> bool {
    match item.ty() {
        // These should always be removed by `extract_event`:
        ItemType::Event => true,
        ItemType::Transaction => true,
        ItemType::Security => true,
        ItemType::FormData => true,
        ItemType::RawSecurity => true,
        ItemType::UserReportV2 => true,

        // These should be removed conditionally:
        ItemType::UnrealReport => processing_enabled,

        // These may be forwarded to upstream / store:
        ItemType::Attachment => false,
        ItemType::Nel => false,
        ItemType::UserReport => false,

        // Aggregate data is never considered as part of deduplication
        ItemType::Session => false,
        ItemType::Sessions => false,
        ItemType::Statsd => false,
        ItemType::MetricBuckets => false,
        ItemType::ClientReport => false,
        ItemType::Profile => false,
        ItemType::ReplayEvent => false,
        ItemType::ReplayRecording => false,
        ItemType::ReplayVideo => false,
        ItemType::CheckIn => false,
        ItemType::Log => false,
        ItemType::OtelLog => false,
        ItemType::Span => false,
        ItemType::OtelSpan => false,
        ItemType::OtelTracesData => false,
        ItemType::ProfileChunk => false,

        // Without knowing more, `Unknown` items are allowed to be repeated
        ItemType::Unknown(_) => false,
    }
}

fn event_from_json_payload(
    item: Item,
    event_type: Option<EventType>,
) -> Result<ExtractedEvent, ProcessingError> {
    let mut event = Annotated::<Event>::from_json_bytes(&item.payload())
        .map_err(ProcessingError::InvalidJson)?;

    if let Some(event_value) = event.value_mut() {
        if event_type.is_some() {
            event_value.ty.set_value(event_type);
        }
    }

    Ok((event, item.len()))
}

fn event_from_security_report(
    item: Item,
    meta: &RequestMeta,
) -> Result<ExtractedEvent, ProcessingError> {
    let len = item.len();
    let mut event = Event::default();

    let bytes = item.payload();
    let data = &bytes;
    let Some(report_type) =
        SecurityReportType::from_json(data).map_err(ProcessingError::InvalidJson)?
    else {
        return Err(ProcessingError::InvalidSecurityType(bytes));
    };

    let (apply_result, event_type) = match report_type {
        SecurityReportType::Csp => (Csp::apply_to_event(data, &mut event), EventType::Csp),
        SecurityReportType::ExpectCt => (
            ExpectCt::apply_to_event(data, &mut event),
            EventType::ExpectCt,
        ),
        SecurityReportType::ExpectStaple => (
            ExpectStaple::apply_to_event(data, &mut event),
            EventType::ExpectStaple,
        ),
        SecurityReportType::Hpkp => (Hpkp::apply_to_event(data, &mut event), EventType::Hpkp),
        SecurityReportType::Unsupported => return Err(ProcessingError::UnsupportedSecurityType),
    };

    if let Err(json_error) = apply_result {
        // logged in extract_event
        relay_log::configure_scope(|scope| {
            scope.set_extra("payload", String::from_utf8_lossy(data).into());
        });

        return Err(ProcessingError::InvalidSecurityReport(json_error));
    }

    if let Some(release) = item.get_header("sentry_release").and_then(Value::as_str) {
        event.release = Annotated::from(LenientString(release.to_owned()));
    }

    if let Some(env) = item
        .get_header("sentry_environment")
        .and_then(Value::as_str)
    {
        event.environment = Annotated::from(env.to_owned());
    }

    if let Some(origin) = meta.origin() {
        event
            .request
            .get_or_insert_with(Default::default)
            .headers
            .get_or_insert_with(Default::default)
            .insert("Origin".into(), Annotated::new(origin.to_string().into()));
    }

    // Explicitly set the event type. This is required so that a `Security` item can be created
    // instead of a regular `Event` item.
    event.ty = Annotated::new(event_type);

    Ok((Annotated::new(event), len))
}

fn extract_attached_event(
    config: &Config,
    item: Option<Item>,
) -> Result<Annotated<Event>, ProcessingError> {
    let item = match item {
        Some(item) if !item.is_empty() => item,
        _ => return Ok(Annotated::new(Event::default())),
    };

    // Protect against blowing up during deserialization. Attachments can have a significantly
    // larger size than regular events and may cause significant processing delays.
    if item.len() > config.max_event_size() {
        return Err(ProcessingError::PayloadTooLarge(
            item.attachment_type()
                .map(|t| t.into())
                .unwrap_or_else(|| item.ty().into()),
        ));
    }

    let payload = item.payload();
    let deserializer = &mut rmp_serde::Deserializer::from_read_ref(payload.as_ref());
    Annotated::deserialize_with_meta(deserializer).map_err(ProcessingError::InvalidMsgpack)
}

fn parse_msgpack_breadcrumbs(
    config: &Config,
    item: Option<Item>,
) -> Result<Array<Breadcrumb>, ProcessingError> {
    let mut breadcrumbs = Array::new();
    let item = match item {
        Some(item) if !item.is_empty() => item,
        _ => return Ok(breadcrumbs),
    };

    // Validate that we do not exceed the maximum breadcrumb payload length. Breadcrumbs are
    // truncated to a maximum of 100 in event normalization, but this is to protect us from
    // blowing up during deserialization. As approximation, we use the maximum event payload
    // size as bound, which is roughly in the right ballpark.
    if item.len() > config.max_event_size() {
        return Err(ProcessingError::PayloadTooLarge(
            item.attachment_type()
                .map(|t| t.into())
                .unwrap_or_else(|| item.ty().into()),
        ));
    }

    let payload = item.payload();
    let mut deserializer = rmp_serde::Deserializer::new(payload.as_ref());

    while !deserializer.get_ref().is_empty() {
        let breadcrumb = Annotated::deserialize_with_meta(&mut deserializer)?;
        breadcrumbs.push(breadcrumb);
    }

    Ok(breadcrumbs)
}

fn event_from_attachments(
    config: &Config,
    event_item: Option<Item>,
    breadcrumbs_item1: Option<Item>,
    breadcrumbs_item2: Option<Item>,
) -> Result<ExtractedEvent, ProcessingError> {
    let len = event_item.as_ref().map_or(0, |item| item.len())
        + breadcrumbs_item1.as_ref().map_or(0, |item| item.len())
        + breadcrumbs_item2.as_ref().map_or(0, |item| item.len());

    let mut event = extract_attached_event(config, event_item)?;
    let mut breadcrumbs1 = parse_msgpack_breadcrumbs(config, breadcrumbs_item1)?;
    let mut breadcrumbs2 = parse_msgpack_breadcrumbs(config, breadcrumbs_item2)?;

    let timestamp1 = breadcrumbs1
        .iter()
        .rev()
        .find_map(|breadcrumb| breadcrumb.value().and_then(|b| b.timestamp.value()));

    let timestamp2 = breadcrumbs2
        .iter()
        .rev()
        .find_map(|breadcrumb| breadcrumb.value().and_then(|b| b.timestamp.value()));

    // Sort breadcrumbs by date. We presume that last timestamp from each row gives the
    // relative sequence of the whole sequence, i.e., we don't need to splice the sequences
    // to get the breadrumbs sorted.
    if timestamp1 > timestamp2 {
        std::mem::swap(&mut breadcrumbs1, &mut breadcrumbs2);
    }

    // Limit the total length of the breadcrumbs. We presume that if we have both
    // breadcrumbs with items one contains the maximum number of breadcrumbs allowed.
    let max_length = std::cmp::max(breadcrumbs1.len(), breadcrumbs2.len());

    breadcrumbs1.extend(breadcrumbs2);

    if breadcrumbs1.len() > max_length {
        // Keep only the last max_length elements from the vectors
        breadcrumbs1.drain(0..(breadcrumbs1.len() - max_length));
    }

    if !breadcrumbs1.is_empty() {
        event.get_or_insert_with(Event::default).breadcrumbs = Annotated::new(Values {
            values: Annotated::new(breadcrumbs1),
            other: Object::default(),
        });
    }

    Ok((event, len))
}

fn merge_formdata(target: &mut SerdeValue, item: Item) {
    let payload = item.payload();
    let mut aggregator = ChunkedFormDataAggregator::new();

    for entry in FormDataIter::new(&payload) {
        if entry.key() == "sentry" || entry.key().starts_with("sentry___") {
            // Custom clients can submit longer payloads and should JSON encode event data into
            // the optional `sentry` field or a `sentry___<namespace>` field.
            match serde_json::from_str(entry.value()) {
                Ok(event) => utils::merge_values(target, event),
                Err(_) => relay_log::debug!("invalid json event payload in sentry form field"),
            }
        } else if let Some(index) = utils::get_sentry_chunk_index(entry.key(), "sentry__") {
            // Electron SDK splits up long payloads into chunks starting at sentry__1 with an
            // incrementing counter. Assemble these chunks here and then decode them below.
            aggregator.insert(index, entry.value());
        } else if let Some(keys) = utils::get_sentry_entry_indexes(entry.key()) {
            // Try to parse the nested form syntax `sentry[key][key]` This is required for the
            // Breakpad client library, which only supports string values of up to 64
            // characters.
            utils::update_nested_value(target, &keys, entry.value());
        } else {
            // Merge additional form fields from the request with `extra` data from the event
            // payload and set defaults for processing. This is sent by clients like Breakpad or
            // Crashpad.
            utils::update_nested_value(target, &["extra", entry.key()], entry.value());
        }
    }

    if !aggregator.is_empty() {
        match serde_json::from_str(&aggregator.join()) {
            Ok(event) => utils::merge_values(target, event),
            Err(_) => relay_log::debug!("invalid json event payload in sentry__* form fields"),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::collections::BTreeMap;

    use chrono::{DateTime, TimeZone, Utc};

    use super::*;

    fn create_breadcrumbs_item(breadcrumbs: &[(Option<DateTime<Utc>>, &str)]) -> Item {
        let mut data = Vec::new();

        for (date, message) in breadcrumbs {
            let mut breadcrumb = BTreeMap::new();
            breadcrumb.insert("message", (*message).to_owned());
            if let Some(date) = date {
                breadcrumb.insert("timestamp", date.to_rfc3339());
            }

            rmp_serde::encode::write(&mut data, &breadcrumb).expect("write msgpack");
        }

        let mut item = Item::new(ItemType::Attachment);
        item.set_payload(ContentType::MsgPack, data);
        item
    }

    fn breadcrumbs_from_event(event: &Annotated<Event>) -> &Vec<Annotated<Breadcrumb>> {
        event
            .value()
            .unwrap()
            .breadcrumbs
            .value()
            .unwrap()
            .values
            .value()
            .unwrap()
    }

    #[test]
    fn test_breadcrumbs_file1() {
        let item = create_breadcrumbs_item(&[(None, "item1")]);

        // NOTE: using (Some, None) here:
        let result = event_from_attachments(&Config::default(), None, Some(item), None);

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);

        assert_eq!(breadcrumbs.len(), 1);
        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        assert_eq!("item1", first_breadcrumb_message);
    }

    #[test]
    fn test_breadcrumbs_file2() {
        let item = create_breadcrumbs_item(&[(None, "item2")]);

        // NOTE: using (None, Some) here:
        let result = event_from_attachments(&Config::default(), None, None, Some(item));

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 1);

        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        assert_eq!("item2", first_breadcrumb_message);
    }

    #[test]
    fn test_breadcrumbs_truncation() {
        let item1 = create_breadcrumbs_item(&[(None, "crumb1")]);
        let item2 = create_breadcrumbs_item(&[(None, "crumb2"), (None, "crumb3")]);

        let result = event_from_attachments(&Config::default(), None, Some(item1), Some(item2));

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);
    }

    #[test]
    fn test_breadcrumbs_order_with_none() {
        let d1 = Utc.with_ymd_and_hms(2019, 10, 10, 12, 10, 10).unwrap();
        let d2 = Utc.with_ymd_and_hms(2019, 10, 11, 12, 10, 10).unwrap();

        let item1 = create_breadcrumbs_item(&[(None, "none"), (Some(d1), "d1")]);
        let item2 = create_breadcrumbs_item(&[(Some(d2), "d2")]);

        let result = event_from_attachments(&Config::default(), None, Some(item1), Some(item2));

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);

        assert_eq!(Some("d1"), breadcrumbs[0].value().unwrap().message.as_str());
        assert_eq!(Some("d2"), breadcrumbs[1].value().unwrap().message.as_str());
    }

    #[test]
    fn test_breadcrumbs_reversed_with_none() {
        let d1 = Utc.with_ymd_and_hms(2019, 10, 10, 12, 10, 10).unwrap();
        let d2 = Utc.with_ymd_and_hms(2019, 10, 11, 12, 10, 10).unwrap();

        let item1 = create_breadcrumbs_item(&[(Some(d2), "d2")]);
        let item2 = create_breadcrumbs_item(&[(None, "none"), (Some(d1), "d1")]);

        let result = event_from_attachments(&Config::default(), None, Some(item1), Some(item2));

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);

        assert_eq!(Some("d1"), breadcrumbs[0].value().unwrap().message.as_str());
        assert_eq!(Some("d2"), breadcrumbs[1].value().unwrap().message.as_str());
    }

    #[test]
    fn test_empty_breadcrumbs_item() {
        let item1 = create_breadcrumbs_item(&[]);
        let item2 = create_breadcrumbs_item(&[]);
        let item3 = create_breadcrumbs_item(&[]);

        let result =
            event_from_attachments(&Config::default(), Some(item1), Some(item2), Some(item3));

        // regression test to ensure we don't fail parsing an empty file
        result.expect("event_from_attachments");
    }

    #[test]
    #[cfg(feature = "processing")]
    fn test_unprintable_fields() {
        let event = Annotated::new(Event {
            environment: Annotated::new(String::from(
                "�9�~YY���)�����9�~YY���)�����9�~YY���)�����9�~YY���)�����",
            )),
            ..Default::default()
        });
        assert!(has_unprintable_fields(&event));

        let event = Annotated::new(Event {
            release: Annotated::new(
                String::from("���7��#1G����7��#1G����7��#1G����7��#1G����7��#")
                    .into(),
            ),
            ..Default::default()
        });
        assert!(has_unprintable_fields(&event));

        let event = Annotated::new(Event {
            environment: Annotated::new(String::from("production")),
            ..Default::default()
        });
        assert!(!has_unprintable_fields(&event));

        let event = Annotated::new(Event {
            release: Annotated::new(
                String::from("release with\t some\n normal\r\nwhitespace").into(),
            ),
            ..Default::default()
        });
        assert!(!has_unprintable_fields(&event));
    }
}
