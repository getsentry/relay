use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use failure::Fail;
use futures::prelude::*;
use parking_lot::RwLock;
use serde_json::Value as SerdeValue;

use relay_common::{clone, metric, LogError};
use relay_config::{Config, RelayMode};
use relay_general::pii::PiiProcessor;
use relay_general::processor::{process_value, ProcessingState};
use relay_general::protocol::{
    Breadcrumb, Csp, Event, EventId, ExpectCt, ExpectStaple, Hpkp, LenientString, Metrics,
    SecurityReportType, Values,
};
use relay_general::types::{Annotated, Array, Object, ProcessingAction, Value};
use relay_quotas::RateLimits;
use relay_redis::RedisPool;

use crate::actors::outcome::{DiscardReason, Outcome, OutcomeProducer, TrackOutcome};
use crate::actors::project::{
    EventAction, GetEventAction, GetProjectState, GetScoping, Project, ProjectState,
    UpdateRateLimits,
};
use crate::actors::project_cache::ProjectError;
use crate::actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequestError};
use crate::envelope::{self, AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::metrics::{RelayCounters, RelayHistograms, RelaySets, RelayTimers};
use crate::service::ServerError;
use crate::utils::{self, FormDataIter, FutureExt};

#[cfg(feature = "processing")]
use {
    crate::actors::store::{StoreEnvelope, StoreError, StoreForwarder},
    crate::service::ServerErrorKind,
    failure::ResultExt,
    relay_filter::FilterStatKey,
    relay_general::protocol::IpAddr,
    relay_general::store::{GeoIpLookup, StoreConfig, StoreProcessor},
    relay_quotas::{DataCategory, RateLimiter, RateLimitingError},
};

#[derive(Debug, Fail)]
pub enum QueueEnvelopeError {
    #[fail(display = "Too many events (max_concurrent_events reached)")]
    TooManyEvents,
}

#[derive(Debug, Fail)]
enum ProcessingError {
    #[fail(display = "invalid json in event")]
    InvalidJson(#[cause] serde_json::Error),

    #[fail(display = "invalid message pack event payload")]
    InvalidMsgpack(#[cause] rmp_serde::decode::Error),

    #[cfg(feature = "processing")]
    #[fail(display = "invalid unreal crash report")]
    InvalidUnrealReport(#[cause] symbolic::unreal::Unreal4Error),

    #[fail(display = "event payload too large")]
    PayloadTooLarge,

    #[cfg(feature = "processing")]
    #[fail(display = "invalid transaction event")]
    InvalidTransaction,

    #[fail(display = "event processor failed")]
    ProcessingFailed(#[cause] ProcessingAction),

    #[fail(display = "duplicate {} in event", _0)]
    DuplicateItem(ItemType),

    #[fail(display = "could not schedule project fetch")]
    ScheduleFailed(#[cause] MailboxError),

    #[fail(display = "failed to resolve project information")]
    ProjectFailed(#[cause] ProjectError),

    #[fail(display = "invalid security report type")]
    InvalidSecurityReportType,

    #[fail(display = "invalid security report")]
    InvalidSecurityReport(#[cause] serde_json::Error),

    #[fail(display = "event submission rejected with reason: {:?}", _0)]
    EventRejected(DiscardReason),

    #[cfg(feature = "processing")]
    #[fail(display = "event filtered with reason: {:?}", _0)]
    EventFiltered(FilterStatKey),

    #[fail(display = "could not serialize event payload")]
    SerializeFailed(#[cause] serde_json::Error),

    #[fail(display = "could not send event to upstream")]
    SendFailed(#[cause] UpstreamRequestError),

    #[cfg(feature = "processing")]
    #[fail(display = "could not store event")]
    StoreFailed(#[cause] StoreError),

    #[fail(display = "event rate limited")]
    RateLimited(RateLimits),

    #[cfg(feature = "processing")]
    #[fail(display = "failed to apply quotas")]
    QuotasFailed(#[cause] RateLimitingError),

    #[fail(display = "event exceeded its configured lifetime")]
    Timeout,
}

type ExtractedEvent = (Annotated<Event>, usize);

struct EventProcessor {
    config: Arc<Config>,
    #[cfg(feature = "processing")]
    rate_limiter: Option<RateLimiter>,
    #[cfg(feature = "processing")]
    geoip_lookup: Option<Arc<GeoIpLookup>>,
}

impl EventProcessor {
    #[cfg(feature = "processing")]
    pub fn new(
        config: Arc<Config>,
        rate_limiter: Option<RateLimiter>,
        geoip_lookup: Option<Arc<GeoIpLookup>>,
    ) -> Self {
        Self {
            config,
            rate_limiter,
            geoip_lookup,
        }
    }

    #[cfg(not(feature = "processing"))]
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }

    /// Writes a placeholder to indicate that this event has an associated minidump or an apple
    /// crash report.
    ///
    /// This will indicate to the ingestion pipeline that this event will need to be processed. The
    /// payload can be checked via `is_minidump_event`.
    #[cfg(feature = "processing")]
    fn write_native_placeholder(&self, event: &mut Annotated<Event>, is_minidump: bool) {
        use relay_general::protocol::{Exception, JsonLenientString, Level, Mechanism};

        let event = event.get_or_insert_with(Event::default);

        // Events must be native platform.
        let platform = event.platform.value_mut();
        *platform = Some("native".to_string());

        // Assume that this minidump is the result of a crash and assign the fatal
        // level. Note that the use of `setdefault` here doesn't generally allow the
        // user to override the minidump's level as processing will overwrite it
        // later.
        event.level.get_or_insert_with(|| Level::Fatal);

        // Create a placeholder exception. This signals normalization that this is an
        // error event and also serves as a placeholder if processing of the minidump
        // fails.
        let exceptions = event
            .exceptions
            .value_mut()
            .get_or_insert_with(Values::default)
            .values
            .value_mut()
            .get_or_insert_with(Vec::new);

        exceptions.clear(); // clear previous errors if any

        let (type_name, value, mechanism_type) = if is_minidump {
            ("Minidump", "Invalid Minidump", "minidump")
        } else {
            (
                "AppleCrashReport",
                "Invalid Apple Crash Report",
                "applecrashreport",
            )
        };

        exceptions.push(Annotated::new(Exception {
            ty: Annotated::new(type_name.to_string()),
            value: Annotated::new(JsonLenientString(value.to_string())),
            mechanism: Annotated::new(Mechanism {
                ty: Annotated::from(mechanism_type.to_string()),
                handled: Annotated::from(false),
                synthetic: Annotated::from(true),
                ..Mechanism::default()
            }),
            ..Exception::default()
        }));
    }

    fn event_from_json_payload(&self, item: Item) -> Result<ExtractedEvent, ProcessingError> {
        Annotated::from_json_bytes(&item.payload())
            .map(|event| (event, item.len()))
            .map_err(ProcessingError::InvalidJson)
    }

    fn event_from_security_report(&self, item: Item) -> Result<ExtractedEvent, ProcessingError> {
        let len = item.len();
        let mut event = Event::default();

        let data = &item.payload();
        let report_type = SecurityReportType::from_json(data)
            .map_err(ProcessingError::InvalidJson)?
            .ok_or(ProcessingError::InvalidSecurityReportType)?;

        match report_type {
            SecurityReportType::Csp => Csp::apply_to_event(data, &mut event),
            SecurityReportType::ExpectCt => ExpectCt::apply_to_event(data, &mut event),
            SecurityReportType::ExpectStaple => ExpectStaple::apply_to_event(data, &mut event),
            SecurityReportType::Hpkp => Hpkp::apply_to_event(data, &mut event),
        }
        .map_err(ProcessingError::InvalidSecurityReport)?;

        if let Some(release) = item.get_header("sentry_release").and_then(Value::as_str) {
            event.release = Annotated::from(LenientString(release.to_owned()));
        }

        if let Some(env) = item
            .get_header("sentry_environment")
            .and_then(Value::as_str)
        {
            event.environment = Annotated::from(env.to_owned());
        }

        Ok((Annotated::new(event), len))
    }

    fn merge_formdata(&self, target: &mut SerdeValue, item: Item) {
        let payload = item.payload();
        for entry in FormDataIter::new(&payload) {
            if entry.key() == "sentry" {
                // Custom clients can submit longer payloads and should JSON encode event data into
                // the optional `sentry` field.
                match serde_json::from_str(entry.value()) {
                    Ok(event) => utils::merge_values(target, event),
                    Err(_) => log::debug!("invalid json event payload in form data"),
                }
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
            return Err(ProcessingError::PayloadTooLarge);
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
            return Err(ProcessingError::PayloadTooLarge);
        }

        let payload = item.payload();
        let mut deserializer = rmp_serde::Deserializer::new(payload.as_ref());

        while !deserializer.get_ref().is_empty() {
            let breadcrumb = Annotated::deserialize_with_meta(&mut deserializer)
                .map_err(ProcessingError::InvalidMsgpack)?;
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

        let mut event = Self::extract_attached_event(config, event_item)?;
        let mut breadcrumbs1 = Self::parse_msgpack_breadcrumbs(config, breadcrumbs_item1)?;
        let mut breadcrumbs2 = Self::parse_msgpack_breadcrumbs(config, breadcrumbs_item2)?;

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

    /// Extracts the primary event payload from an envelope.
    ///
    /// The event is obtained from only one source in the following precedence:
    ///  1. An explicit event item. This is also the case for JSON uploads.
    ///  2. A security report item.
    ///  3. Attachments `__sentry-event` and `__sentry-breadcrumb1/2`.
    ///  4. A multipart form data body.
    ///  5. If none match, `Annotated::empty()`.
    ///
    /// The return value is a tuple of the extracted event and the original ingested size.
    fn extract_event(&self, envelope: &mut Envelope) -> Result<ExtractedEvent, ProcessingError> {
        // Remove all items first, and then process them. After this function returns, only
        // attachments can remain in the envelope. The event will be added again at the end of
        // `process_event`.
        let event_item = envelope.take_item_by(|item| item.ty() == ItemType::Event);
        let security_item = envelope.take_item_by(|item| item.ty() == ItemType::SecurityReport);
        let form_item = envelope.take_item_by(|item| item.ty() == ItemType::FormData);
        let attachment_item = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::EventPayload));
        let breadcrumbs_item1 = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::Breadcrumbs));
        let breadcrumbs_item2 = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::Breadcrumbs));

        if let Some(item) = event_item {
            log::trace!("processing json event");
            return Ok(metric!(timer(RelayTimers::EventProcessingDeserialize), {
                self.event_from_json_payload(item)?
            }));
        }

        if let Some(item) = security_item {
            log::trace!("processing security report");
            return Ok(self.event_from_security_report(item)?);
        }

        if attachment_item.is_some() || breadcrumbs_item1.is_some() || breadcrumbs_item2.is_some() {
            log::trace!("extracting attached event data");
            return Ok(Self::event_from_attachments(
                &self.config,
                attachment_item,
                breadcrumbs_item1,
                breadcrumbs_item2,
            )?);
        }

        if let Some(item) = form_item {
            log::trace!("extracting form data");
            let len = item.len();

            let mut value = SerdeValue::Object(Default::default());
            self.merge_formdata(&mut value, item);
            let event = Annotated::deserialize_with_meta(value).unwrap_or_default();

            return Ok((event, len));
        }

        log::trace!("no event in envelope");
        Ok((Annotated::empty(), 0))
    }

    #[cfg(feature = "processing")]
    fn enforce_quotas(
        &self,
        envelope: &Envelope,
        state: &ProjectState,
    ) -> Result<(), ProcessingError> {
        let rate_limiter = match self.rate_limiter.as_ref() {
            Some(rate_limiter) => rate_limiter,
            None => return Ok(()),
        };

        let public_key = envelope.meta().public_key();
        let quotas = if !state.config.quotas.is_empty() {
            state.config.quotas.as_slice()
        } else if let Some(ref key_config) = state.get_public_key_config(public_key) {
            key_config.legacy_quotas.as_slice()
        } else {
            &[]
        };

        if quotas.is_empty() {
            return Ok(());
        }

        // Fetch scoping again from the project state. This is a rather cheap operation at this
        // point and it is easier than passing scoping through all layers of `process_envelope`.
        let scoping = state.get_scoping(envelope.meta());

        let rate_limits = metric!(timer(RelayTimers::EventProcessingRateLimiting), {
            rate_limiter
                .is_rate_limited(quotas, scoping.item(DataCategory::Error))
                .map_err(ProcessingError::QuotasFailed)?
        });

        if rate_limits.is_limited() {
            return Err(ProcessingError::RateLimited(rate_limits));
        }

        Ok(())
    }

    #[cfg(feature = "processing")]
    fn store_process_event(
        &self,
        event: &mut Annotated<Event>,
        envelope: &Envelope,
        project_state: &ProjectState,
    ) -> Result<(), ProcessingError> {
        let geoip_lookup = self.geoip_lookup.as_deref();
        let key_id = project_state
            .get_public_key_config(&envelope.meta().public_key())
            .and_then(|k| Some(k.numeric_id?.to_string()));

        if key_id.is_none() {
            log::error!("can't find key in project config, but we verified auth before already");
        }

        let store_config = StoreConfig {
            project_id: Some(envelope.meta().project_id().value()),
            client_ip: envelope.meta().client_addr().map(IpAddr::from),
            client: envelope.meta().client().map(str::to_owned),
            key_id,
            protocol_version: Some(envelope.meta().version().to_string()),
            grouping_config: project_state.config.grouping_config.clone(),
            user_agent: envelope.meta().user_agent().map(str::to_owned),
            max_secs_in_future: Some(self.config.max_secs_in_future()),
            max_secs_in_past: Some(self.config.max_secs_in_past()),
            enable_trimming: Some(true),
            is_renormalize: Some(false),
            remove_other: Some(true),
            normalize_user_agent: Some(true),
            sent_at: envelope.sent_at(),
        };

        let mut store_processor = StoreProcessor::new(store_config, geoip_lookup);
        metric!(timer(RelayTimers::EventProcessingProcess), {
            process_value(event, &mut store_processor, ProcessingState::root())
                .map_err(|_| ProcessingError::InvalidTransaction)?;
        });

        // Event filters assume a normalized event. Unfortunately, this requires us to run
        // expensive normalization first.
        if let Some(event) = event.value_mut() {
            let client_ip = envelope.meta().client_addr();
            let filter_settings = &project_state.config.filter_settings;
            let filter_result = metric!(timer(RelayTimers::EventProcessingFiltering), {
                relay_filter::should_filter(event, client_ip, filter_settings)
            });

            if let Err(reason) = filter_result {
                // If the event should be filtered, no more processing is needed
                return Err(ProcessingError::EventFiltered(reason));
            }
        }

        // Run rate limiting after normalizing the event and running all filters. If the event is
        // dropped or filtered for a different reason before that, it should not count against
        // quotas. Also, this allows to reduce the number of requests to the rate limiter (currently
        // implemented in Redis).
        self.enforce_quotas(envelope, project_state)?;

        Ok(())
    }

    /// Checks for duplicate items in an envelope.
    ///
    /// An item is considered duplicate if it was not removed by sanitation in `process_event` and
    /// `extract_event`. This partially depends on the `processing_enabled` flag.
    fn is_duplicate(&self, item: &Item) -> bool {
        match item.ty() {
            // These should always be removed by `extract_event`:
            ItemType::Event => true,
            ItemType::FormData => true,
            ItemType::SecurityReport => true,

            // These should be removed conditionally:
            ItemType::UnrealReport => self.config.processing_enabled(),

            // These may be forwarded to upstream / store:
            ItemType::Attachment => false,
            ItemType::UserReport => false,

            // session data is never considered as part of deduplication
            ItemType::Session => false,
        }
    }

    fn process(
        &self,
        message: ProcessEnvelope,
    ) -> Result<ProcessEnvelopeResponse, ProcessingError> {
        let mut envelope = message.envelope;

        macro_rules! if_processing {
            ($($tt:tt)*) => {
                #[cfg(feature = "processing")] {
                    if self.config.processing_enabled() {
                        $($tt)*
                    }
                }
            };
        }

        // Set the event retention. Effectively, this value will only be available in processing
        // mode when the full project config is queried from the upstream.
        if let Some(retention) = message.project_state.config.event_retention {
            envelope.set_retention(retention);
        }

        // Unreal endpoint puts the whole request into an item. This is done to make the endpoint
        // fast. For envelopes containing an Unreal request, we will look into the unreal item and
        // expand it so it can be consumed like any other event (e.g. `__sentry-event`). External
        // Relays should leave this as-is.
        if_processing! {
            if let Some(item) = envelope.take_item_by(|item| item.ty() == ItemType::UnrealReport) {
                utils::expand_unreal_envelope(item, &mut envelope)
                    .map_err(ProcessingError::InvalidUnrealReport)?;
            }
        }

        // Carry metrics on event sizes through the entire normalization process. Without
        // processing, this value is unused and will be optimized away. Note how we need to extract
        // sizes at different stages of processing and apply them after `store_process_event`.
        let mut _metrics = Metrics::default();

        // Extract the event from the envelope. This removes all items from the envelope that should
        // not be forwarded, including the event item itself.
        let (mut event, event_len) = self.extract_event(&mut envelope)?;
        _metrics.bytes_ingested_event = Annotated::new(event_len as u64);

        // `extract_event` must remove all unique items from the envelope. Once the envelope is
        // processed, an `Event` item will be added to the envelope again. All additional items will
        // count as duplicates.
        if let Some(duplicate) = envelope.get_item_by(|item| self.is_duplicate(item)) {
            return Err(ProcessingError::DuplicateItem(duplicate.ty()));
        }

        if_processing! {
            // This envelope may contain UE4 crash report information, which needs to be patched on
            // the event returned from `extract_event`.
            utils::process_unreal_envelope(&mut event, &mut envelope)
                .map_err(ProcessingError::InvalidUnrealReport)?;

            // If special attachments are present in the envelope, add placeholder payloads to the
            // event. This indicates to the pipeline that the event needs special processing.
            let minidump_attachment = envelope
                .get_item_by(|item| item.attachment_type() == Some(AttachmentType::Minidump));
            let apple_crash_report_attachment = envelope
                .get_item_by(|item| item.attachment_type() == Some(AttachmentType::AppleCrashReport));

            if let Some(item) = minidump_attachment {
                _metrics.bytes_ingested_event_minidump = Annotated::new(item.len() as u64);
                self.write_native_placeholder(&mut event, true);
            } else if let Some(item) =  apple_crash_report_attachment {
                _metrics.bytes_ingested_event_applecrashreport = Annotated::new(item.len() as u64);
                self.write_native_placeholder(&mut event, false);
            }

            let attachment_size = envelope.items()
                .filter(|item| item.attachment_type() == Some(AttachmentType::Attachment))
                .map(|item| item.len())
                .sum::<usize>();

            if attachment_size > 0 {
                _metrics.bytes_ingested_event_attachment = Annotated::new(attachment_size as u64);
            }
        }

        if let Some(event) = event.value_mut() {
            // Event id is set statically in the ingest path.
            let event_id = envelope.event_id().unwrap_or_default();
            debug_assert!(!event_id.is_nil());

            // Ensure that the event id in the payload is consistent with the envelope. If an event
            // id was ingested, this will already be the case. Otherwise, this will insert a new
            // event id. To be defensive, we always overwrite to ensure consistency.
            event.id = Annotated::new(event_id);
        } else {
            // If we have an envelope without event at this point, we are done with processing. This
            // envelope only contains attachments or user reports. We should not run filters or
            // apply rate limits.
            log::trace!("no event for envelope, skipping processing");
            return Ok(ProcessEnvelopeResponse { envelope });
        }

        if_processing! {
            self.store_process_event(&mut event, &envelope, &message.project_state)?;

            // Write metrics into the fully processed event. This ensures that whatever happens
            // during processing is overwritten at last.
            if let Some(event) = event.value_mut() {
                event._metrics = Annotated::new(_metrics);
            }
        }

        // Run PII stripping last since normalization can add PII (e.g. IP addresses).
        metric!(timer(RelayTimers::EventProcessingPii), {
            if let Some(ref config) = message.project_state.config.pii_config {
                let compiled = config.compiled();
                let mut processor = PiiProcessor::new(&compiled);
                process_value(&mut event, &mut processor, ProcessingState::root())
                    .map_err(ProcessingError::ProcessingFailed)?;
            }

            let config = message
                .project_state
                .config
                .datascrubbing_settings
                .pii_config();

            if let Some(ref config) = *config {
                let compiled = config.compiled();

                let mut processor = PiiProcessor::new(&compiled);
                process_value(&mut event, &mut processor, ProcessingState::root())
                    .map_err(ProcessingError::ProcessingFailed)?;
            }
        });

        // We're done now. Serialize the event back into JSON and put it in an envelope so that it
        // can be sent to the upstream or processing queue.
        let data = metric!(timer(RelayTimers::EventProcessingSerialization), {
            event.to_json().map_err(ProcessingError::SerializeFailed)?
        });

        // Add the normalized event back to the envelope. All the other items are attachments.
        let mut event_item = Item::new(ItemType::Event);
        event_item.set_payload(ContentType::Json, data);
        if let Some(ty) = event.value().and_then(|e| e.ty.value()) {
            event_item.set_event_type(*ty);
        }
        envelope.add_item(event_item);

        Ok(ProcessEnvelopeResponse { envelope })
    }
}

impl Actor for EventProcessor {
    type Context = SyncContext<Self>;
}

struct ProcessEnvelope {
    pub envelope: Envelope,
    pub project_state: Arc<ProjectState>,
    pub start_time: Instant,
}

#[cfg_attr(not(feature = "processing"), allow(dead_code))]
struct ProcessEnvelopeResponse {
    envelope: Envelope,
}

impl Message for ProcessEnvelope {
    type Result = Result<ProcessEnvelopeResponse, ProcessingError>;
}

impl Handler<ProcessEnvelope> for EventProcessor {
    type Result = Result<ProcessEnvelopeResponse, ProcessingError>;

    fn handle(&mut self, message: ProcessEnvelope, _context: &mut Self::Context) -> Self::Result {
        metric!(timer(RelayTimers::EventWaitTime) = message.start_time.elapsed());
        metric!(timer(RelayTimers::EventProcessingTime), {
            self.process(message)
        })
    }
}

pub type CapturedEvent = Result<Envelope, String>;

pub struct EventManager {
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    processor: Addr<EventProcessor>,
    current_active_events: u32,
    outcome_producer: Addr<OutcomeProducer>,
    captured_events: Arc<RwLock<BTreeMap<EventId, CapturedEvent>>>,

    #[cfg(feature = "processing")]
    store_forwarder: Option<Addr<StoreForwarder>>,
}

impl EventManager {
    pub fn create(
        config: Arc<Config>,
        upstream: Addr<UpstreamRelay>,
        outcome_producer: Addr<OutcomeProducer>,
        redis_pool: Option<RedisPool>,
    ) -> Result<Self, ServerError> {
        let thread_count = config.cpu_concurrency();
        log::info!("starting {} event processing workers", thread_count);

        #[cfg(not(feature = "processing"))]
        let _ = redis_pool;

        #[cfg(feature = "processing")]
        let processor = {
            let geoip_lookup = match config.geoip_path() {
                Some(p) => Some(Arc::new(
                    GeoIpLookup::open(p).context(ServerErrorKind::GeoIpError)?,
                )),
                None => None,
            };

            let rate_limiter =
                redis_pool.map(|pool| RateLimiter::new(pool).max_limit(config.max_rate_limit()));

            SyncArbiter::start(
                thread_count,
                clone!(config, || EventProcessor::new(
                    config.clone(),
                    rate_limiter.clone(),
                    geoip_lookup.clone(),
                )),
            )
        };

        #[cfg(not(feature = "processing"))]
        let processor = SyncArbiter::start(
            thread_count,
            clone!(config, || EventProcessor::new(config.clone())),
        );

        #[cfg(feature = "processing")]
        let store_forwarder = if config.processing_enabled() {
            let actor = StoreForwarder::create(config.clone())?;
            Some(Arbiter::start(move |_| actor))
        } else {
            None
        };

        Ok(EventManager {
            config,
            upstream,
            processor,
            current_active_events: 0,
            captured_events: Arc::default(),

            #[cfg(feature = "processing")]
            store_forwarder,

            outcome_producer,
        })
    }
}

impl Actor for EventManager {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        // Set the mailbox size to the size of the event buffer. This is a rough estimate but
        // should ensure that we're not dropping events unintentionally after we've accepted them.
        let mailbox_size = self.config.event_buffer_size() as usize;
        context.set_mailbox_capacity(mailbox_size);
        log::info!("event manager started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        log::info!("event manager stopped");
    }
}

pub struct QueueEnvelope {
    pub envelope: Envelope,
    pub project: Addr<Project>,
    pub start_time: Instant,
}

impl Message for QueueEnvelope {
    type Result = Result<Option<EventId>, QueueEnvelopeError>;
}

impl Handler<QueueEnvelope> for EventManager {
    type Result = Result<Option<EventId>, QueueEnvelopeError>;

    fn handle(&mut self, message: QueueEnvelope, context: &mut Self::Context) -> Self::Result {
        metric!(histogram(RelayHistograms::EventQueueSize) = u64::from(self.current_active_events));

        metric!(
            histogram(RelayHistograms::EventQueueSizePct) = {
                let queue_size_pct = self.current_active_events as f32 * 100.0
                    / self.config.event_buffer_size() as f32;
                queue_size_pct.floor() as u64
            }
        );

        if self.config.event_buffer_size() <= self.current_active_events {
            return Err(QueueEnvelopeError::TooManyEvents);
        }

        self.current_active_events += 1;

        let event_id = message.envelope.event_id();

        // Actual event handling is performed asynchronously in a separate future. The lifetime of
        // that future will be tied to the EventManager's context. This allows to keep the Project
        // actor alive even if it is cleaned up in the ProjectManager.
        context.notify(HandleEnvelope {
            envelope: message.envelope,
            project: message.project,
            start_time: message.start_time,
        });

        log::trace!("queued event");
        Ok(event_id)
    }
}

struct HandleEnvelope {
    pub envelope: Envelope,
    pub project: Addr<Project>,
    pub start_time: Instant,
}

impl Message for HandleEnvelope {
    type Result = Result<(), ()>;
}

impl Handler<HandleEnvelope> for EventManager {
    type Result = ResponseActFuture<Self, (), ()>;

    fn handle(&mut self, message: HandleEnvelope, _context: &mut Self::Context) -> Self::Result {
        // We measure three timers while handling events, once they have been initially accepted:
        //
        // 1. `event.wait_time`: The time we take to get all dependencies for events before
        //    they actually start processing. This includes scheduling overheads, project config
        //    fetching, batched requests and congestions in the sync processor arbiter. This does
        //    not include delays in the incoming request (body upload) and skips all events that are
        //    fast-rejected.
        //
        // 2. `event.processing_time`: The time the sync processor takes to parse the event payload,
        //    apply normalizations, strip PII and finally re-serialize it into a byte stream. This
        //    is recorded directly in the EventProcessor.
        //
        // 3. `event.total_time`: The full time an event takes from being initially accepted up to
        //    being sent to the upstream (including delays in the upstream). This can be regarded
        //    the total time an event spent in this relay, corrected by incoming network delays.

        let upstream = self.upstream.clone();
        let processor = self.processor.clone();
        let outcome_producer = self.outcome_producer.clone();
        let captured_events = self.captured_events.clone();
        let capture = self.config.relay_mode() == RelayMode::Capture;

        #[cfg(feature = "processing")]
        let store_forwarder = self.store_forwarder.clone();

        let HandleEnvelope {
            envelope,
            project,
            start_time,
        } = message;

        let event_id = envelope.event_id();
        let project_id = envelope.meta().project_id();
        let remote_addr = envelope.meta().client_addr();
        let shared_meta = Arc::new(envelope.meta().clone());

        // Compute whether this envelope contains an event. This is used in error handling to
        // appropriately emit an outecome. Envelopes not containing events (such as standalone
        // attachment uploads or user reports) should never create outcomes.
        let is_event = envelope.items().any(Item::creates_event);

        let scoping = Rc::new(RefCell::new(envelope.meta().get_partial_scoping()));

        metric!(set(RelaySets::UniqueProjects) = project_id.value() as i64);

        let future = project
            .send(GetScoping::fetched(shared_meta.clone()))
            .map_err(ProcessingError::ScheduleFailed)
            .and_then(|scoping_result| scoping_result.map_err(ProcessingError::ProjectFailed))
            .and_then(clone!(project, scoping, |new_scoping| {
                scoping.replace(new_scoping);
                project
                    .send(GetEventAction::new(shared_meta))
                    .map_err(ProcessingError::ScheduleFailed)
            }))
            .and_then(|action| match action {
                EventAction::Accept => Ok(()),
                EventAction::RateLimit(limits) => Err(ProcessingError::RateLimited(limits)),
                EventAction::Discard(reason) => Err(ProcessingError::EventRejected(reason)),
            })
            .and_then(clone!(project, |_| {
                project
                    .send(GetProjectState)
                    .map_err(ProcessingError::ScheduleFailed)
                    .and_then(|result| result.map_err(ProcessingError::ProjectFailed))
            }))
            .and_then(move |project_state| {
                processor
                    .send(ProcessEnvelope {
                        envelope,
                        project_state,
                        start_time,
                    })
                    .map_err(ProcessingError::ScheduleFailed)
                    .flatten()
            })
            .and_then(clone!(captured_events, scoping, |processed| {
                let envelope = processed.envelope;

                #[cfg(feature = "processing")]
                {
                    if let Some(store_forwarder) = store_forwarder {
                        log::trace!("sending envelope to kafka");
                        let future = store_forwarder
                            .send(StoreEnvelope {
                                envelope,
                                start_time,
                                scoping: scoping.borrow().clone(),
                            })
                            .map_err(ProcessingError::ScheduleFailed)
                            .and_then(move |result| result.map_err(ProcessingError::StoreFailed));

                        return Box::new(future) as ResponseFuture<_, _>;
                    }
                }

                // if we are in capture mode, we stash away the event instead of
                // forwarding it.
                if capture {
                    // XXX: this is wrong because captured_events does not take envelopes without
                    // event_id into account.
                    if let Some(event_id) = event_id {
                        log::debug!("capturing envelope");
                        captured_events
                            .write()
                            .insert(event_id, CapturedEvent::Ok(envelope));
                    } else {
                        log::debug!("dropping non event envelope");
                    }
                    return Box::new(Ok(()).into_future()) as ResponseFuture<_, _>;
                }

                log::trace!("sending event to sentry endpoint");
                let request = SendRequest::post(format!("/api/{}/store/", project_id)).build(
                    move |builder| {
                        let meta = envelope.meta();

                        if let Some(origin) = meta.origin() {
                            builder.header("Origin", origin.to_string());
                        }

                        if let Some(user_agent) = meta.user_agent() {
                            builder.header("User-Agent", user_agent);
                        }

                        builder
                            .header("X-Sentry-Auth", meta.auth_header())
                            .header("X-Forwarded-For", meta.forwarded_for())
                            .header("Content-Type", envelope::CONTENT_TYPE)
                            .body(envelope.to_vec().map_err(failure::Error::from)?)
                    },
                );

                let future = upstream
                    .send(request)
                    .map_err(ProcessingError::ScheduleFailed)
                    .and_then(move |result| {
                        result.map_err(move |error| match error {
                            UpstreamRequestError::RateLimited(upstream_limits) => {
                                let limits = upstream_limits.scope(&scoping.borrow());
                                ProcessingError::RateLimited(limits)
                            }
                            other => ProcessingError::SendFailed(other),
                        })
                    });

                Box::new(future) as ResponseFuture<_, _>
            }))
            .into_actor(self)
            .timeout(self.config.event_buffer_expiry(), ProcessingError::Timeout)
            .map(|_, _, _| metric!(counter(RelayCounters::EventAccepted) += 1))
            .map_err(clone!(project, captured_events, |error, _, _| {
                // Rate limits need special handling: Cache them on the project to avoid
                // expensive processing while the limit is active.
                if let ProcessingError::RateLimited(ref rate_limits) = error {
                    project.do_send(UpdateRateLimits(rate_limits.clone()));
                }

                // if we are in capture mode, we stash away the event instead of
                // forwarding it.
                if capture {
                    // XXX: does not work with envelopes without event_id
                    if let Some(event_id) = event_id {
                        log::debug!("capturing failed event {}", event_id);
                        let msg = LogError(&error).to_string();
                        captured_events
                            .write()
                            .insert(event_id, CapturedEvent::Err(msg));
                    } else {
                        log::debug!("dropping failed envelope without event");
                    }
                }

                // Do not track outcomes or capture events for non-event envelopes (such as
                // individual attachments)
                if !is_event {
                    return;
                }

                metric!(counter(RelayCounters::EventRejected) += 1);
                let outcome_params = match error {
                    // General outcomes for invalid events
                    ProcessingError::PayloadTooLarge => {
                        Some(Outcome::Invalid(DiscardReason::TooLarge))
                    }
                    ProcessingError::InvalidJson(_) => {
                        Some(Outcome::Invalid(DiscardReason::InvalidJson))
                    }
                    ProcessingError::InvalidMsgpack(_) => {
                        Some(Outcome::Invalid(DiscardReason::InvalidMsgpack))
                    }
                    ProcessingError::EventRejected(outcome_reason) => {
                        Some(Outcome::Invalid(outcome_reason))
                    }
                    ProcessingError::InvalidSecurityReportType => {
                        Some(Outcome::Invalid(DiscardReason::SecurityReportType))
                    }
                    ProcessingError::InvalidSecurityReport(_) => {
                        Some(Outcome::Invalid(DiscardReason::SecurityReport))
                    }
                    ProcessingError::DuplicateItem(_) => {
                        Some(Outcome::Invalid(DiscardReason::DuplicateItem))
                    }

                    // Processing-only outcomes (Sentry-internal Relays)
                    #[cfg(feature = "processing")]
                    ProcessingError::InvalidUnrealReport(_) => {
                        Some(Outcome::Invalid(DiscardReason::ProcessUnreal))
                    }
                    #[cfg(feature = "processing")]
                    ProcessingError::InvalidTransaction => {
                        Some(Outcome::Invalid(DiscardReason::InvalidTransaction))
                    }
                    #[cfg(feature = "processing")]
                    ProcessingError::EventFiltered(ref filter_stat_key) => {
                        Some(Outcome::Filtered(*filter_stat_key))
                    }
                    // Processing-only but not feature flagged
                    ProcessingError::RateLimited(ref rate_limits) => rate_limits
                        .longest()
                        .map(|r| Outcome::RateLimited(r.reason_code.clone())),

                    // Internal errors
                    ProcessingError::SerializeFailed(_)
                    | ProcessingError::ScheduleFailed(_)
                    | ProcessingError::ProjectFailed(_)
                    | ProcessingError::Timeout
                    | ProcessingError::ProcessingFailed(_) => {
                        Some(Outcome::Invalid(DiscardReason::Internal))
                    }
                    #[cfg(feature = "processing")]
                    ProcessingError::StoreFailed(_) | ProcessingError::QuotasFailed(_) => {
                        Some(Outcome::Invalid(DiscardReason::Internal))
                    }

                    // If we send to an upstream, we don't emit outcomes.
                    ProcessingError::SendFailed(_) => None,
                };

                if let Some(Outcome::Invalid(DiscardReason::Internal)) = outcome_params {
                    // Errors are only logged for what we consider an internal discard reason. These
                    // indicate errors in the infrastructure or implementation bugs. In other cases,
                    // we "expect" errors and log them as info level.
                    log::error!("error processing event: {}", LogError(&error));
                } else {
                    log::debug!("dropped event: {}", LogError(&error));
                }

                if let Some(outcome) = outcome_params {
                    outcome_producer.do_send(TrackOutcome {
                        timestamp: Instant::now(),
                        scoping: scoping.borrow().clone(),
                        outcome,
                        event_id,
                        remote_addr,
                    })
                }
            }))
            .then(move |x, slf, _| {
                metric!(timer(RelayTimers::EventTotalTime) = start_time.elapsed());
                slf.current_active_events -= 1;
                fut::result(x)
            })
            .drop_guard("process_event");

        Box::new(future)
    }
}

pub struct GetCapturedEvent {
    pub event_id: EventId,
}

impl Message for GetCapturedEvent {
    type Result = Option<CapturedEvent>;
}

impl Handler<GetCapturedEvent> for EventManager {
    type Result = Option<CapturedEvent>;

    fn handle(&mut self, message: GetCapturedEvent, _context: &mut Self::Context) -> Self::Result {
        self.captured_events.read().get(&message.event_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{DateTime, TimeZone, Utc};

    fn create_breadcrumbs_item(breadcrumbs: &[(Option<DateTime<Utc>>, &str)]) -> Item {
        let mut data = Vec::new();

        for (date, message) in breadcrumbs {
            let mut breadcrumb = BTreeMap::new();
            breadcrumb.insert("message", (*message).to_string());
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
        let result =
            EventProcessor::event_from_attachments(&Config::default(), None, Some(item), None);

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
        let result =
            EventProcessor::event_from_attachments(&Config::default(), None, None, Some(item));

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

        let result = EventProcessor::event_from_attachments(
            &Config::default(),
            None,
            Some(item1),
            Some(item2),
        );

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);
    }

    #[test]
    fn test_breadcrumbs_order_with_none() {
        let d1 = Utc.ymd(2019, 10, 10).and_hms(12, 10, 10);
        let d2 = Utc.ymd(2019, 10, 11).and_hms(12, 10, 10);

        let item1 = create_breadcrumbs_item(&[(None, "none"), (Some(d1), "d1")]);
        let item2 = create_breadcrumbs_item(&[(Some(d2), "d2")]);

        let result = EventProcessor::event_from_attachments(
            &Config::default(),
            None,
            Some(item1),
            Some(item2),
        );

        let event = result.unwrap().0;
        let breadcrumbs = breadcrumbs_from_event(&event);
        assert_eq!(breadcrumbs.len(), 2);

        assert_eq!(Some("d1"), breadcrumbs[0].value().unwrap().message.as_str());
        assert_eq!(Some("d2"), breadcrumbs[1].value().unwrap().message.as_str());
    }

    #[test]
    fn test_breadcrumbs_reversed_with_none() {
        let d1 = Utc.ymd(2019, 10, 10).and_hms(12, 10, 10);
        let d2 = Utc.ymd(2019, 10, 11).and_hms(12, 10, 10);

        let item1 = create_breadcrumbs_item(&[(Some(d2), "d2")]);
        let item2 = create_breadcrumbs_item(&[(None, "none"), (Some(d1), "d1")]);

        let result = EventProcessor::event_from_attachments(
            &Config::default(),
            None,
            Some(item1),
            Some(item2),
        );

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

        let result = EventProcessor::event_from_attachments(
            &Config::default(),
            Some(item1),
            Some(item2),
            Some(item3),
        );

        // regression test to ensure we don't fail parsing an empty file
        result.expect("event_from_attachments");
    }
}
