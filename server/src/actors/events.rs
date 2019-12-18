use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::fut::result;
use actix::prelude::*;
use failure::Fail;
use futures::prelude::*;
use parking_lot::{Mutex, RwLock};
use serde_json::Value as SerdeValue;

use semaphore_common::{clone, metric, Config, LogError, RelayMode};
use semaphore_general::pii::PiiProcessor;
use semaphore_general::processor::{process_value, ProcessingState};
use semaphore_general::protocol::{
    Breadcrumb, Csp, Event, EventId, Exception, ExpectCt, ExpectStaple, Hpkp, JsonLenientString,
    LenientString, Level, Mechanism, SecurityReportType, Values,
};
use semaphore_general::types::{Annotated, Array, Object, ProcessingAction, Value};

use crate::actors::outcome::{DiscardReason, Outcome, OutcomeProducer, TrackOutcome};
use crate::actors::project::{
    EventAction, GetEventAction, GetProjectState, Project, ProjectError, ProjectState, RateLimit,
    RateLimitScope, RetryAfter,
};
use crate::actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequestError};
use crate::envelope::{self, AttachmentType, ContentType, Envelope, Item, ItemType};
use crate::service::ServerError;
use crate::utils::{self, FormDataIter, FutureExt};

#[cfg(feature = "processing")]
use {
    crate::actors::store::{StoreError, StoreEvent, StoreForwarder},
    crate::quotas::{QuotasError, RateLimiter},
    crate::service::ServerErrorKind,
    failure::ResultExt,
    semaphore_general::filter::{should_filter, FilterStatKey},
    semaphore_general::protocol::IpAddr,
    semaphore_general::store::{GeoIpLookup, StoreConfig, StoreProcessor},
};

#[derive(Debug, Fail)]
pub enum QueueEventError {
    #[fail(display = "Too many events (max_concurrent_events reached)")]
    TooManyEvents,
}

#[derive(Debug, Fail)]
enum ProcessingError {
    #[fail(display = "invalid json in event")]
    InvalidJson(#[cause] serde_json::Error),

    #[fail(display = "invalid message pack event payload")]
    InvalidMsgpack(#[cause] rmp_serde::decode::Error),

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

    #[fail(display = "failed to determine event action")]
    NoAction(#[cause] ProjectError),

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

    #[fail(display = "sending failed due to rate limit: {:?}", _0)]
    RateLimited(RateLimit),

    #[cfg(feature = "processing")]
    #[fail(display = "failed to apply quotas")]
    QuotasFailed(#[cause] QuotasError),

    #[fail(display = "event exceeded its configured lifetime")]
    Timeout,
}

struct EventProcessor {
    config: Arc<Config>,
    #[cfg(feature = "processing")]
    rate_limiter: RateLimiter,
    #[cfg(feature = "processing")]
    geoip_lookup: Option<Arc<GeoIpLookup>>,
}

impl EventProcessor {
    #[cfg(feature = "processing")]
    pub fn new(
        config: Arc<Config>,
        rate_limiter: RateLimiter,
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
    fn write_native_placeholder(&self, event: &mut Annotated<Event>, is_minidump: bool) {
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

    fn event_from_json_payload(&self, item: Item) -> Result<Annotated<Event>, ProcessingError> {
        Annotated::from_json_bytes(&item.payload()).map_err(ProcessingError::InvalidJson)
    }

    fn event_from_security_report(&self, item: Item) -> Result<Annotated<Event>, ProcessingError> {
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

        Ok(Annotated::new(event))
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
        item: Item,
    ) -> Result<Annotated<Event>, ProcessingError> {
        // Protect against blowing up during deserialization. Attachments can have a significantly
        // larger size than regular events and may cause significant processing delays.
        if item.len() > config.max_event_payload_size() {
            return Err(ProcessingError::PayloadTooLarge);
        }

        let payload = item.payload();
        let deserializer = &mut rmp_serde::Deserializer::from_slice(&payload);
        Annotated::deserialize_with_meta(deserializer).map_err(ProcessingError::InvalidMsgpack)
    }

    fn parse_msgpack_breadcrumbs(
        config: &Config,
        item: Item,
    ) -> Result<Option<Array<Breadcrumb>>, ProcessingError> {
        // Validate that we do not exceed the maximum breadcrumb payload length. Breadcrumbs are
        // truncated to a maximum of 100 in event normalization, but this is to protect us from
        // blowing up during deserialization. As approximation, we use the maximum event payload
        // size as bound, which is roughly in the right ballpark.
        if item.len() > config.max_event_payload_size() {
            return Err(ProcessingError::PayloadTooLarge);
        }

        let payload = item.payload();
        let deserializer = &mut rmp_serde::Deserializer::from_slice(&payload);

        match Annotated::deserialize_with_meta(deserializer) {
            Ok(annotated) => Ok(annotated.into_value()),
            Err(error) => Err(ProcessingError::InvalidMsgpack(error)),
        }
    }

    fn event_from_attachments(
        config: &Config,
        event_item: Option<Item>,
        breadcrumbs_item1: Option<Item>,
        breadcrumbs_item2: Option<Item>,
    ) -> Result<Annotated<Event>, ProcessingError> {
        let mut event = match event_item {
            Some(item) => Self::extract_attached_event(config, item)?,
            None => Annotated::new(Event::default()),
        };

        let mut breadcrumbs1 = match breadcrumbs_item1 {
            Some(item) => Self::parse_msgpack_breadcrumbs(config, item)?.unwrap_or_default(),
            None => Array::new(),
        };

        let mut breadcrumbs2 = match breadcrumbs_item2 {
            Some(item) => Self::parse_msgpack_breadcrumbs(config, item)?.unwrap_or_default(),
            None => Array::new(),
        };

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

        Ok(event)
    }

    /// Extracts the primary event payload from an envelope.
    ///
    /// The event is obtained from only one source in the following precedence:
    ///  1. An explicit event item. This is also the case for JSON uploads.
    ///  2. A security report item.
    ///  3. Attachments `__sentry-event` and `__sentry-breadcrumbs1/2`.
    ///  4. A multipart form data body.
    ///  5. If none match, `Annotated::empty()`.
    fn extract_event(&self, envelope: &mut Envelope) -> Result<Annotated<Event>, ProcessingError> {
        // Remove all items first, and then process them. After this function returns, only
        // attachments can remain in the envelope. The event will be added again at the end of
        // `process_event`.
        let event_item = envelope.take_item_by(|item| item.ty() == ItemType::Event);
        let security_item = envelope.take_item_by(|item| item.ty() == ItemType::SecurityReport);
        let form_item = envelope.take_item_by(|item| item.ty() == ItemType::FormData);
        let attachment_item = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::MsgpackEvent));
        let breadcrumbs_item1 = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::Breadcrumbs));
        let breadcrumbs_item2 = envelope
            .take_item_by(|item| item.attachment_type() == Some(AttachmentType::Breadcrumbs));

        if let Some(item) = event_item {
            log::trace!("processing json event {}", envelope.event_id());
            return Ok(metric!(timer("event_processing.deserialize"), {
                self.event_from_json_payload(item)?
            }));
        }

        if let Some(item) = security_item {
            log::trace!("processing security report {}", envelope.event_id());
            return Ok(self.event_from_security_report(item)?);
        }

        if attachment_item.is_some() || breadcrumbs_item1.is_some() || breadcrumbs_item2.is_some() {
            log::trace!("extracting attached event data {}", envelope.event_id());
            return Ok(Self::event_from_attachments(
                &self.config,
                attachment_item,
                breadcrumbs_item1,
                breadcrumbs_item2,
            )?);
        }

        if let Some(item) = form_item {
            log::trace!("extracting form data {}", envelope.event_id());
            let mut value = SerdeValue::Object(Default::default());
            self.merge_formdata(&mut value, item);
            return Ok(Annotated::deserialize_with_meta(value).unwrap_or_default());
        }

        Ok(Annotated::empty())
    }

    #[cfg(feature = "processing")]
    fn store_process_event(
        &self,
        event: &mut Annotated<Event>,
        envelope: &Envelope,
        project_state: &ProjectState,
    ) -> Result<(), ProcessingError> {
        let geoip_lookup = self.geoip_lookup.as_ref().map(Arc::as_ref);
        let key_id = project_state
            .get_public_key_config(&envelope.meta().public_key())
            .and_then(|k| Some(k.numeric_id?.to_string()));

        if key_id.is_none() {
            log::error!("can't find key in project config, but we verified auth before already");
        }

        let store_config = StoreConfig {
            project_id: Some(envelope.meta().project_id()),
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
        };

        let mut store_processor = StoreProcessor::new(store_config, geoip_lookup);
        metric!(timer("event_processing.process"), {
            process_value(event, &mut store_processor, ProcessingState::root())
                .map_err(|_| ProcessingError::InvalidTransaction)?;
        });

        // Event filters assume a normalized event. Unfortunately, this requires us to run
        // expensive normalization first.
        if let Some(event) = event.value_mut() {
            let client_ip = envelope.meta().client_addr();
            let filter_settings = &project_state.config.filter_settings;
            let filter_result = metric!(timer("event_processing.filtering"), {
                should_filter(event, client_ip, filter_settings)
            });

            if let Err(reason) = filter_result {
                // If the event should be filtered, no more processing is needed
                return Err(ProcessingError::EventFiltered(reason));
            }

            // TODO: Remove this once cutover is complete.
            event.other.insert(
                "_relay_processed".to_owned(),
                Annotated::new(Value::Bool(true)),
            );
        }

        // Run rate limiting after normalizing the event and running all filters. If the event is
        // dropped or filtered for a different reason before that, it should not count against
        // quotas. Also, this allows to reduce the number of requests to the rate limiter (currently
        // implemented in Redis).
        if let Some(organization_id) = project_state.organization_id {
            let key_config = project_state.get_public_key_config(&envelope.meta().public_key());
            if let Some(key_config) = key_config {
                let rate_limit = metric!(timer("event_processing.rate_limiting"), {
                    self.rate_limiter
                        .is_rate_limited(&key_config.quotas, organization_id)
                        .map_err(ProcessingError::QuotasFailed)?
                });

                if let Some(retry_after) = rate_limit {
                    // TODO: Use quota prefix to determine scope
                    let scope = RateLimitScope::Key(key_config.public_key.clone());
                    return Err(ProcessingError::RateLimited(RateLimit(scope, retry_after)));
                }
            }
        }

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
        }
    }

    fn process(&self, message: ProcessEvent) -> Result<ProcessEventResponse, ProcessingError> {
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

        // Extract the event from the envelope. This removes all items from the envelope that should
        // not be forwarded, including the event item itself.
        let mut event = self.extract_event(&mut envelope)?;

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

            if minidump_attachment.is_some() {
                self.write_native_placeholder(&mut event, true);
            } else if apple_crash_report_attachment.is_some() {
                self.write_native_placeholder(&mut event, false);
            }
        }

        let event_id = envelope.event_id();
        if let Some(event) = event.value_mut() {
            // Ensure that the event id in the payload is consistent with the envelope. If an event
            // id was ingested, this will already be the case. Otherwise, this will insert a new
            // event id. To be defensive, we always overwrite to ensure consistency.
            event.id = Annotated::new(event_id);
        } else {
            // If we have an envelope without event at this point, we are done with processing. This
            // envelope only contains attachments or user reports. We should not run filters or
            // apply rate limits.
            log::trace!("no event for envelope {}, skipping processing", event_id);
            return Ok(ProcessEventResponse { envelope });
        }

        if_processing! {
            self.store_process_event(&mut event, &envelope, &message.project_state)?;
        }

        // Run PII stripping last since normalization can add PII (e.g. IP addresses).
        metric!(timer("event_processing.pii"), {
            for pii_config in message.project_state.config.pii_configs() {
                let mut processor = PiiProcessor::new(pii_config);
                process_value(&mut event, &mut processor, ProcessingState::root())
                    .map_err(ProcessingError::ProcessingFailed)?;
            }
        });

        // We're done now. Serialize the event back into JSON and put it in an envelope so that it
        // can be sent to the upstream or processing queue.
        let data = metric!(timer("event_processing.serialization"), {
            event.to_json().map_err(ProcessingError::SerializeFailed)?
        });

        // Add the normalized event back to the envelope. All the other items are attachments.
        let mut event_item = Item::new(ItemType::Event);
        event_item.set_payload(ContentType::Json, data);
        if let Some(ty) = event.value().and_then(|e| e.ty.value()) {
            event_item.set_event_type(*ty);
        }
        envelope.add_item(event_item);

        Ok(ProcessEventResponse { envelope })
    }
}

impl Actor for EventProcessor {
    type Context = SyncContext<Self>;
}

struct ProcessEvent {
    pub envelope: Envelope,
    pub project_state: Arc<ProjectState>,
    pub start_time: Instant,
}

#[cfg_attr(not(feature = "processing"), allow(dead_code))]
struct ProcessEventResponse {
    envelope: Envelope,
}

impl Message for ProcessEvent {
    type Result = Result<ProcessEventResponse, ProcessingError>;
}

impl Handler<ProcessEvent> for EventProcessor {
    type Result = Result<ProcessEventResponse, ProcessingError>;

    fn handle(&mut self, message: ProcessEvent, _context: &mut Self::Context) -> Self::Result {
        metric!(timer("event.wait_time") = message.start_time.elapsed());
        metric!(timer("event.processing_time"), { self.process(message) })
    }
}

pub type CapturedEvent = Result<Envelope, String>;

pub struct EventManager {
    config: Arc<Config>,
    upstream: Addr<UpstreamRelay>,
    processor: Addr<EventProcessor>,
    current_active_events: u32,
    outcome_producer: Addr<OutcomeProducer>,

    #[cfg(feature = "processing")]
    store_forwarder: Option<Addr<StoreForwarder>>,

    captured_events: Arc<RwLock<BTreeMap<EventId, CapturedEvent>>>,
}

impl EventManager {
    pub fn create(
        config: Arc<Config>,
        upstream: Addr<UpstreamRelay>,
        outcome_producer: Addr<OutcomeProducer>,
    ) -> Result<Self, ServerError> {
        let thread_count = config.cpu_concurrency();
        log::info!("starting {} event processing workers", thread_count);

        #[cfg(feature = "processing")]
        let processor = {
            let geoip_lookup = match config.geoip_path() {
                Some(p) => Some(Arc::new(
                    GeoIpLookup::open(p).context(ServerErrorKind::GeoIpError)?,
                )),
                None => None,
            };

            let rate_limiter = RateLimiter::new(&config).context(ServerErrorKind::RedisError)?;

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

pub struct QueueEvent {
    pub envelope: Envelope,
    pub project: Addr<Project>,
    pub start_time: Instant,
}

impl Message for QueueEvent {
    type Result = Result<EventId, QueueEventError>;
}

impl Handler<QueueEvent> for EventManager {
    type Result = Result<EventId, QueueEventError>;

    fn handle(&mut self, message: QueueEvent, context: &mut Self::Context) -> Self::Result {
        metric!(histogram("event.queue_size") = u64::from(self.current_active_events));

        metric!(
            histogram("event.queue_size.pct") = {
                let queue_size_pct = self.current_active_events as f32 * 100.0
                    / self.config.event_buffer_size() as f32;
                queue_size_pct.floor() as u64
            }
        );

        if self.config.event_buffer_size() <= self.current_active_events {
            return Err(QueueEventError::TooManyEvents);
        }

        self.current_active_events += 1;
        let event_id = message.envelope.event_id();

        // Actual event handling is performed asynchronously in a separate future. The lifetime of
        // that future will be tied to the EventManager's context. This allows to keep the Project
        // actor alive even if it is cleaned up in the ProjectManager.
        context.notify(HandleEvent {
            envelope: message.envelope,
            project: message.project,
            start_time: message.start_time,
        });

        log::trace!("queued event {}", event_id);
        Ok(event_id)
    }
}

struct HandleEvent {
    pub envelope: Envelope,
    pub project: Addr<Project>,
    pub start_time: Instant,
}

impl Message for HandleEvent {
    type Result = Result<(), ()>;
}

impl Handler<HandleEvent> for EventManager {
    type Result = ResponseActFuture<Self, (), ()>;

    fn handle(&mut self, message: HandleEvent, _context: &mut Self::Context) -> Self::Result {
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

        let HandleEvent {
            envelope,
            project,
            start_time,
        } = message;

        let event_id = envelope.event_id();
        let is_event = envelope
            .get_item_by(|item| item.ty() == ItemType::Event)
            .is_some();
        let project_id = envelope.meta().project_id();
        let remote_addr = envelope.meta().client_addr();
        let meta_clone = Arc::new(envelope.meta().clone());

        let org_id_for_err = Rc::new(Mutex::new(None::<u64>));

        metric!(set("unique_projects") = project_id as i64);

        let future = project
            .send(GetEventAction::fetched(meta_clone))
            .map_err(ProcessingError::ScheduleFailed)
            .and_then(|action| match action.map_err(ProcessingError::NoAction)? {
                EventAction::Accept => Ok(()),
                EventAction::RetryAfter(r) => Err(ProcessingError::RateLimited(r)),
                EventAction::Discard(reason) => Err(ProcessingError::EventRejected(reason)),
            })
            .and_then(clone!(project, |_| {
                project
                    .send(GetProjectState)
                    .map_err(ProcessingError::ScheduleFailed)
                    .and_then(|result| result.map_err(ProcessingError::ProjectFailed))
            }))
            .and_then(clone!(org_id_for_err, |project_state| {
                *org_id_for_err.lock() = project_state.organization_id;
                processor
                    .send(ProcessEvent {
                        envelope,
                        project_state,
                        start_time,
                    })
                    .map_err(ProcessingError::ScheduleFailed)
                    .flatten()
            }))
            .and_then(clone!(captured_events, |processed| {
                let envelope = processed.envelope;

                #[cfg(feature = "processing")]
                {
                    if let Some(store_forwarder) = store_forwarder {
                        log::trace!("sending envelope to kafka {}", event_id);
                        let future = store_forwarder
                            .send(StoreEvent {
                                envelope,
                                start_time,
                                project_id,
                            })
                            .map_err(ProcessingError::ScheduleFailed)
                            .and_then(move |result| result.map_err(ProcessingError::StoreFailed));

                        return Box::new(future) as ResponseFuture<_, _>;
                    }
                }

                // if we are in capture mode, we stash away the event instead of
                // forwarding it.
                if capture {
                    log::debug!("capturing event {}", event_id);
                    captured_events
                        .write()
                        .insert(event_id, CapturedEvent::Ok(envelope));
                    return Box::new(Ok(()).into_future()) as ResponseFuture<_, _>;
                }

                let public_key = envelope.meta().public_key().to_string();

                log::trace!("sending event to sentry endpoint {}", event_id);
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
                            UpstreamRequestError::RateLimited(secs) => {
                                ProcessingError::RateLimited(RateLimit(
                                    // TODO: Maybe add a header that tells us the value for
                                    // RateLimitScope?
                                    RateLimitScope::Key(public_key),
                                    RetryAfter {
                                        when: Instant::now() + Duration::from_secs(secs),
                                        reason_code: None,
                                    },
                                ))
                            }
                            other => ProcessingError::SendFailed(other),
                        })
                    });

                Box::new(future) as ResponseFuture<_, _>
            }))
            .into_actor(self)
            .timeout(self.config.event_buffer_expiry(), ProcessingError::Timeout)
            .map(|_, _, _| metric!(counter("event.accepted") += 1))
            .map_err(clone!(project, captured_events, |error, _, _| {
                // Do not track outcomes or capture events for non-event envelopes (such as
                // individual attachments)
                if !is_event {
                    return;
                }
                // if we are in capture mode, we stash away the event instead of
                // forwarding it.
                if capture {
                    log::debug!("capturing failed event {}", event_id);
                    let msg = LogError(&error).to_string();
                    captured_events
                        .write()
                        .insert(event_id, CapturedEvent::Err(msg));
                }

                metric!(counter("event.rejected") += 1);
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
                    ProcessingError::InvalidUnrealReport(_) => {
                        Some(Outcome::Invalid(DiscardReason::ProcessUnreal))
                    }
                    ProcessingError::DuplicateItem(_) => {
                        Some(Outcome::Invalid(DiscardReason::DuplicateItem))
                    }

                    // Rate limits need special handling: Cache them on the project to avoid
                    // expensive processing while the limit is active.
                    ProcessingError::RateLimited(ref rate_limit) => {
                        project.do_send(rate_limit.clone());
                        Some(Outcome::RateLimited(rate_limit.clone()))
                    }

                    // Processing-only outcomes (Sentry-internal Relays)
                    #[cfg(feature = "processing")]
                    ProcessingError::InvalidTransaction => {
                        Some(Outcome::Invalid(DiscardReason::InvalidTransaction))
                    }
                    #[cfg(feature = "processing")]
                    ProcessingError::EventFiltered(ref filter_stat_key) => {
                        Some(Outcome::Filtered(*filter_stat_key))
                    }

                    // Internal errors
                    ProcessingError::SerializeFailed(_)
                    | ProcessingError::ScheduleFailed(_)
                    | ProcessingError::ProjectFailed(_)
                    | ProcessingError::Timeout
                    | ProcessingError::ProcessingFailed(_)
                    | ProcessingError::NoAction(_) => {
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
                    log::error!("error processing event {}: {}", event_id, LogError(&error));
                } else {
                    log::info!("dropped event {}: {}", event_id, LogError(&error));
                }

                if let Some(outcome) = outcome_params {
                    outcome_producer.do_send(TrackOutcome {
                        timestamp: Instant::now(),
                        project_id: project_id,
                        org_id: *(org_id_for_err.lock()),
                        key_id: None,
                        outcome,
                        event_id: Some(event_id),
                        remote_addr,
                    })
                }
            }))
            .then(move |x, slf, _| {
                metric!(timer("event.total_time") = start_time.elapsed());
                slf.current_active_events -= 1;
                result(x)
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

    use chrono::naive::{NaiveDate, NaiveDateTime};
    use chrono::{DateTime, Utc};
    use serde_json::Map as SerdeMap;

    fn create_breadcrumbs_envelope(breadcrumbs: &[(Option<NaiveDateTime>, &str)]) -> Item {
        let breadcrumbs: Vec<SerdeValue> = breadcrumbs
            .iter()
            .map(|(ndt, msg)| {
                let mut m = SerdeMap::new();
                ndt.map(|ndt| {
                    m.insert(
                        "timestamp".into(),
                        SerdeValue::from(DateTime::<Utc>::from_utc(ndt, Utc).to_rfc3339()),
                    )
                });
                m.insert("message".into(), SerdeValue::from(*msg));
                m.into()
            })
            .collect();

        let v = rmp_serde::to_vec(&breadcrumbs).unwrap();
        let mut ret_val = Item::new(ItemType::Attachment);
        ret_val.set_payload(ContentType::OctetStream, v);
        ret_val
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
    fn message_pack_breadcrumbs_replace_the_existing_bread_crumbs() {
        let item = create_breadcrumbs_envelope(&[(None, "new1")]);

        let evt =
            EventProcessor::event_from_attachments(&Config::default(), None, Some(item), None)
                .unwrap();

        let breadcrumbs = breadcrumbs_from_event(&evt);

        assert_eq!(breadcrumbs.len(), 1);
        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        assert_eq!("new1", first_breadcrumb_message);

        let item = create_breadcrumbs_envelope(&[(None, "new2")]);

        let evt =
            EventProcessor::event_from_attachments(&Config::default(), None, None, Some(item))
                .unwrap();

        let breadcrumbs = breadcrumbs_from_event(&evt);
        assert_eq!(breadcrumbs.len(), 1);

        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        assert_eq!("new2", first_breadcrumb_message);
    }

    #[test]
    fn message_pack_breadcrumbs_are_ordered_by_date_and_capped() {
        let d1 = NaiveDate::from_ymd(2019, 10, 10).and_hms(12, 10, 10);
        let d2 = NaiveDate::from_ymd(2019, 10, 11).and_hms(12, 10, 10);
        let item1 = create_breadcrumbs_envelope(&[(None, "old1"), (Some(d1), "old2")]);
        let item2 = create_breadcrumbs_envelope(&[(Some(d2), "new")]);

        let evt = EventProcessor::event_from_attachments(
            &Config::default(),
            None,
            Some(item1),
            Some(item2),
        )
        .unwrap();

        let breadcrumbs = breadcrumbs_from_event(&evt);
        assert_eq!(breadcrumbs.len(), 2);

        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        let second_breadcrumb_message = breadcrumbs[1].value().unwrap().message.value().unwrap();
        assert_eq!("old2", first_breadcrumb_message);
        assert_eq!("new", second_breadcrumb_message);

        // now try new/old
        let item1 = create_breadcrumbs_envelope(&[(Some(d2), "new")]);
        let item2 = create_breadcrumbs_envelope(&[(None, "old1"), (Some(d1), "old2")]);

        let evt = EventProcessor::event_from_attachments(
            &Config::default(),
            None,
            Some(item1),
            Some(item2),
        )
        .unwrap();

        let breadcrumbs = breadcrumbs_from_event(&evt);
        assert_eq!(breadcrumbs.len(), 2);

        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        let second_breadcrumb_message = breadcrumbs[1].value().unwrap().message.value().unwrap();
        assert_eq!("old2", first_breadcrumb_message);
        assert_eq!("new", second_breadcrumb_message);
    }
}
