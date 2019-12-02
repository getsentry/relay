use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix::fut::result;
use actix::prelude::*;
use failure::{Fail, ResultExt};
use futures::prelude::*;
use parking_lot::{Mutex, RwLock};
use rmp_serde as mps;
use serde_json::{Map as SerdeMap, Value as SerdeValue};

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
use crate::envelope::{self, ContentType, Envelope, Item, ItemType};
use crate::quotas::{QuotasError, RateLimiter};
use crate::service::{ServerError, ServerErrorKind};
use crate::utils::{get_sentry_entry_indexes, merge_vals, update_json_object, FutureExt};

#[cfg(feature = "processing")]
use {
    crate::actors::store::{StoreError, StoreEvent, StoreForwarder},
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

    #[fail(display = "failed to apply quotas")]
    QuotasFailed(#[cause] QuotasError),

    #[fail(display = "event exceeded its configured lifetime")]
    Timeout,
}

struct EventProcessor {
    rate_limiter: RateLimiter,
    #[cfg(feature = "processing")]
    config: Arc<Config>,
    #[cfg(feature = "processing")]
    geoip_lookup: Option<Arc<GeoIpLookup>>,
}

impl EventProcessor {
    #[cfg(feature = "processing")]
    pub fn new(
        config: Arc<Config>,
        geoip_lookup: Option<Arc<GeoIpLookup>>,
        rate_limiter: RateLimiter,
    ) -> Self {
        Self {
            config,
            geoip_lookup,
            rate_limiter,
        }
    }

    #[cfg(not(feature = "processing"))]
    pub fn new(rate_limiter: RateLimiter) -> Self {
        Self { rate_limiter }
    }

    fn message_pack_to_annotated_breadcrumbs(item: &Item) -> Option<Array<Breadcrumb>> {
        let payload = item.payload();
        let des = &mut mps::Deserializer::from_slice(&payload);

        Annotated::<Array<Breadcrumb>>::deserialize_with_meta(des)
            .ok()
            .and_then(Annotated::into_value)
    }

    /// Merges breadcrumbs passed in as an message pack attachment (in `__sentry-breacrumbs1/2`)
    fn add_message_pack_breadcrumbs(
        evt: &mut Annotated<Event>,
        breadcrumbs1: Option<Item>,
        breadcrumbs2: Option<Item>,
    ) {
        let mut breadcrumbs1 = breadcrumbs1
            .as_ref()
            .and_then(Self::message_pack_to_annotated_breadcrumbs)
            .unwrap_or_default();

        let mut breadcrumbs2 = breadcrumbs2
            .as_ref()
            .and_then(Self::message_pack_to_annotated_breadcrumbs)
            .unwrap_or_default();

        let timestamp1 = breadcrumbs1
            .iter()
            .rev()
            .find_map(|breadcrumb| breadcrumb.value().and_then(|b| b.timestamp.value()));

        let timestamp2 = breadcrumbs2
            .iter()
            .rev()
            .find_map(|breadcrumb| breadcrumb.value().and_then(|b| b.timestamp.value()));

        // sort breadcrumbs by date ( we presume that last timestamp from each row gives the
        // relative sequence of the whole sequence, i.e. we don't need to splice the sequences
        // to get the breadrumbs sorted)
        if timestamp1 > timestamp2 {
            std::mem::swap(&mut breadcrumbs1, &mut breadcrumbs2);
        }

        // limit the total length of the breadcrumbs, we presume that if we have both
        // breadcrumbs with items one contains the maximum number of breadcrumbs allowed.
        let max_length = std::cmp::max(breadcrumbs1.len(), breadcrumbs2.len());

        breadcrumbs1.extend(breadcrumbs2);

        if breadcrumbs1.len() > max_length {
            // keep only the last max_length elements from the vectors
            breadcrumbs1.drain(0..(breadcrumbs1.len() - max_length));
        }

        let evt = evt.get_or_insert_with(Event::default);
        evt.breadcrumbs = Annotated::new(Values {
            values: Annotated::new(breadcrumbs1.into()),
            other: Object::default(),
        });
    }

    /// Writes a placeholder to indicate that this event has an associated minidump.
    ///
    ///   This will indicate to the ingestion pipeline that this event will need to be
    ///   processed. The payload can be checked via ``is_minidump_event``.
    fn write_minidump_placeholder(evt: &mut Annotated<Event>) {
        let event = evt.get_or_insert_with(Event::default);

        // Minidump events must be native platform.
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

        exceptions.push(Annotated::new(Exception {
            ty: Annotated::new("Minidump".to_string()),
            value: Annotated::new(JsonLenientString("Invalid Minidump".to_string())),
            mechanism: Annotated::new(Mechanism {
                ty: Annotated::from("minidump".to_string()),
                handled: Annotated::from(false),
                synthetic: Annotated::from(true),
                ..Mechanism::default()
            }),
            ..Exception::default()
        }));
    }

    /// Merge form data from a multipart request into the event.
    ///
    /// Some SDKs send event and bredcrumbs as messagepack attachments.
    /// This function merges them into the main event.
    fn event_from_minidump_data(
        &self,
        form_data: Item,
        event_data_mps: Option<Item>,
        breadcrumbs1: Option<Item>,
        breadcrumbs2: Option<Item>,
    ) -> Annotated<Event> {
        // Check that the form-data item is in the expected format
        // Content type is Text (since it is not a json object but multiple).
        // json arrays serialized one after the other.
        if form_data.ty() != ItemType::FormData
            || form_data.content_type() != Some(&ContentType::Text)
        {
            // This should never happen it is probably a programming error.
            log::error!("merge_form_data called without a proper FormData item");
            return Annotated::new(Event::default());
        }

        #[derive(serde::Deserialize)]
        struct StringPair<'a>(pub &'a str, pub &'a str);

        let payload = form_data.payload();
        let consolidated_data_stream =
            serde_json::Deserializer::from_slice(payload.deref()).into_iter();

        let mut event_data = SerdeValue::Object(SerdeMap::new());

        for result in consolidated_data_stream {
            match result {
                Err(e) => log::error!("Form data deserialization failed {:?}", e),
                Ok(val) => {
                    let val: StringPair = val;
                    let (key, val) = (val.0, val.1);
                    if let Some(keys) = get_sentry_entry_indexes(key) {
                        //iterate through all 'sentry[...' params
                        update_json_object(&mut event_data, &keys, val);
                    } else if key == "sentry" {
                        //this should be a json string representing the event, merge it into what
                        //we have already created
                        let evt_from_json = serde_json::from_str(val);
                        if let Ok(evt_from_json) = evt_from_json {
                            merge_vals(&mut event_data, evt_from_json)
                        }
                    } else {
                        //an unknown entry, just add it to extra
                        update_json_object(&mut event_data, &["extra", key], val);
                    }
                }
            }
        }

        if let Some(event_data_mps) = event_data_mps {
            if event_data_mps.len() < self.config.max_msgpack_event_payload_size() {
                if let Ok(evt) = mps::from_slice(event_data_mps.payload().as_ref()) {
                    merge_vals(&mut event_data, evt);
                } else {
                    log::debug!("Invalid message pack event data in form data");
                }
            }
        }

        // get the existing event or create an event if we don't already have one
        // so that we have a valid event to merge the breadcrumbs in.
        let mut annotated_event = Annotated::deserialize_with_meta(event_data).unwrap_or_default();

        Self::add_message_pack_breadcrumbs(&mut annotated_event, breadcrumbs1, breadcrumbs2);

        Self::write_minidump_placeholder(&mut annotated_event);

        annotated_event
    }

    fn process(&self, message: ProcessEvent) -> Result<ProcessEventResponse, ProcessingError> {
        let mut envelope = message.envelope;
        let event_id = envelope.event_id();

        // TODO: What about envelope headers?
        // TODO: Move or copy headers

        let event_item = envelope.take_item_by_type(ItemType::Event);
        let security_item = envelope.take_item_by_type(ItemType::SecurityReport);
        let form_item = envelope.take_item_by_type(ItemType::FormData);
        let mps_event = envelope.take_item_by_name("__sentry-event");

        let small_enough_or_none = |item: Item| {
            if item.payload().len() < self.config.max_msgpack_breadcrumb_payload_size() {
                Some(item)
            } else {
                None
            }
        };

        let mps_breadcrumbs1 = envelope
            .take_item_by_name("__sentry-breadcrumb1")
            .and_then(small_enough_or_none);

        let mps_breadcrumbs2 = envelope
            .take_item_by_name("__sentry-breadcrumb2")
            .and_then(small_enough_or_none);

        // TODO: describe validation
        let duplicate_item = envelope
            .items()
            .find(|item| item.ty() != ItemType::Attachment);

        if let Some(duplicate_item) = duplicate_item {
            return Err(ProcessingError::DuplicateItem(duplicate_item.ty()));
        }

        let mut event = match (event_item, form_item) {
            (Some(item), _) => metric!(timer("event_processing.deserialize"), {
                log::trace!("processing event {}", event_id);
                Annotated::<Event>::from_json_bytes(&item.payload())
                    .map_err(ProcessingError::InvalidJson)?
            }),
            (None, Some(item)) => {
                self.event_from_minidump_data(item, mps_event, mps_breadcrumbs1, mps_breadcrumbs2)
            }
            (None, None) => {
                log::trace!("creating empty event {}", event_id);
                Annotated::new(Event::default())
            }
        };

        if let Some(event) = event.value_mut() {
            event.id = Annotated::new(event_id);
        }

        if let Some(event) = event.value_mut() {
            if let Some(security) = security_item {
                let data = &security.payload();
                let report_type = SecurityReportType::from_json(data)
                    .map_err(ProcessingError::InvalidJson)?
                    .ok_or(ProcessingError::InvalidSecurityReportType)?;

                let apply_result = match report_type {
                    SecurityReportType::Csp => Csp::apply_to_event(data, event),
                    SecurityReportType::ExpectCt => ExpectCt::apply_to_event(data, event),
                    SecurityReportType::ExpectStaple => ExpectStaple::apply_to_event(data, event),
                    SecurityReportType::Hpkp => Hpkp::apply_to_event(data, event),
                };

                if let Some(release) = security
                    .get_header("sentry_release")
                    .and_then(Value::as_str)
                {
                    event.release = Annotated::from(LenientString(release.to_owned()));
                }

                if let Some(env) = security
                    .get_header("sentry_environment")
                    .and_then(Value::as_str)
                {
                    event.environment = Annotated::from(env.to_owned());
                }

                apply_result.map_err(ProcessingError::InvalidSecurityReport)?;
            }
        }

        #[cfg(feature = "processing")]
        {
            if self.config.processing_enabled() {
                let geoip_lookup = self.geoip_lookup.as_ref().map(Arc::as_ref);
                let key_id = message
                    .project_state
                    .get_public_key_config(&envelope.meta().public_key())
                    .and_then(|k| Some(k.numeric_id?.to_string()));

                if key_id.is_none() {
                    log::error!(
                        "can't find key in project config, but we verified auth before already"
                    );
                }

                let store_config = StoreConfig {
                    project_id: Some(envelope.meta().project_id()),
                    client_ip: envelope.meta().client_addr().map(IpAddr::from),
                    client: envelope.meta().client().map(str::to_owned),
                    key_id,
                    protocol_version: Some(envelope.meta().version().to_string()),
                    grouping_config: message.project_state.config.grouping_config.clone(),
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
                    process_value(&mut event, &mut store_processor, ProcessingState::root())
                        .map_err(|_| ProcessingError::InvalidTransaction)?;
                });

                // Event filters assume a normalized event. Unfortunately, this requires us to run
                // expensive normalization first.
                if let Some(event) = event.value_mut() {
                    let client_ip = envelope.meta().client_addr();
                    let filter_settings = &message.project_state.config.filter_settings;
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
            }
        }

        // Run rate limiting after normalizing the event and running all filters. If the event is
        // dropped or filtered for a different reason before that, it should not count against
        // quotas. Also, this allows to reduce the number of requests to the rate limiter (currently
        // implemented in Redis).
        if let Some(organization_id) = message.project_state.organization_id {
            let key_config = message
                .project_state
                .get_public_key_config(&envelope.meta().public_key());

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

        // Run PII stripping after normalization because normalization adds IP addresses to the
        // event.
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
        let rate_limiter = RateLimiter::new(&config).context(ServerErrorKind::RedisError)?;

        // TODO: Make the number configurable via config file
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

            SyncArbiter::start(
                thread_count,
                clone!(config, || EventProcessor::new(
                    config.clone(),
                    geoip_lookup.clone(),
                    rate_limiter.clone()
                )),
            )
        };

        #[cfg(not(feature = "processing"))]
        let processor = SyncArbiter::start(thread_count, move || {
            EventProcessor::new(rate_limiter.clone())
        });

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

    fn started(&mut self, _ctx: &mut Self::Context) {
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
        let project_id = envelope.meta().project_id();
        let remote_addr = envelope.meta().client_addr();
        let meta_clone = Arc::new(envelope.meta().clone());

        let org_id_for_err = Arc::new(Mutex::new(None::<u64>));

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
                        log::trace!("sending event to kafka {}", event_id);
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
            .inspect(move |_| metric!(timer("event.total_time") = start_time.elapsed()))
            .into_actor(self)
            .timeout(self.config.event_buffer_expiry(), ProcessingError::Timeout)
            .map(|_, _, _| metric!(counter("event.accepted") += 1))
            .map_err(clone!(project, captured_events, |error, _, _| {
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
                let outcome_params: Option<Outcome> = match error {
                    ProcessingError::SerializeFailed(_)
                    | ProcessingError::ScheduleFailed(_)
                    | ProcessingError::ProjectFailed(_)
                    | ProcessingError::Timeout
                    | ProcessingError::ProcessingFailed(_)
                    | ProcessingError::NoAction(_)
                    | ProcessingError::QuotasFailed(_) => {
                        Some(Outcome::Invalid(DiscardReason::Internal))
                    }
                    #[cfg(feature = "processing")]
                    ProcessingError::InvalidTransaction => {
                        Some(Outcome::Invalid(DiscardReason::InvalidTransaction))
                    }
                    ProcessingError::InvalidJson(_) => {
                        Some(Outcome::Invalid(DiscardReason::InvalidJson))
                    }
                    #[cfg(feature = "processing")]
                    ProcessingError::StoreFailed(_) => {
                        Some(Outcome::Invalid(DiscardReason::Internal))
                    }
                    #[cfg(feature = "processing")]
                    ProcessingError::EventFiltered(ref filter_stat_key) => {
                        Some(Outcome::Filtered(*filter_stat_key))
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
                    // if we have an upstream, we don't emit outcomes. the upstream should deal with
                    // this
                    ProcessingError::SendFailed(_) => None,

                    ProcessingError::RateLimited(ref rate_limit) => {
                        project.do_send(rate_limit.clone());
                        Some(Outcome::RateLimited(rate_limit.clone()))
                    }

                    ProcessingError::DuplicateItem(_) => {
                        Some(Outcome::Invalid(DiscardReason::DuplicateItem))
                    }
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
            .then(|x, slf, _| {
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
    use chrono::{
        naive::{NaiveDate, NaiveDateTime},
        DateTime, Utc,
    };
    use std::iter::Iterator;

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

        let v = mps::to_vec(&breadcrumbs).unwrap();
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
        //let ndt: NaiveDateTime = NaiveDate::from_ymd(2000, 10, 12).and_hms(12, 10, 10);

        let mut evt = Annotated::<Event>::new(Event {
            breadcrumbs: Annotated::new(Values::new(vec![Annotated::new(Breadcrumb {
                message: Annotated::new("old".into()),
                ..Breadcrumb::default()
            })])),
            ..Event::default()
        });
        let item = create_breadcrumbs_envelope(&[(None, "new1")]);

        EventProcessor::add_message_pack_breadcrumbs(&mut evt, Some(item), None);

        let breadcrumbs = breadcrumbs_from_event(&evt);

        assert_eq!(breadcrumbs.len(), 1);
        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        assert_eq!("new1", first_breadcrumb_message);

        let item = create_breadcrumbs_envelope(&[(None, "new2")]);

        EventProcessor::add_message_pack_breadcrumbs(&mut evt, None, Some(item));

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

        //let mut evt = Annotated::<Event>::default();
        let mut evt = Annotated::<Event>::new(Event {
            breadcrumbs: Annotated::new(Values::new(vec![Annotated::new(Breadcrumb {
                message: Annotated::new("old".into()),
                ..Breadcrumb::default()
            })])),
            ..Event::default()
        });
        EventProcessor::add_message_pack_breadcrumbs(&mut evt, Some(item1), Some(item2));

        let breadcrumbs = breadcrumbs_from_event(&evt);

        assert_eq!(breadcrumbs.len(), 2);
        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        let second_breadcrumb_message = breadcrumbs[1].value().unwrap().message.value().unwrap();
        assert_eq!("old2", first_breadcrumb_message);
        assert_eq!("new", second_breadcrumb_message);

        // now try new/old
        let item1 = create_breadcrumbs_envelope(&[(Some(d2), "new")]);
        let item2 = create_breadcrumbs_envelope(&[(None, "old1"), (Some(d1), "old2")]);
        EventProcessor::add_message_pack_breadcrumbs(&mut evt, Some(item2), Some(item1));

        let breadcrumbs = breadcrumbs_from_event(&evt);

        assert_eq!(breadcrumbs.len(), 2);
        let first_breadcrumb_message = breadcrumbs[0].value().unwrap().message.value().unwrap();
        let second_breadcrumb_message = breadcrumbs[1].value().unwrap().message.value().unwrap();
        assert_eq!("old2", first_breadcrumb_message);
        assert_eq!("new", second_breadcrumb_message);
    }
}
