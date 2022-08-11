use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use actix_web::http::Method;
use chrono::Utc;
use failure::Fail;
use futures01::{future, prelude::*, sync::oneshot};

use relay_common::ProjectKey;
use relay_config::{Config, HttpEncoding, RelayMode};
use relay_general::protocol::{ClientReport, EventId};
use relay_log::LogError;
use relay_metrics::Bucket;
use relay_quotas::Scoping;
use relay_statsd::metric;

use crate::actors::outcome::{DiscardReason, Outcome};
use crate::actors::processor::{
    EncodeEnvelope, EnvelopeProcessor, ProcessEnvelope, ProcessMetrics, ProcessingError,
};
use crate::actors::project_cache::{
    CheckEnvelope, GetProjectState, ProjectCache, UpdateRateLimits,
};
use crate::actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError};
use crate::envelope::{self, ContentType, Envelope, EnvelopeError, Item, ItemType};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::http::{HttpError, Request, RequestBuilder, Response};
use crate::service::ServerError;
use crate::statsd::{RelayHistograms, RelaySets};
use crate::utils::{self, EnvelopeContext, FutureExt as _, Semaphore};

#[cfg(feature = "processing")]
use {
    crate::actors::store::{StoreAddr, StoreEnvelope, StoreError, StoreForwarder},
    futures::{FutureExt, TryFutureExt},
    tokio::runtime::Runtime,
};

#[derive(Debug, Fail)]
pub enum QueueEnvelopeError {
    #[fail(display = "Too many envelopes (event_buffer_size reached)")]
    TooManyEnvelopes,
}

/// Access control for envelope processing.
///
/// The buffer guard is basically a semaphore that ensures the buffer does not outgrow the maximum
/// number of envelopes configured through [`Config::envelope_buffer_size`]. To enter a new envelope
/// into the processing pipeline, use [`BufferGuard::enter`].
#[derive(Debug)]
pub struct BufferGuard {
    inner: Semaphore,
    capacity: usize,
}

impl BufferGuard {
    /// Creates a new `BufferGuard` based on config values.
    pub fn new(capacity: usize) -> Self {
        let inner = Semaphore::new(capacity);
        Self { inner, capacity }
    }

    /// Returns the unused capacity of the pipeline.
    pub fn available(&self) -> usize {
        self.inner.available()
    }

    /// Returns the number of envelopes in the pipeline.
    pub fn used(&self) -> usize {
        self.capacity.saturating_sub(self.available())
    }

    /// Reserves resources for processing an envelope in Relay.
    ///
    /// Returns `Ok(EnvelopeContext)` on success, which internally holds a handle to the reserved
    /// resources. When the envelope context is dropped, the slot is automatically reclaimed and can
    /// be reused by a subsequent call to `enter`.
    ///
    /// If the buffer is full, this function returns `Err`.
    pub fn enter(&self, envelope: &Envelope) -> Result<EnvelopeContext, QueueEnvelopeError> {
        let permit = self
            .inner
            .try_acquire()
            .ok_or(QueueEnvelopeError::TooManyEnvelopes)?;

        metric!(histogram(RelayHistograms::EnvelopeQueueSize) = self.used() as u64);

        metric!(
            histogram(RelayHistograms::EnvelopeQueueSizePct) = {
                let queue_size_pct = self.used() as f64 * 100.0 / self.capacity as f64;
                queue_size_pct.floor() as u64
            }
        );

        Ok(EnvelopeContext::new(envelope, permit))
    }
}

/// Error created while handling [`SendEnvelope`].
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum SendEnvelopeError {
    #[cfg(feature = "processing")]
    ScheduleFailed,
    #[cfg(feature = "processing")]
    StoreFailed(StoreError),
    EnvelopeBuildFailed(EnvelopeError),
    BodyEncodingFailed(std::io::Error),
    UpstreamRequestFailed(UpstreamRequestError),
}

/// Either a captured envelope or an error that occured during processing.
pub type CapturedEnvelope = Result<Envelope, String>;

/// An upstream request that submits an envelope via HTTP.
#[derive(Debug)]
pub struct SendEnvelope {
    pub envelope_body: Vec<u8>,
    pub envelope_meta: RequestMeta,
    pub scoping: Scoping,
    pub http_encoding: HttpEncoding,
    pub response_sender: Option<oneshot::Sender<Result<(), SendEnvelopeError>>>,
    pub project_key: ProjectKey,
}

impl UpstreamRequest for SendEnvelope {
    fn method(&self) -> Method {
        Method::POST
    }

    fn path(&self) -> Cow<'_, str> {
        format!("/api/{}/envelope/", self.scoping.project_id).into()
    }

    fn build(&mut self, mut builder: RequestBuilder) -> Result<Request, HttpError> {
        let meta = &self.envelope_meta;
        builder
            .content_encoding(self.http_encoding)
            .header_opt("Origin", meta.origin().map(|url| url.as_str()))
            .header_opt("User-Agent", meta.user_agent())
            .header("X-Sentry-Auth", meta.auth_header())
            .header("X-Forwarded-For", meta.forwarded_for())
            .header("Content-Type", envelope::CONTENT_TYPE);

        let envelope_body = self.envelope_body.clone();
        metric!(histogram(RelayHistograms::UpstreamEnvelopeBodySize) = envelope_body.len() as u64);
        builder.body(envelope_body)
    }

    fn respond(
        &mut self,
        result: Result<Response, UpstreamRequestError>,
    ) -> ResponseFuture<(), ()> {
        let sender = self.response_sender.take();

        match result {
            Ok(response) => {
                let future = response
                    .consume()
                    .map_err(UpstreamRequestError::Http)
                    .map(|_| ())
                    .then(move |body_result| {
                        sender.map(|sender| {
                            sender
                                .send(body_result.map_err(SendEnvelopeError::UpstreamRequestFailed))
                                .ok()
                        });
                        Ok(())
                    });

                Box::new(future)
            }
            Err(error) => {
                match error {
                    UpstreamRequestError::RateLimited(upstream_limits) => {
                        ProjectCache::from_registry().do_send(UpdateRateLimits::new(
                            self.project_key,
                            upstream_limits.clone().scope(&self.scoping),
                        ));
                        if let Some(sender) = sender {
                            sender
                                .send(Err(SendEnvelopeError::UpstreamRequestFailed(
                                    UpstreamRequestError::RateLimited(upstream_limits),
                                )))
                                .ok();
                        }
                    }
                    error => {
                        if let Some(sender) = sender {
                            sender
                                .send(Err(SendEnvelopeError::UpstreamRequestFailed(error)))
                                .ok();
                        }
                    }
                };
                Box::new(future::err(()))
            }
        }
    }
}

pub struct EnvelopeManager {
    config: Arc<Config>,
    buffer_guard: Arc<BufferGuard>,
    captures: BTreeMap<EventId, CapturedEnvelope>,
    processor: Addr<EnvelopeProcessor>,
    #[cfg(feature = "processing")]
    store_forwarder: Option<StoreAddr<StoreEnvelope>>,
    #[cfg(feature = "processing")]
    _runtime: Runtime,
}

impl EnvelopeManager {
    pub fn create(
        config: Arc<Config>,
        processor: Addr<EnvelopeProcessor>,
        buffer_guard: Arc<BufferGuard>,
    ) -> Result<Self, ServerError> {
        // Enter the tokio runtime so we can start spawning tasks from the outside.
        #[cfg(feature = "processing")]
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        #[cfg(feature = "processing")]
        let _guard = runtime.enter();

        #[cfg(feature = "processing")]
        let store_forwarder = if config.processing_enabled() {
            let actor = StoreForwarder::create(config.clone())?;
            Some(actor.start())
        } else {
            None
        };

        Ok(EnvelopeManager {
            config,
            buffer_guard,
            captures: BTreeMap::new(),
            processor,
            #[cfg(feature = "processing")]
            store_forwarder,
            #[cfg(feature = "processing")]
            _runtime: runtime,
        })
    }

    /// Sends an envelope to the upstream or Kafka and handles returned rate limits.
    fn send_envelope(
        &mut self,
        project_key: ProjectKey,
        mut envelope: Envelope,
        scoping: Scoping,
        #[allow(unused_variables)] start_time: Instant,
    ) -> ResponseFuture<(), SendEnvelopeError> {
        #[cfg(feature = "processing")]
        {
            if let Some(store_forwarder) = self.store_forwarder.clone() {
                relay_log::trace!("sending envelope to kafka");
                let fut = async move {
                    let addr = store_forwarder.clone();
                    addr.send(StoreEnvelope {
                        envelope,
                        start_time,
                        scoping,
                    })
                    .await
                };

                let future = fut
                    .boxed_local()
                    .compat()
                    .map_err(|_| SendEnvelopeError::ScheduleFailed)
                    .and_then(|result| result.map_err(SendEnvelopeError::StoreFailed));
                return Box::new(future);
            }
        }

        // if we are in capture mode, we stash away the event instead of forwarding it.
        if self.config.relay_mode() == RelayMode::Capture {
            // XXX: this is wrong because captured_events does not take envelopes without
            // event_id into account.
            if let Some(event_id) = envelope.event_id() {
                relay_log::debug!("capturing envelope");
                self.captures.insert(event_id, Ok(envelope));
            } else {
                relay_log::debug!("dropping non event envelope");
            }

            return Box::new(future::ok(()));
        }

        relay_log::trace!("sending envelope to sentry endpoint");

        // Override the `sent_at` timestamp. Since the envelope went through basic
        // normalization, all timestamps have been corrected. We propagate the new
        // `sent_at` to allow the next Relay to double-check this timestamp and
        // potentially apply correction again. This is done as close to sending as
        // possible so that we avoid internal delays.
        envelope.set_sent_at(Utc::now());

        let envelope_body = match envelope.to_vec() {
            Ok(v) => v,
            Err(e) => return Box::new(future::err(SendEnvelopeError::EnvelopeBuildFailed(e))),
        };

        let (tx, rx) = oneshot::channel();
        let request = SendEnvelope {
            envelope_body,
            envelope_meta: envelope.meta().clone(),
            scoping,
            http_encoding: self.config.http_encoding(),
            response_sender: Some(tx),
            project_key,
        };

        if let HttpEncoding::Identity = request.http_encoding {
            UpstreamRelay::from_registry().do_send(SendRequest(request));
        } else {
            self.processor.do_send(EncodeEnvelope::new(request));
        }

        Box::new(
            rx.map_err(|_| {
                SendEnvelopeError::UpstreamRequestFailed(UpstreamRequestError::ChannelClosed)
            })
            .flatten(),
        )
    }
}

impl Actor for EnvelopeManager {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        // Set the mailbox size to the size of the envelope buffer. This is a rough estimate but
        // should ensure that we're not dropping envelopes unintentionally after we've accepted
        // them.
        let mailbox_size = self.config.envelope_buffer_size() as usize;
        context.set_mailbox_capacity(mailbox_size);
        relay_log::info!("envelope manager started");
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("envelope manager stopped");
    }
}

impl Supervised for EnvelopeManager {}

impl SystemService for EnvelopeManager {}

impl Default for EnvelopeManager {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
    }
}

/// Queues an envelope for processing.
///
/// Depending on the items in the envelope, there are multiple outcomes:
///
/// - Events and event related items, such as attachments, are always queued together. See
///   [`HandleEnvelope`] for a full description of how queued envelopes are processed by the
///   `EnvelopeManager`.
/// - Sessions and Session batches are always queued separately. If they occur in the same envelope
///   as an event, they are split off.
/// - Metrics are directly sent to the `EnvelopeProcessor`, bypassing the manager's queue and going
///   straight into metrics aggregation. See [`ProcessMetrics`] for a full description.
///
/// Queueing can fail if the queue exceeds [`Config::envelope_buffer_size`]. In this case, `Err` is
/// returned and the envelope is not queued. Otherwise, this message responds with `Ok`. If it
/// contained an event-related item, such as an event payload or an attachment, this contains
/// `Some(EventId)`.
pub struct QueueEnvelope {
    pub envelope: Envelope,
    pub envelope_context: EnvelopeContext,
    pub project_key: ProjectKey,
    pub start_time: Instant,
}

impl Message for QueueEnvelope {
    type Result = Result<Option<EventId>, QueueEnvelopeError>;
}

impl Handler<QueueEnvelope> for EnvelopeManager {
    type Result = Result<Option<EventId>, QueueEnvelopeError>;

    fn handle(&mut self, message: QueueEnvelope, context: &mut Self::Context) -> Self::Result {
        let QueueEnvelope {
            mut envelope,
            mut envelope_context,
            project_key,
            start_time,
        } = message;

        let event_id = envelope.event_id();

        // Remove metrics from the envelope and queue them directly on the project's `Aggregator`.
        let mut metric_items = Vec::new();
        let is_metric = |i: &Item| matches!(i.ty(), ItemType::Metrics | ItemType::MetricBuckets);
        while let Some(item) = envelope.take_item_by(is_metric) {
            metric_items.push(item);
        }

        if !metric_items.is_empty() {
            relay_log::trace!("sending metrics into processing queue");
            self.processor.do_send(ProcessMetrics {
                items: metric_items,
                project_key,
                start_time,
                sent_at: envelope.sent_at(),
            });
        }

        // Split the envelope into event-related items and other items. This allows to fast-track:
        //  1. Envelopes with only session items. They only require rate limiting.
        //  2. Event envelope processing can bail out if the event is filtered or rate limited,
        //     since all items depend on this event.
        if let Some(event_envelope) = envelope.split_by(Item::requires_event) {
            relay_log::trace!("queueing separate envelope for non-event items");

            // The envelope has been split, so we need to fork the context.
            let event_context = self.buffer_guard.enter(&event_envelope)?;
            // Update the old context after successful forking.
            envelope_context.update(&envelope);

            context.notify(HandleEnvelope {
                envelope: event_envelope,
                envelope_context: event_context,
                project_key,
            });
        }

        if envelope.is_empty() {
            // The envelope can be empty here if it contained only metrics items which were removed
            // above. In this case, the envelope was accepted and needs no further queueing.
            envelope_context.accept();
        } else {
            relay_log::trace!("queueing envelope");
            context.notify(HandleEnvelope {
                envelope,
                envelope_context,
                project_key,
            });
        }

        // Actual event handling is performed asynchronously in a separate future. The lifetime of
        // that future will be tied to the EnvelopeManager's context. This allows to keep the Project
        // actor alive even if it is cleaned up in the ProjectManager.

        Ok(event_id)
    }
}

/// Handles a queued envelope.
///
/// 1. Ensures the project state is up-to-date and then validates the envelope against the state and
///    cached rate limits. See [`CheckEnvelope`] for full information.
/// 2. Executes dynamic sampling using the sampling project.
/// 3. Runs the envelope through the [`EnvelopeProcessor`] worker pool, which parses items, applies
///    normalization, and runs filtering logic.
/// 4. Sends the envelope to the upstream or stores it in Kafka, depending on the
///    [`processing`](Config::processing_enabled) flag.
/// 5. Captures [`Outcome`]s for dropped items and envelopes.
///
/// This operation is invoked by [`QueueEnvelope`] for envelopes containing all items except
/// metrics.
struct HandleEnvelope {
    pub envelope: Envelope,
    pub envelope_context: EnvelopeContext,
    pub project_key: ProjectKey,
}

impl Message for HandleEnvelope {
    type Result = Result<(), ()>;
}

impl Handler<HandleEnvelope> for EnvelopeManager {
    type Result = ResponseActFuture<Self, (), ()>;

    fn handle(&mut self, message: HandleEnvelope, _ctx: &mut Self::Context) -> Self::Result {
        // We measure three timers while handling envelopes, once they have been initially accepted:
        //
        // 1. `event.wait_time`: The time we take to get all dependencies for envelopes before they
        //    actually start processing. This includes scheduling overheads, project config
        //    fetching, batched requests and congestions in the sync processor arbiter. This does
        //    not include delays in the incoming request (body upload) and skips all envelopes that
        //    are fast-rejected.
        //
        // 2. `event.processing_time`: The time the sync processor takes to parse the event payload,
        //    apply normalizations, strip PII and finally re-serialize it into a byte stream. This
        //    is recorded directly in the EnvelopeProcessor.
        //
        // 3. `event.total_time`: The full time an envelope takes from being initially accepted up
        //    to being sent to the upstream (including delays in the upstream). This can be regarded
        //    the total time an envelope spent in this Relay, corrected by incoming network delays.

        let processor = self.processor.clone();
        let capture = self.config.relay_mode() == RelayMode::Capture;

        let HandleEnvelope {
            envelope,
            envelope_context,
            project_key,
        } = message;

        let start_time = envelope.meta().start_time();
        let event_id = envelope.event_id();

        let future = ProjectCache::from_registry()
            .send(CheckEnvelope::fetched(
                project_key,
                envelope,
                envelope_context,
            ))
            .map_err(|_| ProcessingError::ScheduleFailed)
            .and_then(|result| result.map_err(ProcessingError::ProjectFailed))
            .and_then(move |response| {
                // Use the project id from the loaded project state to account for redirects.
                let project_id = response.scoping.project_id.value();
                metric!(set(RelaySets::UniqueProjects) = project_id as i64);

                let checked = response.result.map_err(ProcessingError::Rejected)?;
                checked.envelope.ok_or(ProcessingError::RateLimited)
            })
            .and_then(move |(envelope, envelope_context)| {
                // get the state for the current project. we can always fetch the cached version
                // even if the no_cache flag was passed, as the cache was updated prior in
                // `CheckEnvelope`.
                ProjectCache::from_registry()
                    .send(GetProjectState::new(project_key))
                    .map_err(|_| ProcessingError::ScheduleFailed)
                    .and_then(|result| result.map_err(ProcessingError::ProjectFailed))
                    .map(|state| (envelope, envelope_context, state))
            })
            .and_then(|(envelope, envelope_context, project_state)| {
                // get the state for the sampling project.
                // TODO: Could this run concurrently with main project cache fetch?
                if let Some(sampling_project_key) = utils::get_sampling_key(&envelope) {
                    let future = ProjectCache::from_registry()
                        .send(GetProjectState::new(sampling_project_key))
                        .then(move |response| {
                            Ok(ProcessEnvelope {
                                envelope,
                                envelope_context,
                                project_state,
                                // ignore all errors and leave envelope unsampled
                                sampling_project_state: response.ok().and_then(|r| r.ok()),
                            })
                        });

                    Box::new(future) as ResponseFuture<_, _>
                } else {
                    Box::new(future::ok(ProcessEnvelope {
                        envelope,
                        envelope_context,
                        project_state,
                        sampling_project_state: None,
                    }))
                }
            })
            .and_then(move |process_message| {
                processor
                    .send(process_message)
                    .map_err(|_| ProcessingError::ScheduleFailed)
                    .flatten()
            })
            .and_then(move |processed| {
                // Processing returned new rate limits. Cache them on the project to avoid expensive
                // processing while the limit is active.
                let rate_limits = processed.rate_limits;
                if rate_limits.is_limited() {
                    let project_cache = ProjectCache::from_registry();
                    project_cache.do_send(UpdateRateLimits::new(project_key, rate_limits));
                }

                processed.envelope.ok_or(ProcessingError::RateLimited)
            })
            .into_actor(self)
            .and_then(move |(envelope, mut envelope_context), slf, _| {
                let scoping = envelope_context.scoping();
                slf.send_envelope(project_key, envelope, scoping, start_time)
                    .then(move |result| match result {
                        Ok(_) => {
                            envelope_context.accept();
                            Ok(())
                        }
                        Err(error) => {
                            let outcome = Outcome::Invalid(DiscardReason::Internal);

                            Err(match error {
                                #[cfg(feature = "processing")]
                                SendEnvelopeError::ScheduleFailed => {
                                    envelope_context.reject(outcome);
                                    ProcessingError::ScheduleFailed
                                }

                                #[cfg(feature = "processing")]
                                SendEnvelopeError::StoreFailed(e) => {
                                    envelope_context.reject(outcome);
                                    ProcessingError::StoreFailed(e)
                                }

                                SendEnvelopeError::BodyEncodingFailed(e) => {
                                    envelope_context.reject(outcome);
                                    ProcessingError::BodyEncodingFailed(e)
                                }

                                SendEnvelopeError::EnvelopeBuildFailed(e) => {
                                    envelope_context.reject(outcome);
                                    ProcessingError::EnvelopeBuildFailed(e)
                                }

                                SendEnvelopeError::UpstreamRequestFailed(e) => {
                                    if e.is_received() {
                                        envelope_context.accept();
                                    } else {
                                        envelope_context.reject(outcome);
                                    }

                                    ProcessingError::UpstreamRequestFailed(e)
                                }
                            })
                        }
                    })
                    .into_actor(slf)
            })
            .map_err(move |error, slf, _| {
                // if we are in capture mode, we stash away the event instead of forwarding it.
                if capture {
                    // XXX: does not work with envelopes without event_id
                    if let Some(event_id) = event_id {
                        relay_log::debug!("capturing failed event {}", event_id);
                        let msg = LogError(&error).to_string();
                        slf.captures.insert(event_id, Err(msg));
                    } else {
                        relay_log::debug!("dropping failed envelope without event");
                    }
                }

                let outcome = error.to_outcome();
                if let Some(Outcome::Invalid(DiscardReason::Internal)) = outcome {
                    // Errors are only logged for what we consider an internal discard reason. These
                    // indicate errors in the infrastructure or implementation bugs. In other cases,
                    // we "expect" errors and log them as debug level.
                    relay_log::with_scope(
                        |scope| scope.set_tag("project_key", project_key),
                        || relay_log::error!("error processing envelope: {}", LogError(&error)),
                    );
                } else {
                    relay_log::debug!("dropped envelope: {}", LogError(&error));
                }
            })
            .drop_guard("process_envelope");

        Box::new(future)
    }
}

/// Sends a batch of pre-aggregated metrics to the upstream or Kafka.
///
/// Responds with `Err` if there was an error sending some or all of the buckets, containing the
/// failed buckets.
pub struct SendMetrics {
    /// The pre-aggregated metric buckets.
    pub buckets: Vec<Bucket>,
    /// Scoping information for the metrics.
    pub scoping: Scoping,
    /// The project of the metrics.
    pub project_key: ProjectKey,
}

impl fmt::Debug for SendMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(std::any::type_name::<Self>())
            .field("buckets", &self.buckets)
            .field("scoping", &self.scoping)
            .field("project", &format_args!("Addr<Project>"))
            .finish()
    }
}

impl Message for SendMetrics {
    type Result = Result<(), Vec<Bucket>>;
}

impl Handler<SendMetrics> for EnvelopeManager {
    type Result = ResponseFuture<(), Vec<Bucket>>;

    fn handle(&mut self, message: SendMetrics, _context: &mut Self::Context) -> Self::Result {
        let SendMetrics {
            buckets,
            scoping,
            project_key,
        } = message;

        let upstream = self.config.upstream_descriptor();
        let dsn = PartialDsn {
            scheme: upstream.scheme(),
            public_key: scoping.project_key,
            host: upstream.host().to_owned(),
            port: upstream.port(),
            path: "".to_owned(),
            project_id: Some(scoping.project_id),
        };

        let mut item = Item::new(ItemType::MetricBuckets);
        item.set_payload(ContentType::Json, Bucket::serialize_all(&buckets).unwrap());
        let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn));
        envelope.add_item(item);

        let future = self
            .send_envelope(project_key, envelope, scoping, Instant::now())
            .map_err(|_| buckets);

        Box::new(future)
    }
}

/// Sends a client report to the upstream.
pub struct SendClientReports {
    /// The client report to be sent.
    pub client_reports: Vec<ClientReport>,
    /// Scoping information for the client report.
    pub scoping: Scoping,
}

impl Message for SendClientReports {
    type Result = Result<(), ()>;
}

impl Handler<SendClientReports> for EnvelopeManager {
    type Result = ResponseFuture<(), ()>;

    fn handle(&mut self, message: SendClientReports, _context: &mut Self::Context) -> Self::Result {
        let SendClientReports {
            client_reports,
            scoping,
        } = message;

        let upstream = self.config.upstream_descriptor();
        let dsn = PartialDsn {
            scheme: upstream.scheme(),
            public_key: scoping.project_key,
            host: upstream.host().to_owned(),
            port: upstream.port(),
            path: "".to_owned(),
            project_id: Some(scoping.project_id),
        };

        let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn));
        for client_report in client_reports {
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(ContentType::Json, client_report.serialize().unwrap()); // TODO: unwrap OK?
            envelope.add_item(item);
        }
        let future = self
            .send_envelope(scoping.project_key, envelope, scoping, Instant::now())
            .map_err(|e| {
                relay_log::trace!("Failed to send envelope for client report: {:?}", e);
            });

        Box::new(future)
    }
}

/// Resolves a [`CapturedEnvelope`] by the given `event_id`.
pub struct GetCapturedEnvelope {
    pub event_id: EventId,
}

impl Message for GetCapturedEnvelope {
    type Result = Option<CapturedEnvelope>;
}

impl Handler<GetCapturedEnvelope> for EnvelopeManager {
    type Result = Option<CapturedEnvelope>;

    fn handle(
        &mut self,
        message: GetCapturedEnvelope,
        _context: &mut Self::Context,
    ) -> Self::Result {
        self.captures.get(&message.event_id).cloned()
    }
}
