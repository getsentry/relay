use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use actix::prelude::*;
use actix_web::http::Method;
use chrono::Utc;
use failure::{AsFail, Fail};
use futures01::{future, prelude::*, sync::oneshot};

use relay_common::ProjectKey;
use relay_config::{Config, HttpEncoding, RelayMode};
use relay_general::protocol::{ClientReport, EventId};
use relay_log::LogError;
use relay_metrics::Bucket;
use relay_quotas::Scoping;
use relay_statsd::metric;

use crate::actors::outcome::{DiscardReason, Outcome};
use crate::actors::processor::{EncodeEnvelope, EnvelopeProcessor, ProcessMetrics};
use crate::actors::project_cache::{ProjectCache, UpdateRateLimits, ValidateEnvelope};
use crate::actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError};
use crate::envelope::{self, ContentType, Envelope, EnvelopeError, Item, ItemType};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::http::{HttpError, Request, RequestBuilder, Response};
use crate::service::ServerError;
use crate::statsd::RelayHistograms;
use crate::utils::{EnvelopeContext, FutureExt as _, Semaphore};

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
    #[cfg(feature = "processing")]
    store_forwarder: Option<StoreAddr<StoreEnvelope>>,
    #[cfg(feature = "processing")]
    _runtime: Runtime,
}

impl EnvelopeManager {
    pub fn create(
        config: Arc<Config>,
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
            #[cfg(feature = "processing")]
            store_forwarder,
            #[cfg(feature = "processing")]
            _runtime: runtime,
        })
    }

    /// Sends an envelope to the upstream or Kafka.
    fn submit_envelope(
        &mut self,
        project_key: ProjectKey,
        mut envelope: Envelope,
        scoping: Scoping,
        #[allow(unused_variables)] start_time: Instant,
        context: &mut <Self as Actor>::Context,
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
        if Capture::should_capture(&self.config) {
            context.notify(Capture::accepted(envelope));
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
            EnvelopeProcessor::from_registry().do_send(EncodeEnvelope::new(request));
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

    fn handle(&mut self, message: QueueEnvelope, _: &mut Self::Context) -> Self::Result {
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
            EnvelopeProcessor::from_registry().do_send(ProcessMetrics {
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

            ProjectCache::from_registry().do_send(ValidateEnvelope::new(
                project_key,
                event_envelope,
                event_context,
            ));
        }

        if envelope.is_empty() {
            // The envelope can be empty here if it contained only metrics items which were removed
            // above. In this case, the envelope was accepted and needs no further queueing.
            envelope_context.accept();
        } else {
            relay_log::trace!("queueing envelope");
            ProjectCache::from_registry().do_send(ValidateEnvelope::new(
                project_key,
                envelope,
                envelope_context,
            ));
        }

        // Actual event handling is performed asynchronously in a separate future. The lifetime of
        // that future will be tied to the EnvelopeManager's context. This allows to keep the Project
        // actor alive even if it is cleaned up in the ProjectManager.

        Ok(event_id)
    }
}

/// Sends an envelope to the upstream or Kafka.
pub struct SubmitEnvelope {
    pub envelope: Envelope,
    pub envelope_context: EnvelopeContext,
}

impl Message for SubmitEnvelope {
    type Result = ();
}

impl Handler<SubmitEnvelope> for EnvelopeManager {
    type Result = ();

    fn handle(&mut self, message: SubmitEnvelope, context: &mut Self::Context) -> Self::Result {
        let SubmitEnvelope {
            envelope,
            mut envelope_context,
        } = message;

        let scoping = envelope_context.scoping();
        let start_time = envelope.meta().start_time();
        let project_key = envelope.meta().public_key();

        self.submit_envelope(project_key, envelope, scoping, start_time, context)
            .then(move |result| match result {
                Ok(_) => {
                    envelope_context.accept();
                    Ok(())
                }
                Err(error) => {
                    match error {
                        SendEnvelopeError::UpstreamRequestFailed(e) if e.is_received() => {
                            envelope_context.accept()
                        }
                        _ => envelope_context.reject(Outcome::Invalid(DiscardReason::Internal)),
                    }

                    // TODO(ja): Error handling?
                    // // Errors are only logged for what we consider an internal discard reason. These
                    // // indicate errors in the infrastructure or implementation bugs. In other cases,
                    // // we "expect" errors and log them as debug level.
                    // relay_log::with_scope(
                    //     |scope| scope.set_tag("project_key", project_key),
                    //     || relay_log::error!("error sending envelope: {}", LogError(&error)),
                    // );

                    Err(())
                }
            })
            // TODO(ja): Capture errors
            .drop_guard("submit_envelope")
            .into_actor(self)
            .spawn(context);
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

    fn handle(&mut self, message: SendMetrics, context: &mut Self::Context) -> Self::Result {
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
            .submit_envelope(project_key, envelope, scoping, Instant::now(), context)
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

    fn handle(&mut self, message: SendClientReports, ctx: &mut Self::Context) -> Self::Result {
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
            .submit_envelope(scoping.project_key, envelope, scoping, Instant::now(), ctx)
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

/// Inserts an envelope or failure into internal captures.
///
/// Can be retrieved using [`GetCapturedEnvelope`]. Use [`Capture::should_capture`] to check whether
/// the message should even be sent to reduce the overheads.
pub struct Capture {
    event_id: Option<EventId>,
    capture: CapturedEnvelope,
}

impl Capture {
    /// Returns `true` if Relay is in capture mode.
    ///
    /// The `Capture` message can still be sent and and will be ignored. This function is purely for
    /// optimization purposes.
    pub fn should_capture(config: &Config) -> bool {
        matches!(config.relay_mode(), RelayMode::Capture)
    }

    /// Captures an accepted envelope.
    pub fn accepted(envelope: Envelope) -> Self {
        Self {
            event_id: envelope.event_id(),
            capture: Ok(envelope),
        }
    }

    /// Captures the error that lead to envelope rejection.
    pub fn rejected<E: AsFail + ?Sized>(event_id: Option<EventId>, error: &E) -> Self {
        Self {
            event_id,
            capture: Err(LogError(error).to_string()),
        }
    }
}

impl Message for Capture {
    type Result = ();
}

impl Handler<Capture> for EnvelopeManager {
    type Result = ();

    fn handle(&mut self, msg: Capture, _ctx: &mut Self::Context) -> Self::Result {
        if let RelayMode::Capture = self.config.relay_mode() {
            match (msg.event_id, msg.capture) {
                (Some(event_id), Ok(envelope)) => {
                    relay_log::debug!("capturing envelope");
                    self.captures.insert(event_id, Ok(envelope));
                }
                (Some(event_id), Err(message)) => {
                    relay_log::debug!("capturing failed event {}", event_id);
                    self.captures.insert(event_id, Err(message));
                }

                // XXX: does not work with envelopes without event_id
                (None, Ok(_)) => relay_log::debug!("dropping non event envelope"),
                (None, Err(_)) => relay_log::debug!("dropping failed envelope without event"),
            }
        }
    }
}
