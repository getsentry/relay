use std::borrow::Cow;
use std::sync::Arc;

use actix::{ResponseFuture, SystemService};
use actix_web::http::Method;
use chrono::Utc;
use futures::compat::Future01CompatExt;
use futures01::{future, sync::oneshot, Future as _};

use relay_common::ProjectKey;
use relay_config::{Config, HttpEncoding};
use relay_general::protocol::ClientReport;
use relay_log::LogError;
use relay_metrics::{Bucket, MergeBuckets};
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, NoResponse};

use crate::actors::outcome::{DiscardReason, Outcome};
use crate::actors::processor::{EncodeEnvelope, EnvelopeProcessor};
use crate::actors::project_cache::{ProjectCache, UpdateRateLimits};
use crate::actors::test_store::{Capture, TestStore};
use crate::actors::upstream::{SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError};
use crate::envelope::{self, ContentType, Envelope, EnvelopeError, Item, ItemType};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::http::{HttpError, Request, RequestBuilder, Response};
use crate::service::{Registry, REGISTRY};
use crate::statsd::RelayHistograms;
use crate::utils::EnvelopeContext;

#[cfg(feature = "processing")]
use crate::actors::store::{Store, StoreEnvelope, StoreError};

/// Error created while handling [`SendEnvelope`].
#[derive(Debug, failure::Fail)]
#[allow(clippy::enum_variant_names)]
pub enum SendEnvelopeError {
    #[cfg(feature = "processing")]
    #[fail(display = "could not schedule submission of envelope")]
    ScheduleFailed,
    #[cfg(feature = "processing")]
    #[fail(display = "could not store envelope")]
    StoreFailed(#[cause] StoreError),
    #[fail(display = "could not build envelope for upstream")]
    EnvelopeBuildFailed(#[cause] EnvelopeError),
    #[fail(display = "could not encode request body")]
    BodyEncodingFailed(#[cause] std::io::Error),
    #[fail(display = "could not send request to upstream")]
    UpstreamRequestFailed(#[cause] UpstreamRequestError),
}

/// An upstream request that submits an envelope via HTTP.
#[derive(Debug)]
pub struct SendEnvelope {
    pub envelope_body: Vec<u8>,
    pub envelope_meta: RequestMeta,
    pub scoping: Scoping,
    pub http_encoding: HttpEncoding,
    pub response_sender: Option<oneshot::Sender<Result<(), SendEnvelopeError>>>,
    pub project_key: ProjectKey,
    partition_key: Option<String>,
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

        if let Some(partition_key) = &self.partition_key {
            builder.header("X-Sentry-Relay-Shard", partition_key);
        }

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

/// Sends an envelope to the upstream or Kafka.
#[derive(Debug)]
pub struct SubmitEnvelope {
    pub envelope: Envelope,
    pub envelope_context: EnvelopeContext,
}

/// Sends a client report to the upstream.
#[derive(Debug)]
pub struct SendClientReports {
    /// The client report to be sent.
    pub client_reports: Vec<ClientReport>,
    /// Scoping information for the client report.
    pub scoping: Scoping,
}

/// Sends a batch of pre-aggregated metrics to the upstream or Kafka.
///
/// Responds with `Err` if there was an error sending some or all of the buckets, containing the
/// failed buckets.
#[derive(Debug)]
pub struct SendMetrics {
    /// The pre-aggregated metric buckets.
    pub buckets: Vec<Bucket>,
    /// Scoping information for the metrics.
    pub scoping: Scoping,
    /// The key of the logical partition to send the metrics to.
    pub partition_key: Option<u64>,
}

/// Dispatch service for generating and submitting Envelopes.
#[derive(Debug)]
pub enum EnvelopeManager {
    SubmitEnvelope(Box<SubmitEnvelope>),
    SendClientReports(SendClientReports),
    SendMetrics(SendMetrics),
}

impl EnvelopeManager {
    pub fn from_registry() -> Addr<Self> {
        REGISTRY.get().unwrap().envelope_manager.clone()
    }
}

impl relay_system::Interface for EnvelopeManager {}

impl FromMessage<SubmitEnvelope> for EnvelopeManager {
    type Response = NoResponse;

    fn from_message(message: SubmitEnvelope, _: ()) -> Self {
        Self::SubmitEnvelope(Box::new(message))
    }
}

impl FromMessage<SendClientReports> for EnvelopeManager {
    type Response = NoResponse;

    fn from_message(message: SendClientReports, _: ()) -> Self {
        Self::SendClientReports(message)
    }
}

impl FromMessage<SendMetrics> for EnvelopeManager {
    type Response = NoResponse;

    fn from_message(message: SendMetrics, _: ()) -> Self {
        Self::SendMetrics(message)
    }
}

/// Service implementing the [`EnvelopeManager`] interface.
///
/// This service will produce envelopes to one the following backends:
///  1. The [`Store`] via Kafka if configured with `set_store_forwarder`. This is available only if
///     processing mode is compiled and enabled in configuration.
///  2. The in-memory [`TestStore`] if capture mode is enabled. This is meant for integration
///     testing and should not be used in production.
///  3. The [`UpstreamRelay`] via HTTP by default.
#[derive(Debug)]
pub struct EnvelopeManagerService {
    config: Arc<Config>,
    #[cfg(feature = "processing")]
    store_forwarder: Option<Addr<Store>>,
}

impl EnvelopeManagerService {
    /// Creates a new instance of the [`EnvelopeManager`] service.
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            #[cfg(feature = "processing")]
            store_forwarder: None,
        }
    }

    /// Configures a store forwarder to produce Envelopes to Kafka.
    #[cfg(feature = "processing")]
    pub fn set_store_forwarder(&mut self, addr: Addr<Store>) {
        self.store_forwarder = Some(addr);
    }

    /// Sends an envelope to the upstream or Kafka.
    async fn submit_envelope(
        &self,
        mut envelope: Envelope,
        scoping: Scoping,
        partition_key: Option<String>,
    ) -> Result<(), SendEnvelopeError> {
        #[cfg(feature = "processing")]
        {
            if let Some(store_forwarder) = self.store_forwarder.clone() {
                relay_log::trace!("sending envelope to kafka");
                let future = store_forwarder.send(StoreEnvelope {
                    start_time: envelope.meta().start_time(),
                    scoping,
                    envelope,
                });

                return future
                    .await
                    .map_err(|_| SendEnvelopeError::ScheduleFailed)
                    .and_then(|result| result.map_err(SendEnvelopeError::StoreFailed));
            }
        }

        // if we are in capture mode, we stash away the event instead of forwarding it.
        if Capture::should_capture(&self.config) {
            TestStore::from_registry().send(Capture::accepted(envelope));
            return Ok(());
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
            Err(e) => return Err(SendEnvelopeError::EnvelopeBuildFailed(e)),
        };

        let (tx, rx) = oneshot::channel();
        let request = SendEnvelope {
            envelope_body,
            envelope_meta: envelope.meta().clone(),
            scoping,
            http_encoding: self.config.http_encoding(),
            response_sender: Some(tx),
            project_key: scoping.project_key,
            partition_key,
        };

        if let HttpEncoding::Identity = request.http_encoding {
            UpstreamRelay::from_registry().do_send(SendRequest(request));
        } else {
            EnvelopeProcessor::from_registry().send(EncodeEnvelope::new(request));
        }

        match rx.compat().await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(_canceled) => Err(SendEnvelopeError::UpstreamRequestFailed(
                UpstreamRequestError::ChannelClosed,
            )),
        }
    }

    async fn handle_submit(&self, message: SubmitEnvelope) {
        let SubmitEnvelope {
            envelope,
            mut envelope_context,
        } = message;

        let scoping = envelope_context.scoping();
        match self.submit_envelope(envelope, scoping, None).await {
            Ok(_) => {
                envelope_context.accept();
            }
            Err(SendEnvelopeError::UpstreamRequestFailed(e)) if e.is_received() => {
                envelope_context.accept();
            }
            Err(error) => {
                // Errors are only logged for what we consider an internal discard reason. These
                // indicate errors in the infrastructure or implementation bugs.
                relay_log::with_scope(
                    |scope| scope.set_tag("project_key", scoping.project_key),
                    || relay_log::error!("error sending envelope: {}", LogError(&error)),
                );
                envelope_context.reject(Outcome::Invalid(DiscardReason::Internal));
            }
        }
    }

    async fn handle_send_metrics(&self, message: SendMetrics) {
        let SendMetrics {
            buckets,
            scoping,
            partition_key,
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

        let partition_key = partition_key.map(|x| x.to_string());
        let result = self.submit_envelope(envelope, scoping, partition_key).await;
        if let Err(err) = result {
            relay_log::trace!(
                "failed to submit the envelope, merging buckets back: {}",
                err
            );
            Registry::aggregator()
                .send(MergeBuckets::new(scoping.project_key, buckets))
                .await
                .ok();
        }
    }

    async fn handle_send_client_reports(&self, message: SendClientReports) {
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

        if let Err(e) = self.submit_envelope(envelope, scoping, None).await {
            relay_log::trace!("Failed to send envelope for client report: {:?}", e);
        }
    }

    async fn handle_message(&self, message: EnvelopeManager) {
        match message {
            EnvelopeManager::SubmitEnvelope(message) => {
                self.handle_submit(*message).await;
            }
            EnvelopeManager::SendClientReports(message) => {
                self.handle_send_client_reports(message).await;
            }
            EnvelopeManager::SendMetrics(message) => {
                self.handle_send_metrics(message).await;
            }
        }
    }
}

impl relay_system::Service for EnvelopeManagerService {
    type Interface = EnvelopeManager;

    fn spawn_handler(self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("envelope manager started");

            let service = Arc::new(self);
            while let Some(message) = rx.recv().await {
                let service = Arc::clone(&service);
                tokio::spawn(async move {
                    service.handle_message(message).await;
                });
            }

            relay_log::info!("envelope manager stopped");
        });
    }
}
