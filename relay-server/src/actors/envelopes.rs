use std::borrow::Cow;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use chrono::Utc;
use relay_base_schema::project::ProjectKey;
use relay_config::{Config, HttpEncoding};
use relay_event_schema::protocol::ClientReport;
use relay_metrics::Bucket;
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, NoResponse};
use tokio::sync::oneshot;

use crate::actors::outcome::{DiscardReason, Outcome};
use crate::actors::processor::{EncodeEnvelope, EnvelopeProcessor};
use crate::actors::project_cache::{ProjectCache, UpdateRateLimits};
#[cfg(feature = "processing")]
use crate::actors::store::{Store, StoreEnvelope, StoreError};
use crate::actors::test_store::{Capture, TestStore};
use crate::actors::upstream::{
    Method, SendRequest, UpstreamRelay, UpstreamRequest, UpstreamRequestError,
};
use crate::envelope::{self, ContentType, Envelope, EnvelopeError, Item, ItemType};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::http::{HttpError, Request, RequestBuilder, Response};
use crate::statsd::RelayHistograms;
use crate::utils::{ExtractionMode, ManagedEnvelope};

use super::processor::EncodeMetrics;

/// Error created while handling [`SendEnvelope`].
#[derive(Debug, thiserror::Error)]
#[allow(clippy::enum_variant_names)]
pub enum SendEnvelopeError {
    #[cfg(feature = "processing")]
    #[error("could not schedule submission of envelope")]
    ScheduleFailed,
    #[cfg(feature = "processing")]
    #[error("could not store envelope")]
    StoreFailed(#[from] StoreError),
    #[error("could not build envelope for upstream")]
    EnvelopeBuildFailed(#[from] EnvelopeError),
    #[error("could not encode request body")]
    BodyEncodingFailed(#[source] std::io::Error),
    #[error("could not send request to upstream")]
    UpstreamRequestFailed(#[from] UpstreamRequestError),
}

#[cfg(feature = "processing")]
impl From<relay_system::SendError> for SendEnvelopeError {
    fn from(_: relay_system::SendError) -> Self {
        Self::ScheduleFailed
    }
}

/// An upstream request that submits an envelope via HTTP.
#[derive(Debug)]
pub struct SendEnvelope {
    pub envelope_body: Vec<u8>,
    pub envelope_meta: RequestMeta,
    pub project_cache: Addr<ProjectCache>,
    pub scoping: Scoping,
    pub http_encoding: HttpEncoding,
    pub response_sender: oneshot::Sender<Result<(), SendEnvelopeError>>,
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

    fn route(&self) -> &'static str {
        "envelope"
    }

    fn build(&mut self, _: &Config, builder: RequestBuilder) -> Result<Request, HttpError> {
        let envelope_body = &self.envelope_body;
        metric!(histogram(RelayHistograms::UpstreamEnvelopeBodySize) = envelope_body.len() as u64);

        let meta = &self.envelope_meta;
        builder
            .content_encoding(self.http_encoding)
            .header_opt("Origin", meta.origin().map(|url| url.as_str()))
            .header_opt("User-Agent", meta.user_agent())
            .header("X-Sentry-Auth", meta.auth_header())
            .header("X-Forwarded-For", meta.forwarded_for())
            .header("Content-Type", envelope::CONTENT_TYPE)
            .header_opt("X-Sentry-Relay-Shard", self.partition_key.as_ref())
            .body(envelope_body)
    }

    fn respond(
        self: Box<Self>,
        result: Result<Response, UpstreamRequestError>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        Box::pin(async move {
            let result = match result {
                Ok(mut response) => response.consume().await.map_err(UpstreamRequestError::Http),
                Err(error) => {
                    if let UpstreamRequestError::RateLimited(ref upstream_limits) = error {
                        self.project_cache.send(UpdateRateLimits::new(
                            self.project_key,
                            upstream_limits.clone().scope(&self.scoping),
                        ));
                    }

                    Err(error)
                }
            };

            self.response_sender
                .send(result.map_err(SendEnvelopeError::from))
                .ok();
        })
    }
}

/// Sends an envelope to the upstream or Kafka.
#[derive(Debug)]
pub struct SubmitEnvelope {
    pub envelope: ManagedEnvelope,
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
    /// Transaction extraction mode.
    pub extraction_mode: ExtractionMode,
}

/// Dispatch service for generating and submitting Envelopes.
#[derive(Debug)]
pub enum EnvelopeManager {
    SubmitEnvelope(Box<SubmitEnvelope>),
    SendClientReports(SendClientReports),
    SendMetrics(SendMetrics),
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
    enveloper_processor: Addr<EnvelopeProcessor>,
    project_cache: Addr<ProjectCache>,
    test_store: Addr<TestStore>,
    upstream_relay: Addr<UpstreamRelay>,
    #[cfg(feature = "processing")]
    store_forwarder: Option<Addr<Store>>,
}

impl EnvelopeManagerService {
    /// Creates a new instance of the [`EnvelopeManager`] service.
    pub fn new(
        config: Arc<Config>,
        enveloper_processor: Addr<EnvelopeProcessor>,
        project_cache: Addr<ProjectCache>,
        test_store: Addr<TestStore>,
        upstream_relay: Addr<UpstreamRelay>,
    ) -> Self {
        Self {
            config,
            enveloper_processor,
            project_cache,
            test_store,
            upstream_relay,
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
        mut envelope: Box<Envelope>,
        scoping: Scoping,
        partition_key: Option<u64>,
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

                return Ok(future.await??);
            }
        }

        // if we are in capture mode, we stash away the event instead of forwarding it.
        if Capture::should_capture(&self.config) {
            self.test_store.send(Capture::accepted(envelope));
            return Ok(());
        }

        relay_log::trace!("sending envelope to sentry endpoint");

        // Override the `sent_at` timestamp. Since the envelope went through basic
        // normalization, all timestamps have been corrected. We propagate the new
        // `sent_at` to allow the next Relay to double-check this timestamp and
        // potentially apply correction again. This is done as close to sending as
        // possible so that we avoid internal delays.
        envelope.set_sent_at(Utc::now());

        let envelope_body = envelope.to_vec()?;

        let (tx, rx) = oneshot::channel();
        let request = SendEnvelope {
            envelope_body,
            envelope_meta: envelope.meta().clone(),
            project_cache: self.project_cache.clone(),
            scoping,
            http_encoding: self.config.http_encoding(),
            response_sender: tx,
            project_key: scoping.project_key,
            partition_key: partition_key.map(|k| k.to_string()),
        };

        if let HttpEncoding::Identity = request.http_encoding {
            self.upstream_relay.send(SendRequest(request));
        } else {
            self.enveloper_processor.send(EncodeEnvelope::new(request));
        }

        match rx.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(_canceled) => Err(UpstreamRequestError::ChannelClosed.into()),
        }
    }

    async fn handle_submit(&self, message: SubmitEnvelope) {
        let SubmitEnvelope { mut envelope } = message;

        let scoping = envelope.scoping();
        let partition_key = envelope.partition_key();

        let inner_envelope = envelope.take_envelope();
        match self
            .submit_envelope(inner_envelope, scoping, partition_key)
            .await
        {
            Ok(_) => {
                envelope.accept();
            }
            Err(SendEnvelopeError::UpstreamRequestFailed(e)) if e.is_received() => {
                envelope.accept();
            }
            Err(error) => {
                // Errors are only logged for what we consider an internal discard reason. These
                // indicate errors in the infrastructure or implementation bugs.
                relay_log::with_scope(
                    |scope| scope.set_tag("project_key", scoping.project_key),
                    || {
                        relay_log::error!(
                            error = &error as &dyn Error,
                            tags.project_key = %scoping.project_key,
                            "error sending envelope"
                        )
                    },
                );
                envelope.reject(Outcome::Invalid(DiscardReason::Internal));
            }
        }
    }

    async fn handle_send_metrics(&self, message: SendMetrics) {
        let SendMetrics {
            buckets,
            scoping,
            extraction_mode,
        } = message;

        #[allow(unused_mut)]
        let mut partitions = self.config.metrics_partitions();
        #[allow(unused_mut)]
        let mut max_batch_size_bytes = self.config.metrics_max_batch_size_bytes();

        #[cfg(feature = "processing")]
        if self.store_forwarder.is_some() {
            // Partitioning on processing relays does not make sense, they end up all
            // in the same Kafka topic anyways and the partition key is ignored.
            partitions = None;
            max_batch_size_bytes = self.config.metrics_max_batch_size_bytes_processing();
        }

        self.enveloper_processor.send(EncodeMetrics {
            buckets,
            scoping,
            extraction_mode,
            max_batch_size_bytes,
            partitions,
        });
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
            relay_log::trace!(
                error = &e as &dyn Error,
                "failed to send envelope for client report"
            );
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
