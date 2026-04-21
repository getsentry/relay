use std::error::Error;
use std::sync::Arc;

use relay_config::Config;
use relay_statsd::metric;
use relay_system::{Addr, Service};

use crate::envelope::{ContentType, Envelope, EnvelopeError, Item, ItemType};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::managed::ManagedEnvelope;
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::processor::{
    EnvelopeProcessor, ProcessEnvelope, SendEnvelope, SubmitClientReports, encode_payload,
};

use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::upstream::{SendRequest, UpstreamRelay};
use crate::statsd::RelayTimers;

#[cfg(feature = "fanout-http")]
use crate::services::fanout_http::{FanoutEnvelope, FanoutHttpHandle};

/// Service implementing the [`EnvelopeProcessor`] interface.
///
/// Analog to [`crate::services::processor::EnvelopeProcessorService`] this service handles messages when Relay is run in
/// proxy mode.
pub struct ProxyProcessorService {
    config: Arc<Config>,
    project_cache: ProjectCacheHandle,
    addrs: ProxyAddrs,
}

/// Contains the addresses of services that the proxy-processor publishes to.
pub struct ProxyAddrs {
    /// Address of the service used for tracking outcomes.
    pub outcome_aggregator: Addr<TrackOutcome>,
    /// Address of the service used for forwarding envelopes to the upstream.
    pub upstream_relay: Addr<UpstreamRelay>,
    /// Optional handle for the fire-and-forget HTTP fanout tee.
    ///
    /// `None` when the `fanout-http` feature is compiled in but disabled in config, or when
    /// the tee is being constructed and falls back. The tee never affects the primary
    /// upstream forward.
    #[cfg(feature = "fanout-http")]
    pub fanout_http: Option<FanoutHttpHandle>,
}

impl ProxyProcessorService {
    /// Creates a proxy processor.
    pub fn new(config: Arc<Config>, project_cache: ProjectCacheHandle, addrs: ProxyAddrs) -> Self {
        Self {
            project_cache,
            addrs,
            config,
        }
    }

    fn handle_process_envelope(&self, message: ProcessEnvelope) {
        let wait_time = message.envelope.age();
        metric!(timer(RelayTimers::EnvelopeWaitTime) = wait_time);
        self.submit_upstream(message.envelope);
    }

    fn handle_submit_client_reports(&self, message: SubmitClientReports) {
        let SubmitClientReports {
            client_reports,
            scoping,
        } = message;

        let upstream = self.config.upstream_descriptor();
        let dsn = PartialDsn::outbound(&scoping, upstream);

        let mut envelope = Envelope::from_request(None, RequestMeta::outbound(dsn));
        for client_report in client_reports {
            match client_report.serialize() {
                Ok(payload) => {
                    let mut item = Item::new(ItemType::ClientReport);
                    item.set_payload(ContentType::Json, payload);
                    envelope.add_item(item);
                }
                Err(error) => {
                    relay_log::error!(
                        error = &error as &dyn std::error::Error,
                        "failed to serialize client report"
                    );
                }
            }
        }

        let envelope = ManagedEnvelope::new(envelope, self.addrs.outcome_aggregator.clone());
        self.submit_upstream(envelope);
    }

    fn submit_upstream(&self, mut envelope: ManagedEnvelope) {
        if envelope.envelope_mut().is_empty() {
            envelope.accept();
            return;
        }

        relay_log::trace!("sending envelope to sentry endpoint");
        let http_encoding = self.config.http_encoding();
        let result = envelope.envelope().to_vec().and_then(|v| {
            encode_payload(&v.into(), http_encoding).map_err(EnvelopeError::PayloadIoFailed)
        });

        match result {
            Ok(body) => {
                // Fire-and-forget HTTP fanout tee. Runs strictly in parallel with the primary
                // forward; any failure is counted in statsd and never affects the upstream send.
                #[cfg(feature = "fanout-http")]
                if let Some(handle) = self.addrs.fanout_http.as_ref() {
                    let item_types: smallvec::SmallVec<[ItemType; 4]> =
                        envelope.envelope().items().map(|i| i.ty().clone()).collect();
                    if handle.should_send(body.len(), &item_types) {
                        handle.dispatch(FanoutEnvelope {
                            body: body.clone(),
                            content_encoding: http_encoding,
                            scoping: envelope.scoping(),
                            event_id: envelope.envelope().event_id(),
                            received_at: envelope.received_at(),
                            item_types,
                        });
                    }
                }

                self.addrs.upstream_relay.send(SendRequest(SendEnvelope {
                    envelope: envelope.into_processed(),
                    body,
                    http_encoding,
                    project_cache: self.project_cache.clone(),
                }));
            }
            Err(error) => {
                // Errors are only logged for what we consider an internal discard reason. These
                // indicate errors in the infrastructure or implementation bugs.
                relay_log::error!(
                    error = &error as &dyn Error,
                    tags.project_key = %envelope.scoping().project_key,
                    "failed to serialize envelope payload"
                );

                envelope.reject(Outcome::Invalid(DiscardReason::Internal));
            }
        }
    }

    fn handle_message(&self, message: EnvelopeProcessor) {
        let ty = message.variant();

        metric!(timer(RelayTimers::ProcessMessageDuration), message = ty, {
            match message {
                EnvelopeProcessor::ProcessEnvelope(m) => self.handle_process_envelope(*m),
                EnvelopeProcessor::SubmitClientReports(m) => self.handle_submit_client_reports(*m),
                EnvelopeProcessor::ProcessBatchedMetrics(_)
                | EnvelopeProcessor::ProcessProjectMetrics(_)
                | EnvelopeProcessor::FlushBuckets(_) => {
                    relay_log::error!("internal error: Metrics not supported in Proxy mode");
                }
            }
        });
    }
}

impl Service for ProxyProcessorService {
    type Interface = EnvelopeProcessor;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        while let Some(message) = rx.recv().await {
            self.handle_message(message);
        }
    }
}
