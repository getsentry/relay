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
    EnvelopeProcessor, ProcessEnvelope, ProcessingGroup, SendEnvelope, SubmitClientReports,
    encode_payload,
};

use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::upstream::{SendRequest, UpstreamRelay};
use crate::statsd::RelayTimers;

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

        let scoping = message.envelope.scoping();
        for (_, envelope) in ProcessingGroup::split_envelope(
            *message.envelope.into_envelope(),
            &message.project_info,
        ) {
            let mut envelope =
                ManagedEnvelope::new(envelope, self.addrs.outcome_aggregator.clone());
            envelope.scope(scoping);
            self.submit_upstream(envelope);
        }
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
            let mut item = Item::new(ItemType::ClientReport);
            item.set_payload(ContentType::Json, client_report.serialize().unwrap()); // TODO: unwrap OK?
            envelope.add_item(item);
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
