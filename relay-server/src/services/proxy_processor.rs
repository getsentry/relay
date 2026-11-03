use std::error::Error;
use std::sync::Arc;

use relay_config::Config;
use relay_statsd::metric;
use relay_system::{Addr, Service};

use crate::envelope::{ContentType, Envelope, EnvelopeError, Item, ItemType};
use crate::extractors::{PartialDsn, RequestMeta};
use crate::managed::ManagedEnvelope;
use crate::services::outcome::{DiscardReason, Outcome};
use crate::services::processor::{
    EnvelopeProcessor, ProcessEnvelope, SendEnvelope, SubmitClientReports, encode_payload,
};

use crate::services::projects::cache::ProjectCacheHandle;
use crate::services::upstream::{SendRequest, UpstreamRelay};
use crate::statsd::RelayTimers;

/// Service implementing the [`EnvelopeProcessor`] interface.
///
/// Like the [envelope processor](crate::services::processor::envelope),
/// this service handles messages when Relay runs in proxy mode.
pub struct ProxyProcessorService {
    config: Arc<Config>,
    project_cache: ProjectCacheHandle,
    upstream_relay: Addr<UpstreamRelay>,
}

impl ProxyProcessorService {
    /// Creates a proxy processor.
    pub fn new(
        config: Arc<Config>,
        project_cache: ProjectCacheHandle,
        upstream_relay: Addr<UpstreamRelay>,
    ) -> Self {
        Self {
            config,
            project_cache,
            upstream_relay,
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
            outcome_aggregator,
        } = message;

        let config = self.config.current();
        let upstream = config.upstream();
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

        let envelope = ManagedEnvelope::new(envelope, outcome_aggregator);
        self.submit_upstream(envelope);
    }

    fn submit_upstream(&self, mut envelope: ManagedEnvelope) {
        if envelope.envelope_mut().is_empty() {
            envelope.accept();
            return;
        }

        relay_log::trace!("sending envelope to sentry endpoint");
        let config = self.config.current();
        let http_encoding = config.http_encoding();
        let result = envelope.envelope().to_vec().and_then(|v| {
            encode_payload(&v.into(), http_encoding).map_err(EnvelopeError::PayloadIoFailed)
        });

        match result {
            Ok(body) => {
                self.upstream_relay.send(SendRequest(SendEnvelope {
                    upstream: None,
                    envelope,
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
                | EnvelopeProcessor::ProcessProjectMetrics(_) => {
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
