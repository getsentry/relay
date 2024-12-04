use crate::services::buffer::{
    EnvelopeBuffer, EnvelopeBufferError, PartitionedEnvelopeBuffer, ProjectKeyPair,
};
use crate::services::processor::{EnvelopeProcessor, ProcessEnvelope, ProcessingGroup};
use crate::services::projects::cache::{CheckedEnvelope, ProjectCacheHandle};
use crate::Envelope;
use relay_statsd::metric;
use relay_system::{Addr, Interface, Service};
use tokio::sync::mpsc;

use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::projects::project::ProjectState;
use crate::services::test_store::TestStore;

use crate::statsd::RelayTimers;
use crate::utils::ManagedEnvelope;

/// Handle an envelope that was popped from the envelope buffer.
#[derive(Debug)]
pub struct DequeuedEnvelope(pub Box<Envelope>);

/// The legacy project cache.
///
/// It currently just does some project routing from the spool to the processor,
/// which eventually will be moved into the spool and processor, making this service
/// obsolete.
#[derive(Debug)]
pub enum ProjectCache {}

impl ProjectCache {
    pub fn variant(&self) -> &'static str {
        match *self {}
    }
}

impl Interface for ProjectCache {}

/// Holds the addresses of all services required for [`ProjectCache`].
#[derive(Debug, Clone)]
pub struct Services {
    pub envelope_buffer: PartitionedEnvelopeBuffer,
    pub envelope_processor: Addr<EnvelopeProcessor>,
    pub outcome_aggregator: Addr<TrackOutcome>,
    pub test_store: Addr<TestStore>,
}

/// Main broker of the [`ProjectCacheService`].
///
/// This handles incoming public messages, merges resolved project states, and maintains the actual
/// cache of project states.
#[derive(Debug)]
struct ProjectCacheBroker {
    services: Services,
    projects: ProjectCacheHandle,
}

impl ProjectCacheBroker {
    fn handle_dequeued_envelope(
        &mut self,
        envelope: Box<Envelope>,
        envelope_buffer: Addr<EnvelopeBuffer>,
    ) -> Result<(), EnvelopeBufferError> {
        let sampling_key = envelope.sampling_key();
        let services = self.services.clone();

        let own_key = envelope.meta().public_key();
        let project = self.projects.get(own_key);

        // Check if project config is enabled.
        let project_info = match project.state() {
            ProjectState::Enabled(info) => info,
            ProjectState::Disabled => {
                let mut managed_envelope = ManagedEnvelope::new(
                    envelope,
                    self.services.outcome_aggregator.clone(),
                    self.services.test_store.clone(),
                    ProcessingGroup::Ungrouped,
                );
                managed_envelope.reject(Outcome::Invalid(DiscardReason::ProjectId));
                return Ok(());
            }
            ProjectState::Pending => {
                envelope_buffer.send(EnvelopeBuffer::NotReady(own_key, envelope));
                return Ok(());
            }
        };

        // Check if sampling config is enabled.
        let sampling_project_info = match sampling_key.map(|sampling_key| {
            (
                sampling_key,
                self.projects.get(sampling_key).state().clone(),
            )
        }) {
            Some((_, ProjectState::Enabled(info))) => {
                // Only set if it matches the organization ID. Otherwise treat as if there is
                // no sampling project.
                (info.organization_id == project_info.organization_id).then_some(info)
            }
            Some((_, ProjectState::Disabled)) => {
                // Accept envelope even if its sampling state is disabled:
                None
            }
            Some((sampling_key, ProjectState::Pending)) => {
                envelope_buffer.send(EnvelopeBuffer::NotReady(sampling_key, envelope));
                return Ok(());
            }
            None => None,
        };

        // Reassign processing groups and proceed to processing.
        for (group, envelope) in ProcessingGroup::split_envelope(*envelope) {
            let managed_envelope = ManagedEnvelope::new(
                envelope,
                services.outcome_aggregator.clone(),
                services.test_store.clone(),
                group,
            );

            let Ok(CheckedEnvelope {
                envelope: Some(managed_envelope),
                ..
            }) = project.check_envelope(managed_envelope)
            else {
                continue; // Outcomes are emitted by check_envelope
            };

            let reservoir_counters = project.reservoir_counters().clone();
            services.envelope_processor.send(ProcessEnvelope {
                envelope: managed_envelope,
                project_info: project_info.clone(),
                rate_limits: project.rate_limits().current_limits(),
                sampling_project_info: sampling_project_info.clone(),
                reservoir_counters,
            });
        }

        Ok(())
    }

    fn handle_envelope(&mut self, dequeued_envelope: DequeuedEnvelope) {
        let project_key_pair = ProjectKeyPair::from_envelope(&dequeued_envelope.0);
        let envelope_buffer = self
            .services
            .envelope_buffer
            .clone()
            .buffer(project_key_pair)
            .addr();

        if let Err(e) = self.handle_dequeued_envelope(dequeued_envelope.0, envelope_buffer) {
            relay_log::error!(
                error = &e as &dyn std::error::Error,
                "Failed to handle popped envelope"
            );
        }
    }
}

/// Service implementing the [`ProjectCache`] interface.
#[derive(Debug)]
pub struct ProjectCacheService {
    project_cache_handle: ProjectCacheHandle,
    services: Services,
    /// Bounded channel used exclusively to receive envelopes from the envelope buffer.
    envelopes_rx: mpsc::Receiver<DequeuedEnvelope>,
}

impl ProjectCacheService {
    /// Creates a new `ProjectCacheService`.
    pub fn new(
        project_cache_handle: ProjectCacheHandle,
        services: Services,
        envelopes_rx: mpsc::Receiver<DequeuedEnvelope>,
    ) -> Self {
        Self {
            project_cache_handle,
            services,
            envelopes_rx,
        }
    }
}

impl Service for ProjectCacheService {
    type Interface = ProjectCache;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let Self {
            project_cache_handle,
            services,
            mut envelopes_rx,
        } = self;
        relay_log::info!("project cache started");

        let mut broker = ProjectCacheBroker {
            projects: project_cache_handle,
            services,
        };

        loop {
            tokio::select! {
                biased;

                Some(message) = rx.recv() => { match message {} }
                Some(message) = envelopes_rx.recv() => {
                    metric!(timer(RelayTimers::LegacyProjectCacheTaskDuration), task = "handle_envelope", {
                        broker.handle_envelope(message)
                    })
                }
                else => break,
            }
        }

        relay_log::info!("project cache stopped");
    }
}
