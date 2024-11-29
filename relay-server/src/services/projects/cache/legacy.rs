use std::collections::BTreeMap;
use std::sync::Arc;

use crate::services::buffer::{
    EnvelopeBuffer, EnvelopeBufferError, PartitionedEnvelopeBuffer, ProjectKeyPair,
};
use crate::services::global_config;
use crate::services::processor::{
    EncodeMetrics, EnvelopeProcessor, ProcessEnvelope, ProcessingGroup, ProjectMetrics,
};
use crate::services::projects::cache::{CheckedEnvelope, ProjectCacheHandle};
use crate::Envelope;
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, Service};
use tokio::sync::{mpsc, watch};

use crate::services::metrics::{Aggregator, FlushBuckets, MergeBuckets};
use crate::services::outcome::{DiscardReason, Outcome, TrackOutcome};
use crate::services::projects::project::ProjectState;
use crate::services::test_store::TestStore;

use crate::statsd::{RelayCounters, RelayTimers};
use crate::utils::ManagedEnvelope;

/// Handle an envelope that was popped from the envelope buffer.
#[derive(Debug)]
pub struct DequeuedEnvelope(pub Box<Envelope>);

/// The legacy project cache.
///
/// It manages spool v1 and some remaining messages which handle project state.
#[derive(Debug)]
pub enum ProjectCache {
    FlushBuckets(FlushBuckets),
}

impl ProjectCache {
    pub fn variant(&self) -> &'static str {
        match self {
            Self::FlushBuckets(_) => "FlushBuckets",
        }
    }
}

impl Interface for ProjectCache {}

impl FromMessage<FlushBuckets> for ProjectCache {
    type Response = relay_system::NoResponse;

    fn from_message(message: FlushBuckets, _: ()) -> Self {
        Self::FlushBuckets(message)
    }
}

/// Holds the addresses of all services required for [`ProjectCache`].
#[derive(Debug, Clone)]
pub struct Services {
    pub envelope_buffer: PartitionedEnvelopeBuffer,
    pub aggregator: Addr<Aggregator>,
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

    /// Status of the global configuration, used to determine readiness for processing.
    global_config: GlobalConfigStatus,
}

/// Describes the current status of the `GlobalConfig`.
///
/// Either it's ready to be used, or it contains the list of in-flight project keys,
/// to be processed once the config arrives.
#[derive(Debug)]
enum GlobalConfigStatus {
    /// Global config needed for envelope processing.
    Ready,
    /// The global config is not fetched yet.
    Pending,
}

impl ProjectCacheBroker {
    fn set_global_config_ready(&mut self) {
        self.global_config = GlobalConfigStatus::Ready;
    }

    fn handle_flush_buckets(&mut self, message: FlushBuckets) {
        let aggregator = self.services.aggregator.clone();

        let mut no_project = 0;
        let mut scoped_buckets = BTreeMap::new();
        for (project_key, buckets) in message.buckets {
            let project = self.projects.get(project_key);

            let project_info = match project.state() {
                ProjectState::Pending => {
                    no_project += 1;

                    // Return the buckets to the aggregator.
                    aggregator.send(MergeBuckets::new(project_key, buckets));
                    continue;
                }
                ProjectState::Disabled => {
                    // Project loaded and disabled, discard the buckets.
                    //
                    // Ideally we log outcomes for the metrics here, but currently for metric
                    // outcomes we need a valid scoping, which we cannot construct for disabled
                    // projects.
                    continue;
                }
                ProjectState::Enabled(project_info) => project_info,
            };

            let Some(scoping) = project_info.scoping(project_key) else {
                relay_log::error!(
                    tags.project_key = project_key.as_str(),
                    "there is no scoping: dropping {} buckets",
                    buckets.len(),
                );
                continue;
            };

            use std::collections::btree_map::Entry::*;
            match scoped_buckets.entry(scoping) {
                Vacant(entry) => {
                    entry.insert(ProjectMetrics {
                        project_info: Arc::clone(project_info),
                        rate_limits: project.rate_limits().current_limits(),
                        buckets,
                    });
                }
                Occupied(entry) => {
                    entry.into_mut().buckets.extend(buckets);
                }
            }
        }

        self.services.envelope_processor.send(EncodeMetrics {
            partition_key: message.partition_key,
            scopes: scoped_buckets,
        });

        relay_statsd::metric!(
            counter(RelayCounters::ProjectStateFlushMetricsNoProject) += no_project
        );
    }

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

    fn handle_message(&mut self, message: ProjectCache) {
        let ty = message.variant();
        metric!(
            timer(RelayTimers::LegacyProjectCacheMessageDuration),
            message = ty,
            {
                match message {
                    ProjectCache::FlushBuckets(message) => self.handle_flush_buckets(message),
                }
            }
        )
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
    global_config_rx: watch::Receiver<global_config::Status>,
    /// Bounded channel used exclusively to receive envelopes from the envelope buffer.
    envelopes_rx: mpsc::Receiver<DequeuedEnvelope>,
}

impl ProjectCacheService {
    /// Creates a new `ProjectCacheService`.
    pub fn new(
        project_cache_handle: ProjectCacheHandle,
        services: Services,
        global_config_rx: watch::Receiver<global_config::Status>,
        envelopes_rx: mpsc::Receiver<DequeuedEnvelope>,
    ) -> Self {
        Self {
            project_cache_handle,
            services,
            global_config_rx,
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
            mut global_config_rx,
            mut envelopes_rx,
        } = self;
        relay_log::info!("project cache started");

        let global_config = match global_config_rx.borrow().clone() {
            global_config::Status::Ready(_) => {
                relay_log::info!("global config received");
                GlobalConfigStatus::Ready
            }
            global_config::Status::Pending => {
                relay_log::info!("waiting for global config");
                GlobalConfigStatus::Pending
            }
        };

        let mut broker = ProjectCacheBroker {
            projects: project_cache_handle,
            services,
            global_config,
        };

        loop {
            tokio::select! {
                biased;

                Ok(()) = global_config_rx.changed() => {
                    metric!(timer(RelayTimers::LegacyProjectCacheTaskDuration), task = "update_global_config", {
                        match global_config_rx.borrow().clone() {
                            global_config::Status::Ready(_) => broker.set_global_config_ready(),
                            // The watch should only be updated if it gets a new value.
                            // This would imply a logical bug.
                            global_config::Status::Pending => relay_log::error!("still waiting for the global config"),
                        }
                    })
                },
                Some(message) = rx.recv() => {
                    metric!(timer(RelayTimers::LegacyProjectCacheTaskDuration), task = "handle_message", {
                        broker.handle_message(message)
                    })
                }
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
