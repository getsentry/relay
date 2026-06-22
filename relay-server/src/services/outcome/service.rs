//! This module contains the service that tracks outcomes.
//!
//! Outcomes describe the final "fate" of an envelope item. As such, for every item exactly one
//! outcome must be emitted in the entire ingestion pipeline. Since Relay is only one part in this
//! pipeline, outcomes may not be emitted if the item is accepted.

use std::collections::BTreeMap;
use std::mem;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::services::metrics::{Aggregator, MergeBuckets};
use crate::services::outcome::{self, DiscardReason, Outcome};
use crate::services::processor::{EnvelopeProcessor, SubmitClientReports};
use crate::statsd::RelayCounters;
use crate::utils::SleepHandle;
use chrono::{DateTime, Utc};
use relay_common::time::UnixTimestamp;
use relay_config::{Config, EmitOutcomes};
use relay_event_schema::protocol::{ClientReport, DiscardedEvent, EventId};
use relay_quotas::{DataCategory, Scoping};
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};

/// Tracks an [`Outcome`] of an Envelope item.
///
/// See the module level documentation for more information.
#[derive(Clone, Debug, Hash)]
pub struct TrackOutcome {
    /// The timespan of the event outcome.
    pub timestamp: DateTime<Utc>,
    /// Scoping of the request.
    pub scoping: Scoping,
    /// The outcome.
    pub outcome: Outcome,
    /// The event id.
    pub event_id: Option<EventId>,
    /// The client ip address.
    pub remote_addr: Option<IpAddr>,
    /// The event's data category.
    pub category: DataCategory,
    /// The number of events or total attachment size in bytes.
    pub quantity: u32,
}

impl Interface for TrackOutcome {}

impl FromMessage<Self> for TrackOutcome {
    type Response = NoResponse;

    fn from_message(message: Self, _: ()) -> Self {
        message
    }
}

/// Outcome producer backend via HTTP as [`ClientReport`].
#[derive(Debug)]
struct ClientReportOutcomeProducer {
    flush_interval: Duration,
    unsent_reports: BTreeMap<Scoping, Vec<ClientReport>>,
    flush_handle: SleepHandle,
    envelope_processor: Addr<EnvelopeProcessor>,
}

impl ClientReportOutcomeProducer {
    fn new(config: &Config, envelope_processor: Addr<EnvelopeProcessor>) -> Self {
        Self {
            // Use same batch interval as outcome aggregator
            flush_interval: Duration::from_secs(config.outcome_aggregator().flush_interval),
            unsent_reports: BTreeMap::new(),
            flush_handle: SleepHandle::idle(),
            envelope_processor,
        }
    }

    fn flush(&mut self) {
        relay_log::trace!("flushing client reports");
        self.flush_handle.reset();

        let unsent_reports = mem::take(&mut self.unsent_reports);
        for (scoping, client_reports) in unsent_reports.into_iter() {
            self.envelope_processor.send(SubmitClientReports {
                client_reports,
                scoping,
            });
        }
    }

    fn handle_message(&mut self, msg: TrackOutcome) {
        let mut client_report = ClientReport {
            timestamp: Some(UnixTimestamp::from_secs(
                msg.timestamp.timestamp().try_into().unwrap_or(0),
            )),
            ..Default::default()
        };

        // The outcome type determines what field to place the outcome in:
        let discarded_events = match msg.outcome {
            Outcome::Filtered(_) => &mut client_report.filtered_events,
            Outcome::FilteredSampling(_) => &mut client_report.filtered_sampling_events,
            Outcome::RateLimited(_) => &mut client_report.rate_limited_events,
            Outcome::Invalid(DiscardReason::InvalidSignature | DiscardReason::MissingSignature) => {
                &mut client_report.discarded_events
            }
            _ => {
                relay_log::debug!(
                    "Outcome '{}' cannot be converted to client report",
                    msg.outcome
                );
                return;
            }
        };

        // Now that we know where to put it, let's create a DiscardedEvent
        let discarded_event = DiscardedEvent {
            reason: msg.outcome.to_reason().unwrap_or_default().to_string(),
            category: msg.category,
            quantity: msg.quantity,
        };
        discarded_events.push(discarded_event);

        self.unsent_reports
            .entry(msg.scoping)
            .or_default()
            .push(client_report);

        if self.flush_interval == Duration::ZERO {
            // Flush immediately. Useful for integration tests.
            self.flush();
        } else if self.flush_handle.is_idle() {
            self.flush_handle.set(self.flush_interval);
        }
    }
}

impl Service for ClientReportOutcomeProducer {
    type Interface = TrackOutcome;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        loop {
            tokio::select! {
                // Prioritize flush over receiving messages to prevent starving.
                biased;

                () = &mut self.flush_handle => self.flush(),
                Some(message) = rx.recv() => self.handle_message(message),
                else => break,
            }
        }
    }
}

/// Produces [`Outcome`]s to a configurable backend.
///
/// The backend is configured through the `outcomes` configuration object and can be:
///
///  1. Metrics
///  2. Upstream Relay via client reports in external configuration
///  3. (default) Disabled
#[derive(Debug)]
pub struct OutcomeProducer(TrackOutcome);

impl Interface for OutcomeProducer {}

impl FromMessage<TrackOutcome> for OutcomeProducer {
    type Response = NoResponse;

    fn from_message(message: TrackOutcome, _: ()) -> Self {
        Self(message)
    }
}

fn send_outcome_metric(message: &TrackOutcome, to: &'static str) {
    let outcome_name = match message.outcome {
        Outcome::Accepted => "accepted",
        Outcome::Filtered(_) | Outcome::FilteredSampling(_) => "filtered",
        Outcome::RateLimited(_) => "rate_limited",
        Outcome::Invalid(_) => "invalid",
        Outcome::Abuse => "abuse",
        Outcome::ClientDiscard(_) => "client_discard",
    };

    metric!(
        counter(RelayCounters::OutcomeQuantity) += message.quantity.into(),
        category = message.category.name(),
        outcome = outcome_name,
        to = to,
    );
    metric!(
        counter(RelayCounters::Outcomes) += 1,
        reason = message.outcome.to_reason().unwrap_or_default(),
        outcome = outcome_name,
        to = to,
    );
}

#[derive(Debug)]
enum OutcomeBroker {
    ClientReport(Addr<TrackOutcome>),
    Metric(Addr<Aggregator>),
    Disabled,
}

impl OutcomeBroker {
    fn handle_message(&self, message: OutcomeProducer, config: &Config) {
        relay_log::with_scope(|_| {}, || self.handle_track_outcome(message.0, config))
    }

    fn handle_track_outcome(&self, message: TrackOutcome, config: &Config) {
        match self {
            Self::Metric(metrics) => {
                send_outcome_metric(&message, "metric");
                metrics.send(MergeBuckets {
                    project_key: message.scoping.project_key,
                    buckets: vec![outcome::metric::to_metric(&message, config)],
                })
            }
            Self::ClientReport(producer) => {
                send_outcome_metric(&message, "client_report");
                producer.send(message);
            }
            Self::Disabled => (),
        }
    }
}

#[derive(Debug)]
enum ProducerInner {
    Metric(Addr<Aggregator>),
    ClientReport(ClientReportOutcomeProducer),
    Disabled,
}

impl ProducerInner {
    fn start(self) -> OutcomeBroker {
        match self {
            ProducerInner::Metric(inner) => OutcomeBroker::Metric(inner),
            ProducerInner::ClientReport(inner) => {
                OutcomeBroker::ClientReport(inner.start_detached())
            }
            ProducerInner::Disabled => OutcomeBroker::Disabled,
        }
    }
}

/// Service implementing the [`OutcomeProducer`] interface.
#[derive(Debug)]
pub struct OutcomeProducerService {
    config: Arc<Config>,
    inner: ProducerInner,
}

impl OutcomeProducerService {
    pub fn create(
        config: Arc<Config>,
        envelope_processor: Addr<EnvelopeProcessor>,
        metric_aggregator: Addr<Aggregator>,
    ) -> anyhow::Result<Self> {
        let inner = match config.emit_outcomes() {
            EmitOutcomes::AsOutcomes => {
                relay_log::info!("Configured to emit outcomes via metrics");
                ProducerInner::Metric(metric_aggregator)
            }
            EmitOutcomes::AsClientReports => {
                // We emit client reports, and we do NOT accept raw outcomes
                relay_log::info!("Configured to emit outcomes as client reports");
                ProducerInner::ClientReport(ClientReportOutcomeProducer::new(
                    &config,
                    envelope_processor,
                ))
            }
            EmitOutcomes::None => {
                relay_log::info!("Configured to drop all outcomes");
                ProducerInner::Disabled
            }
        };

        Ok(Self { config, inner })
    }
}

impl Service for OutcomeProducerService {
    type Interface = OutcomeProducer;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        let Self { config, inner } = self;

        let broker = inner.start();

        relay_log::info!("OutcomeProducer started.");
        while let Some(message) = rx.recv().await {
            broker.handle_message(message, &config);
        }
        relay_log::info!("OutcomeProducer stopped.");
    }
}
