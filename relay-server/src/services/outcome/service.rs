//! This module contains the service that tracks outcomes.
//!
//! Outcomes describe the final "fate" of an envelope item. As such, for every item exactly one
//! outcome must be emitted in the entire ingestion pipeline. Since Relay is only one part in this
//! pipeline, outcomes may not be emitted if the item is accepted.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use relay_config::Config;
use relay_event_schema::protocol::{ClientReport, DiscardedEvent, EventId};
use relay_metrics::{MetricNamespace, UnixTimestamp};
use relay_quotas::{DataCategory, Scoping};
use relay_statsd::metric;
use relay_system::{Addr, FromMessage, Interface, NoResponse, Service};

use crate::services::metrics::{Aggregator, MergeBuckets};
use crate::services::outcome::{self, DiscardReason, Outcome};
use crate::services::processor::{EnvelopeProcessor, SubmitClientReports};
use crate::statsd::RelayCounters;
use crate::utils::SleepHandle;

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

/// Service implementing the [`TrackOutcome`] interface, dropping all outcomes.
#[derive(Debug)]
pub struct NullOutcomeProducerService {
    _priv: (),
}

impl NullOutcomeProducerService {
    pub fn new() -> Self {
        Self { _priv: () }
    }
}

impl Service for NullOutcomeProducerService {
    type Interface = TrackOutcome;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        while rx.recv().await.is_some() {}
    }
}

/// Service implementing the [`TrackOutcome`] interface.
#[derive(Debug)]
pub struct OutcomeProducerService {
    config: Arc<Config>,
    aggregator: Addr<Aggregator>,
}

impl OutcomeProducerService {
    pub fn new(config: Arc<Config>, aggregator: Addr<Aggregator>) -> Self {
        Self { config, aggregator }
    }

    fn handle_message(&self, message: TrackOutcome) {
        send_outcome_metric(&message);
        self.aggregator.send(MergeBuckets {
            project_key: message.scoping.project_key,
            buckets: vec![outcome::metric::to_metric(&message, &self.config)],
        })
    }
}

impl Service for OutcomeProducerService {
    type Interface = TrackOutcome;

    async fn run(self, mut rx: relay_system::Receiver<Self::Interface>) {
        relay_log::info!("outcome producer started.");
        while let Some(message) = rx.recv().await {
            self.handle_message(message);
        }
        relay_log::info!("outcome producer stopped.");
    }
}

/// Service implementing the [`TrackOutcome`] interface only emitting [`ClientReport`]s.
///
/// This implementation is only useful for Relay instances running in Proxy mode.
/// It is much less optimized and the implementation assumes behaviour of a Proxy mode Relay,
/// like a relatively low volume of data and only a single scoping. It will behave correctly
/// event outside these parameters, but may not perform optimally.
#[derive(Debug)]
pub struct ClientReportOutcomeProducerService {
    processor: Addr<EnvelopeProcessor>,
    buckets: BTreeMap<BucketKey, ClientReport>,
    bucket_interval: u32,
    flush_interval: u32,
    flush_handle: SleepHandle,
}

impl ClientReportOutcomeProducerService {
    pub fn new(config: &Config, processor: Addr<EnvelopeProcessor>) -> Self {
        let agg = &config
            .aggregator_config_for(MetricNamespace::Outcomes)
            .aggregator;

        Self {
            processor,
            buckets: Default::default(),
            bucket_interval: agg.bucket_interval.max(1),
            flush_interval: agg.initial_delay,
            flush_handle: SleepHandle::idle(),
        }
    }

    fn handle_message(&mut self, message: TrackOutcome) {
        send_outcome_metric(&message);

        let offset = u64::try_from(message.timestamp.timestamp()).unwrap_or(0)
            / u64::from(self.bucket_interval);

        let bucket_key = BucketKey {
            offset,
            scoping: message.scoping,
        };

        let discarded_events: fn(&mut ClientReport) -> &mut _ = match message.outcome {
            Outcome::Filtered(_) => |cr| &mut cr.filtered_events,
            Outcome::FilteredSampling(_) => |cr| &mut cr.filtered_sampling_events,
            Outcome::RateLimited(_) => |cr| &mut cr.rate_limited_events,
            Outcome::Invalid(DiscardReason::InvalidSignature | DiscardReason::MissingSignature) => {
                |cr| &mut cr.discarded_events
            }
            _ => {
                relay_log::debug!(
                    "Outcome '{}' cannot be converted to client report",
                    message.outcome
                );
                return;
            }
        };

        let client_report = self.buckets.entry(bucket_key).or_default();
        let discarded_events = discarded_events(client_report);

        let reason = message.outcome.to_reason().unwrap_or_default();
        let category = message.category;

        // Linear search is fine, we only have a limited amount of outcomes and volume as this
        // service is only supposed to be used for proxy mode Relay.
        let discarded_event = discarded_events
            .iter_mut()
            .find(|de| de.reason == reason && de.category == category);

        match discarded_event {
            Some(discarded_event) => discarded_event.quantity += message.quantity,
            None => discarded_events.push(DiscardedEvent {
                reason: reason.into_owned(),
                category,
                quantity: message.quantity,
            }),
        }

        match self.flush_interval {
            0 => self.do_flush(),
            v => self.flush_handle.set_if_idle(Duration::from_secs(v.into())),
        }
    }

    fn do_flush(&mut self) {
        for (bucket_key, client_report) in std::mem::take(&mut self.buckets) {
            let BucketKey { offset, scoping } = bucket_key;

            let timestamp = Some(offset)
                // May be zero as we default timestamps out of range to 0.
                .filter(|offset| *offset > 0)
                .map(|offset| offset * u64::from(self.bucket_interval))
                .map(UnixTimestamp::from_secs);

            let client_report = ClientReport {
                timestamp,
                ..client_report
            };

            self.processor.send(SubmitClientReports {
                client_reports: vec![client_report],
                scoping,
            });
        }
    }

    fn handle_shutdown(&mut self) {
        self.flush_interval = 0;
        self.do_flush();
    }
}

impl Service for ClientReportOutcomeProducerService {
    type Interface = TrackOutcome;

    async fn run(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        let mut shutdown = relay_system::Controller::shutdown_handle();
        relay_log::info!("client report outcome producer started");

        loop {
            tokio::select! {
                biased;

                () = &mut self.flush_handle => self.do_flush(),
                Some(message) = rx.recv() => self.handle_message(message),
                _ = shutdown.notified() => self.handle_shutdown(),

                else => break,
            }
        }
        self.do_flush();
        relay_log::info!("client report outcome producer stopped");
    }
}

/// Contains everything to aggregate a [`ClientReport`].
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BucketKey {
    /// The time slot for which outcomes are aggregated.
    ///
    /// The offset follows the formula: `timestamp = offset * bucket_interval`.
    offset: u64,
    /// Scoping of the outcome.
    scoping: Scoping,
}

fn send_outcome_metric(message: &TrackOutcome) {
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
    );
    metric!(
        counter(RelayCounters::Outcomes) += 1,
        reason = message.outcome.to_reason().unwrap_or_default(),
        outcome = outcome_name,
    );
}
