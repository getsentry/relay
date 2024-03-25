//! This module contains the outcomes aggregator, which collects similar outcomes into groups
//! and flushed them periodically.

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use relay_common::time::UnixTimestamp;
use relay_config::{Config, EmitOutcomes};
use relay_quotas::{DataCategory, Scoping};
use relay_statsd::metric;
use relay_system::{Addr, Controller, Service, Shutdown};

use crate::services::outcome::{Outcome, OutcomeProducer, TrackOutcome};
use crate::statsd::RelayTimers;
use crate::utils::SleepHandle;

/// Contains everything to construct a `TrackOutcome`, except quantity
#[derive(Debug, PartialEq, Eq, Hash)]
struct BucketKey {
    /// The time slot for which outcomes are aggregated. timestamp = offset * bucket_interval
    offset: u64,
    /// Scoping of the request.
    pub scoping: Scoping,
    /// The outcome.
    pub outcome: Outcome,
    /// The event's data category.
    pub category: DataCategory,
}

/// Aggregates [`Outcome`]s into buckets and flushes them periodically.
///
/// This service handles a single message [`TrackOutcome`].
pub struct OutcomeAggregator {
    /// Whether or not to produce outcomes.
    ///
    /// If `true`, all outcomes will be dropped/
    disabled: bool,
    /// The width of each aggregated bucket in seconds
    bucket_interval: u64,
    /// The number of seconds between flushes of all buckets
    flush_interval: u64,
    /// Mapping from bucket key to quantity.
    buckets: HashMap<BucketKey, u32>,
    /// The recipient of the aggregated outcomes
    outcome_producer: Addr<OutcomeProducer>,
    /// An optional timeout to the next scheduled flush.
    flush_handle: SleepHandle,
}

impl OutcomeAggregator {
    pub fn new(config: &Config, outcome_producer: Addr<OutcomeProducer>) -> Self {
        let disabled = matches!(config.emit_outcomes(), EmitOutcomes::None);

        Self {
            disabled,
            bucket_interval: config.outcome_aggregator().bucket_interval,
            flush_interval: config.outcome_aggregator().flush_interval,
            buckets: HashMap::new(),
            outcome_producer,
            flush_handle: SleepHandle::idle(),
        }
    }

    fn handle_shutdown(&mut self, message: Shutdown) {
        if message.timeout.is_some() {
            self.flush();
            relay_log::info!("outcome aggregator stopped");
        }
    }

    fn handle_track_outcome(&mut self, msg: TrackOutcome) {
        if self.disabled {
            return;
        }

        let offset = msg.timestamp.timestamp() as u64 / self.bucket_interval;

        let bucket_key = BucketKey {
            offset,
            scoping: msg.scoping,
            outcome: msg.outcome,
            category: msg.category,
        };

        let counter = self.buckets.entry(bucket_key).or_insert(0);
        *counter += msg.quantity;

        if self.flush_interval == 0 {
            // Flush immediately. This is useful for integration tests.
            self.do_flush();
        } else if self.flush_handle.is_idle() {
            self.flush_handle
                .set(Duration::from_secs(self.flush_interval));
        }
    }

    fn do_flush(&mut self) {
        self.flush_handle.reset();

        let bucket_interval = self.bucket_interval;
        let outcome_producer = self.outcome_producer.clone();

        for (bucket_key, quantity) in self.buckets.drain() {
            let BucketKey {
                offset,
                scoping,
                outcome,
                category,
            } = bucket_key;

            // In case the timestamp cannot be extracted, we fallback to the current UTC `DateTime`
            let timestamp = UnixTimestamp::from_secs(offset * bucket_interval)
                .as_datetime()
                .unwrap_or_else(Utc::now);

            let outcome = TrackOutcome {
                timestamp,
                scoping,
                outcome,
                event_id: None,
                remote_addr: None,
                category,
                quantity,
            };

            outcome_producer.send(outcome);
        }
    }

    fn flush(&mut self) {
        metric!(timer(RelayTimers::OutcomeAggregatorFlushTime), {
            self.do_flush();
        });
    }
}

impl Service for OutcomeAggregator {
    type Interface = TrackOutcome;

    fn spawn_handler(mut self, mut rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            let mut shutdown = Controller::shutdown_handle();
            relay_log::info!("outcome aggregator started");

            loop {
                tokio::select! {
                    // Prioritize flush over receiving messages to prevent starving. Shutdown can be
                    // last since it is not vital if there are still messages in the channel.
                    biased;

                    () = &mut self.flush_handle => self.flush(),
                    Some(message) = rx.recv() => self.handle_track_outcome(message),
                    shutdown = shutdown.notified() => self.handle_shutdown(shutdown),
                    else => break,
                }
            }

            relay_log::info!("outcome aggregator stopped");
        });
    }
}
