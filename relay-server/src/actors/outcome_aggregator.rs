//! This module contains the outcomes aggregator, which collects similar outcomes into groups
//! and flushed them periodically.

use std::{collections::HashMap, net::IpAddr};

use actix::{Actor, AsyncContext, Context, Handler, Recipient, Supervised, SystemService};
use relay_common::{DataCategory, UnixTimestamp};
use relay_config::Config;
use relay_quotas::Scoping;
use relay_statsd::metric;
use std::time::Duration;

use crate::statsd::RelayTimers;

use super::outcome::{Outcome, OutcomeError, TrackOutcome};

/// Contains everything to construct a `TrackOutcome`, except quantity
#[derive(Debug, PartialEq, Eq, Hash)]
struct BucketKey {
    /// The time slot for which outcomes are aggregated. timestamp = offset * bucket_interval
    offset: u64,
    /// Scoping of the request.
    pub scoping: Scoping,
    /// The outcome.
    pub outcome: Outcome,
    /// The client ip address.
    pub remote_addr: Option<IpAddr>,
    /// The event's data category.
    pub category: DataCategory,
}

#[derive(PartialEq)]
enum AggregationMode {
    /// Aggregator drops all outcomes
    DropEverything,
    /// Aggregator keeps all outcome fields intact
    Lossless,
    /// Aggregator removes fields to improve aggregation
    Lossy,
}

/// Aggregates outcomes into buckets, and flushes them periodically.
/// Inspired by [`relay_metrics::Aggregator`].
pub struct OutcomeAggregator {
    mode: AggregationMode,
    /// The width of each aggregated bucket in seconds
    bucket_interval: u64,
    /// The number of seconds between flushes of all buckets
    flush_interval: u64,
    /// Mapping from bucket key to quantity.
    buckets: HashMap<BucketKey, u32>,
    /// The recipient of the aggregated outcomes
    outcome_producer: Recipient<TrackOutcome>,
}

impl OutcomeAggregator {
    pub fn new(config: &Config, outcome_producer: Recipient<TrackOutcome>) -> Self {
        let mode = if config.emit_outcomes_as_client_reports() {
            AggregationMode::Lossy
        } else if config.emit_outcomes() || config.processing_enabled() {
            AggregationMode::Lossless
        } else {
            // Outcomes are completely disabled, so no need to keep anything
            AggregationMode::DropEverything
        };

        Self {
            mode,
            bucket_interval: config.outcome_aggregator().bucket_interval,
            flush_interval: config.outcome_aggregator().flush_interval,
            buckets: HashMap::new(),
            outcome_producer,
        }
    }

    fn do_flush(&mut self) {
        let bucket_interval = self.bucket_interval;
        let outcome_producer = self.outcome_producer.clone();

        for (bucket_key, quantity) in self.buckets.drain() {
            let BucketKey {
                offset,
                scoping,
                outcome,
                remote_addr,
                category,
            } = bucket_key;

            let timestamp = UnixTimestamp::from_secs(offset * bucket_interval);
            let outcome = TrackOutcome {
                timestamp: timestamp.as_datetime(),
                scoping,
                outcome,
                event_id: None,
                remote_addr,
                category,
                quantity,
            };

            relay_log::trace!("Flushing outcome for timestamp {}", timestamp);
            outcome_producer.do_send(outcome).ok();
        }
    }

    fn flush(&mut self, _context: &mut <Self as Actor>::Context) {
        metric!(timer(RelayTimers::OutcomeAggregatorFlushTime), {
            self.do_flush();
        });
    }
}

impl Actor for OutcomeAggregator {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        relay_log::info!("outcome aggregator started");
        if self.mode != AggregationMode::DropEverything {
            ctx.run_interval(Duration::from_secs(self.bucket_interval), Self::flush);
        }
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        relay_log::info!("outcome aggregator stopped");
    }
}

impl Supervised for OutcomeAggregator {}

impl SystemService for OutcomeAggregator {}

impl Default for OutcomeAggregator {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
    }
}

impl Handler<TrackOutcome> for OutcomeAggregator {
    type Result = Result<(), OutcomeError>;

    fn handle(&mut self, msg: TrackOutcome, _ctx: &mut Self::Context) -> Self::Result {
        relay_log::trace!("Outcome aggregation requested: {:?}", msg);

        if self.mode == AggregationMode::DropEverything {
            return Ok(());
        }

        // For lossy aggregation, erase some fields to have fewer buckets
        let (event_id, remote_addr) = match self.mode {
            AggregationMode::Lossy => {
                relay_log::trace!("Erasing event_id, remote_addr for aggregation: {:?}", msg);
                (None, None)
            }
            _ => (msg.event_id, msg.remote_addr),
        };

        if let Some(event_id) = event_id {
            relay_log::trace!("Forwarding outcome without aggregation: {}", event_id);
            self.outcome_producer.do_send(msg).ok();
            return Ok(());
        }

        let TrackOutcome {
            timestamp,
            scoping,
            outcome,
            event_id: _,
            remote_addr,
            category,
            quantity,
        } = msg;

        // TODO: timestamp validation? (min, max)
        let offset = timestamp.timestamp() as u64 / self.bucket_interval;

        let bucket_key = BucketKey {
            offset,
            scoping,
            outcome,
            remote_addr,
            category,
        };

        let counter = self.buckets.entry(bucket_key).or_insert(0);
        *counter += quantity;

        Ok(())
    }
}
