//! This module contains the outcomes aggregator, which collects similar outcomes into groups
//! and flushed them periodically.

use std::{collections::HashMap, net::IpAddr};

use actix::{Actor, AsyncContext, Context, Handler, Recipient, Supervised, SystemService};
use relay_common::{DataCategory, UnixTimestamp};
use relay_config::Config;
use relay_general::protocol::EventId;
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
    /// The event id.
    pub event_id: Option<EventId>,
    /// The client ip address.
    pub remote_addr: Option<IpAddr>,
    /// The event's data category.
    pub category: DataCategory,
}

/// Aggregates outcomes into buckets, and flushes them periodically.
/// Inspired by [`relay_metrics::Aggregator`].
pub struct OutcomeAggregator {
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
        Self {
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
                event_id,
                remote_addr,
                category,
            } = bucket_key;

            let timestamp = UnixTimestamp::from_secs(offset * bucket_interval);
            let outcome = TrackOutcome {
                timestamp: timestamp.as_datetime(),
                scoping,
                outcome,
                event_id,
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
        ctx.run_interval(Duration::from_secs(self.flush_interval), Self::flush);
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

        let TrackOutcome {
            timestamp,
            scoping,
            outcome,
            event_id,
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
            event_id,
            remote_addr,
            category,
        };

        let counter = self.buckets.entry(bucket_key).or_insert(0);
        *counter += quantity;

        Ok(())
    }
}
