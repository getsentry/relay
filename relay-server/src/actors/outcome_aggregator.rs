//! This module contains the outcomes aggregator, which collects similar outcomes into groups
//! and flushed them periodically.

use std::{
    collections::{BTreeMap, HashMap},
    net::IpAddr,
};

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
    /// Scoping of the request.
    pub scoping: Scoping,
    /// The outcome.
    pub outcome: Outcome,
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
    /// The time we should wait before flushing a bucket, in seconds
    flush_delay: u64,
    /// Maximum capacity of the actor's inbox
    mailbox_size: usize,
    /// Mapping from offset to bucket key to quantity. timestamp = offset * bucket_interval
    buckets: BTreeMap<u64, HashMap<BucketKey, u32>>,
    /// The recipient of the aggregated outcomes
    outcome_producer: Recipient<TrackOutcome>,
}

impl OutcomeAggregator {
    pub fn new(config: &Config, outcome_producer: Recipient<TrackOutcome>) -> Self {
        // Set mailbox size to envelope buffer size, as in other global actors
        let mailbox_size = config.envelope_buffer_size() as usize;

        Self {
            bucket_interval: config.outcome_bucket_interval(),
            flush_delay: config.outcome_flush_delay(),
            mailbox_size,
            buckets: BTreeMap::new(),
            outcome_producer,
        }
    }

    fn do_flush(&mut self) {
        let max_offset = (UnixTimestamp::now().as_secs() - self.flush_delay) / self.bucket_interval;
        let bucket_interval = self.bucket_interval;
        let outcome_producer = self.outcome_producer.clone();
        self.buckets.retain(|offset, mapping| {
            if offset <= &max_offset {
                for (bucket_key, quantity) in mapping.drain() {
                    let BucketKey {
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
                    outcome_producer.do_send(outcome).ok(); // TODO: should we handle send errors here?
                }
                false
            } else {
                true
            }
        });
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
        ctx.set_mailbox_capacity(self.mailbox_size);

        relay_log::info!("outcome aggregator started");

        ctx.run_interval(Duration::from_secs(self.bucket_interval), Self::flush);
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

        if msg.event_id.is_some() {
            // event_id is too fine-grained to aggregate, simply forward to producer
            self.outcome_producer.do_send(msg);
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
            scoping,
            outcome,
            remote_addr,
            category,
        };

        let time_slot = self.buckets.entry(offset).or_default();
        let counter = time_slot.entry(bucket_key).or_insert(0);
        *counter += quantity;

        Ok(())
    }
}
