//! This module contains the outcomes aggregator, which collects similar outcomes into groups
//! and flushed them periodically.

use std::{collections::BTreeMap, mem::swap, net::IpAddr};

use actix::{Actor, AsyncContext, Context, Handler, Recipient, Supervised, SystemService};
use relay_common::{DataCategory, UnixTimestamp};
use relay_config::Config;
use relay_general::protocol::EventId;
use relay_quotas::Scoping;
use relay_statsd::metric;
use std::time::Duration;

use crate::statsd::RelayTimers;

use super::outcome::{Outcome, OutcomeError, TrackOutcome};

/// Contains all non-temporal fields of [`BucketKey`].
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct InnerKey {
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

/// Contains everything to construct a `TrackOutcome`, except quantity
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct BucketKey {
    /// The time slot for which outcomes are aggregated. timestamp = offset * bucket_interval
    offset: u64,
    inner: Option<InnerKey>,
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
    /// Mapping from bucket key to quantity.
    buckets: BTreeMap<BucketKey, u32>,
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

        let cutoff = BucketKey {
            offset: max_offset + 1,
            inner: None,
        };

        // Only keep items that are equal or newer than cutoff, flush the rest.
        let mut buckets = self.buckets.split_off(&cutoff);
        swap(&mut self.buckets, &mut buckets);

        for (bucket_key, quantity) in buckets {
            let BucketKey { offset, inner } = bucket_key;

            if let Some(inner_key) = inner {
                let InnerKey {
                    scoping,
                    outcome,
                    event_id,
                    remote_addr,
                    category,
                } = inner_key;

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
            inner: Some(InnerKey {
                scoping,
                outcome,
                event_id,
                remote_addr,
                category,
            }),
        };

        let counter = self.buckets.entry(bucket_key).or_insert(0);
        *counter += quantity;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use relay_common::{ProjectId, ProjectKey};
    use relay_quotas::Scoping;

    use crate::actors::outcome::{DiscardReason, Outcome};

    use super::{BucketKey, InnerKey};

    /// Verify that an "only time" key is smaller than a full key
    #[test]
    fn test_bucket_key_order() {
        let key1 = BucketKey {
            offset: 123,
            inner: None,
        };
        let key2 = BucketKey {
            offset: 123,
            inner: Some(InnerKey {
                scoping: Scoping {
                    organization_id: 0,
                    project_id: ProjectId::new(1),
                    project_key: ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap(),
                    key_id: None,
                },
                outcome: Outcome::Invalid(DiscardReason::AuthClient),
                event_id: None,
                remote_addr: None,
                category: relay_common::DataCategory::Attachment,
            }),
        };

        assert!(key1 < key2);
    }
}
