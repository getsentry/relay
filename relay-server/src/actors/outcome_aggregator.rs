//! This module contains the outcomes aggregator, which collects similar outcomes into groups
//! and flushed them periodically.

use std::collections::HashMap;
use std::net::IpAddr;
use std::pin::Pin;
use std::time::Duration;

use futures::future::{self, Either};
use relay_common::{DataCategory, UnixTimestamp};
use relay_config::{Config, EmitOutcomes};
use relay_quotas::Scoping;
use relay_statsd::metric;
use relay_system::{Addr, Controller, Service, ServiceMessage, Shutdown};
use tokio::sync::{mpsc, oneshot};

use crate::actors::outcome::{DiscardReason, Outcome, OutcomeProducer, TrackOutcome};
use crate::service::REGISTRY;
use crate::statsd::RelayTimers;

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
    outcome_producer: Addr<OutcomeProducer>,

    // Sleep must be `Box<Pin>` in order for it to be Unpin.
    // See: https://docs.rs/tokio/1.21.0/tokio/time/struct.Sleep.html#examples
    flush_handle: Either<future::Pending<()>, Pin<Box<tokio::time::Sleep>>>,
}

impl OutcomeAggregator {
    pub fn from_registry() -> Addr<Self> {
        REGISTRY.get().unwrap().outcome_aggregator.clone()
    }

    pub fn new(config: &Config, outcome_producer: Addr<OutcomeProducer>) -> Self {
        let mode = match config.emit_outcomes() {
            EmitOutcomes::AsOutcomes => AggregationMode::Lossless,
            EmitOutcomes::AsClientReports => AggregationMode::Lossy,
            EmitOutcomes::None => AggregationMode::DropEverything,
        };

        Self {
            mode,
            bucket_interval: config.outcome_aggregator().bucket_interval,
            flush_interval: config.outcome_aggregator().flush_interval,
            buckets: HashMap::new(),
            outcome_producer,
            flush_handle: Either::Left(future::pending()),
        }
    }

    pub fn start(mut self) -> Addr<Self> {
        relay_log::info!("outcome aggregator started");
        let (tx, mut rx) = mpsc::unbounded_channel::<OutcomeAggregatorMessages>();

        tokio::spawn(async move {
            let mut shutdown_rx = Controller::subscribe_v2().await;
            loop {
                tokio::select! {
                    Some(message) = rx.recv() => self.handle_message(message),
                    () = &mut self.flush_handle => self.flush(),
                    _ = shutdown_rx.changed() => {
                        self.handle_shutdown(&shutdown_rx.borrow_and_update())
                    },
                    else => break,
                }
            }
            relay_log::info!("outcome aggregator stopped");
        });

        Addr { tx }
    }

    fn handle_shutdown(&mut self, message: &Option<Shutdown>) {
        if let Some(message) = message {
            if message.timeout.is_some() {
                self.flush();
                relay_log::info!("outcome aggregator stopped");
            }
        }
    }

    fn handle_message(&mut self, message: OutcomeAggregatorMessages) {
        match message {
            OutcomeAggregatorMessages::TrackOutcome(msg) => self.handle_track_outcome(msg),
        }
    }

    fn handle_track_outcome(&mut self, msg: TrackOutcome) {
        relay_log::trace!("Outcome aggregation requested: {:?}", msg);

        if self.mode == AggregationMode::DropEverything {
            return;
        }

        let (event_id, remote_addr) = if self.erase_high_cardinality_fields(&msg) {
            relay_log::trace!("Erasing event_id, remote_addr for aggregation: {:?}", msg);
            (None, None)
        } else {
            (msg.event_id, msg.remote_addr)
        };

        if let Some(event_id) = event_id {
            relay_log::trace!("Forwarding outcome without aggregation: {}", event_id);
            let _ = self.outcome_producer.send(msg);
            return;
        }

        let offset = msg.timestamp.timestamp() as u64 / self.bucket_interval;

        let bucket_key = BucketKey {
            offset,
            scoping: msg.scoping,
            outcome: msg.outcome,
            remote_addr,
            category: msg.category,
        };

        let counter = self.buckets.entry(bucket_key).or_insert(0);
        *counter += msg.quantity;

        if self.flush_interval == 0 {
            // Flush immediately. This is useful for integration tests.
            self.do_flush();
        } else if let Either::Left(_) = &self.flush_handle {
            if self.mode != AggregationMode::DropEverything {
                let flush_interval = Duration::from_secs(self.flush_interval);
                self.flush_handle = Either::Right(Box::pin(tokio::time::sleep(flush_interval)));
            }
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
            let _ = outcome_producer.send(outcome);
        }
    }

    /// Return true if event_id and remote_addr should be erased
    fn erase_high_cardinality_fields(&self, msg: &TrackOutcome) -> bool {
        // In lossy mode, always erase
        matches!(self.mode, AggregationMode::Lossy)
            || matches!(
                // Always erase high-cardinality fields for specific outcomes:
                msg.outcome,
                Outcome::RateLimited(_)
                    | Outcome::Invalid(DiscardReason::ProjectId)
                    | Outcome::FilteredSampling(_)
            )
    }

    fn flush(&mut self) {
        let flush_interval = Duration::from_secs(self.flush_interval);
        self.flush_handle = Either::Right(Box::pin(tokio::time::sleep(flush_interval)));

        metric!(timer(RelayTimers::OutcomeAggregatorFlushTime), {
            self.do_flush();
        });
    }
}

impl Service for OutcomeAggregator {
    type Messages = OutcomeAggregatorMessages;
}

impl ServiceMessage<OutcomeAggregator> for TrackOutcome {
    type Response = ();

    fn into_messages(self) -> (OutcomeAggregatorMessages, oneshot::Receiver<Self::Response>) {
        let (tx, rx) = oneshot::channel();
        tx.send(()).ok();
        (OutcomeAggregatorMessages::TrackOutcome(self), rx)
    }
}

pub enum OutcomeAggregatorMessages {
    TrackOutcome(TrackOutcome),
}

impl Default for OutcomeAggregator {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
    }
}
