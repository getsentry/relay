use std::sync::Arc;

use relay_event_schema::protocol::{SessionAggregates, SessionUpdate};
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::sessions::process::Expansion;
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::Outcome;
#[cfg(feature = "processing")]
use crate::statsd::RelayCounters;

mod filter;
mod process;
#[cfg(feature = "processing")]
mod store;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The sessions are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
    /// Sessions filtered due to a filtering rule.
    #[error("sessions filtered")]
    Filtered(relay_filter::FilterStatKey),
    /// The session item is invalid.
    #[error("invalid sesion item")]
    Invalid,
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        // Currently/historically sessions do not emit outcomes.
        (None, self)
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

/// A processor for sessions, individual updates and aggregates.
pub struct SessionsProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl SessionsProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl processing::Processor for SessionsProcessor {
    type UnitOfWork = SerializedSessions;
    type Output = SessionsOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();

        let updates = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Session))
            .into_vec();

        let aggregates = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::Sessions))
            .into_vec();

        let work = SerializedSessions {
            headers,
            updates,
            aggregates,
        };
        Some(Managed::with_meta_from(envelope, work))
    }

    async fn process(
        &self,
        sessions: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let mut sessions = match process::expand(sessions, ctx) {
            Expansion::Continue(sessions) => sessions,
            Expansion::Forward(sessions) => {
                return Ok(Output::just(SessionsOutput::Forward(sessions)));
            }
        };

        // We can apply filters before normalization here, as our filters currently do not depend
        // on any normalization steps. Changes to the filtering in the future may make it necessary
        // to switch the order of operations.
        filter::filter(&mut sessions, ctx);

        process::normalize(&mut sessions, ctx);

        let sessions = self.limiter.enforce_quotas(sessions, ctx).await?;

        // Check if EAP user sessions double-write is enabled.
        // This feature sends session data to both the legacy metrics pipeline
        // and directly to the snuba-items topic as TRACE_ITEM_TYPE_USER_SESSION.
        let eap_enabled = ctx
            .project_info
            .config
            .features
            .has(relay_dynamic_config::Feature::UserSessionsEap);

        let (metrics, eap_sessions) = process::extract_with_eap(sessions, ctx, eap_enabled);

        if let Some(eap_sessions) = eap_sessions {
            // Return both the EAP sessions for storage and the extracted metrics.
            Ok(Output {
                main: Some(SessionsOutput::Store(eap_sessions)),
                metrics: Some(metrics),
            })
        } else {
            // Legacy path: only return metrics.
            Ok(Output::metrics(metrics))
        }
    }
}

/// Output produced by the [`SessionsProcessor`].
#[derive(Debug)]
pub enum SessionsOutput {
    /// Sessions that should be forwarded (non-processing relay).
    Forward(Managed<SerializedSessions>),
    /// Sessions that should be stored to EAP (processing relay with feature enabled).
    Store(Managed<ExpandedSessions>),
}

impl Forward for SessionsOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        match self {
            Self::Forward(sessions) => {
                Ok(sessions.map(|sessions, _| sessions.serialize_envelope()))
            }
            Self::Store(sessions) => {
                // EAP sessions should be stored, not serialized to envelope.
                Err(sessions
                    .internal_error("EAP sessions should be stored, not serialized to envelope"))
            }
        }
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::forward::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        match self {
            Self::Forward(sessions) => {
                // Non-processing relay path - sessions should have been extracted to metrics.
                Err(sessions.internal_error("sessions should always be extracted into metrics"))
            }
            Self::Store(sessions) => {
                // EAP double-write path: convert expanded sessions to TraceItems and store.
                let store_ctx = store::Context {
                    received_at: sessions.received_at(),
                    scoping: sessions.scoping(),
                    retention: ctx.retention(|r| r.session.as_ref()),
                };

                // Split sessions into updates and aggregates, keeping track of the aggregates
                // for later processing.
                let (updates_managed, aggregates) =
                    sessions.split_once(|s, _| (s.updates, s.aggregates));

                // Convert and store each session update.
                for session in updates_managed.split(|updates| updates) {
                    let item = session.try_map(|session, _| {
                        Ok::<_, std::convert::Infallible>(store::convert_session_update(
                            &session, &store_ctx,
                        ))
                    });
                    if let Ok(item) = item {
                        s.store(item);
                        relay_statsd::metric!(
                            counter(RelayCounters::SessionsEapProduced) += 1,
                            session_type = "update"
                        );
                    }
                }

                // Convert and store each session aggregate.
                // Aggregates are expanded into individual session rows to unify the format.
                for aggregate_batch in aggregates.split(|aggs| aggs) {
                    let release = aggregate_batch.attributes.release.clone();
                    let environment = aggregate_batch.attributes.environment.clone();

                    for aggregate in aggregate_batch.split(|batch| batch.aggregates) {
                        // Convert aggregate to multiple individual session items
                        let items = store::convert_session_aggregate(
                            &aggregate,
                            &release,
                            environment.as_deref(),
                            &store_ctx,
                        );

                        for item in items {
                            let managed_item = aggregate.wrap(item);
                            s.store(managed_item);
                            relay_statsd::metric!(
                                counter(RelayCounters::SessionsEapProduced) += 1,
                                session_type = "aggregate"
                            );
                        }
                    }
                }

                Ok(())
            }
        }
    }
}

/// Spans in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedSessions {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// A list of sessions waiting to be processed.
    updates: Vec<Item>,
    /// A list of session aggregates waiting to be processed.
    aggregates: Vec<Item>,
}

impl SerializedSessions {
    fn serialize_envelope(self) -> Box<Envelope> {
        let (mut long, short) = match self.updates.len() > self.aggregates.len() {
            true => (self.updates, self.aggregates),
            false => (self.aggregates, self.updates),
        };
        long.extend(short);
        Envelope::from_parts(self.headers, Items::from_vec(long))
    }
}

impl Counted for SerializedSessions {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(
            DataCategory::Session,
            item_count(&self.updates) + item_count(&self.aggregates),
        )]
    }
}

#[derive(Clone, Debug)]
pub struct ExpandedSessions {
    /// Original envelope headers.
    pub(crate) headers: EnvelopeHeaders,

    /// A list of parsed session updates.
    pub(crate) updates: Vec<SessionUpdate>,
    /// A list of parsed session aggregates.
    pub(crate) aggregates: Vec<SessionAggregates>,
}

impl Counted for ExpandedSessions {
    fn quantities(&self) -> Quantities {
        let aggregates = self
            .aggregates
            .iter()
            .map(|agg| agg.aggregates.len())
            .sum::<usize>();

        smallvec::smallvec![(DataCategory::Session, self.updates.len() + aggregates)]
    }
}

impl CountRateLimited for Managed<ExpandedSessions> {
    type Error = Error;
}

fn item_count(items: &[Item]) -> usize {
    items
        .iter()
        .map(|item| item.item_count().unwrap_or(1) as usize)
        .sum()
}
