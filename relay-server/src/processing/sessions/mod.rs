use std::sync::Arc;

use relay_cogs::{AppFeature, FeatureWeights};
use relay_event_schema::protocol::{SessionAggregates, SessionUpdate};
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemPayload, Items, SessionItem, SessionsItem};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::sessions::process::Expansion;
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::Outcome;

mod filter;
mod process;

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
    type Input = SerializedSessions;
    type Output = SessionsOutput;
    type Error = Error;

    fn cogs() -> FeatureWeights {
        AppFeature::Sessions.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();

        let updates: Vec<_> = envelope
            .envelope_mut()
            .drain_items::<SessionItem>()
            .collect();

        let aggregates: Vec<_> = envelope
            .envelope_mut()
            .drain_items::<SessionsItem>()
            .collect();

        if updates.is_empty() && aggregates.is_empty() {
            return None;
        }

        let work = SerializedSessions {
            headers,
            updates,
            aggregates,
        };
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        sessions: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let mut sessions = match process::expand(sessions, ctx) {
            Expansion::Continue(sessions) => sessions,
            Expansion::Forward(sessions) => return Ok(Output::just(SessionsOutput(sessions))),
        };

        // We can apply filters before normalization here, as our filters currently do not depend
        // on any normalization steps. Changes to the filtering in the future may make it necessary
        // to switch the order of operations.
        filter::filter(&mut sessions, ctx);

        process::normalize(&mut sessions, ctx);

        let sessions = self.limiter.enforce_quotas(sessions, ctx).await?;

        let sessions = process::extract_metrics(sessions, ctx);
        Ok(Output::metrics(sessions))
    }
}

/// Output produced by the [`SessionsProcessor`].
#[derive(Debug)]
pub struct SessionsOutput(Managed<SerializedSessions>);

impl Forward for SessionsOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        Ok(self.0.map(|sessions, _| sessions.serialize_envelope()))
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        _: processing::forward::StoreHandle<'_>,
        _: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let SessionsOutput(sessions) = self;
        Err(sessions.internal_error("sessions should always be extracted into metrics"))
    }
}

/// Spans in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedSessions {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// A list of sessions waiting to be processed.
    updates: Vec<SessionItem>,
    /// A list of session aggregates waiting to be processed.
    aggregates: Vec<SessionsItem>,
}

impl SerializedSessions {
    fn serialize_envelope(self) -> Box<Envelope> {
        let mut items = Items::with_capacity(self.updates.len() + self.aggregates.len());
        // Preserve the previous longer-collection-first wire order.
        if self.updates.len() > self.aggregates.len() {
            items.extend(self.updates.into_iter().map(Item::from));
            items.extend(self.aggregates.into_iter().map(Item::from));
        } else {
            items.extend(self.aggregates.into_iter().map(Item::from));
            items.extend(self.updates.into_iter().map(Item::from));
        }
        Envelope::from_parts(self.headers, items)
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

#[derive(Debug)]
pub struct ExpandedSessions {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// A list of parsed session updates.
    updates: Vec<SessionUpdate>,
    /// A list of parsed session aggregates.
    aggregates: Vec<SessionAggregates>,
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

fn item_count<T: ItemPayload>(items: &[T]) -> usize {
    items
        .iter()
        .map(|item| item.item_count().unwrap_or(1) as usize)
        .sum()
}
