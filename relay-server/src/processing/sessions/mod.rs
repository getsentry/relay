use std::sync::Arc;

use relay_event_schema::protocol::{SessionAggregates, SessionUpdate};
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

mod process;

type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The check-ins are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),

    #[error("invalid sesion item")]
    Invalid,
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::Invalid => None,
        };
        (outcome, self)
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

/// A processor for Sessions.
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
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        mut sessions: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        // TODO: if session metric extraction is not enabled, we can skip here
        // If it is not enabled and a processing Relay -> drop (fast filter?).
        let mut sessions = process::expand(sessions);

        process::normalize(&mut sessions, ctx);

        // self.limiter.enforce_quotas(&mut check_ins, ctx).await?;

        // Ok(Output::just(SessionsOutput(check_ins)))
        todo!()
    }
}

/// Output produced by the [`CheckInsProcessor`].
#[derive(Debug)]
pub struct SessionsOutput(Managed<SerializedSessions>);

impl Forward for SessionsOutput {
    fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        todo!()
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
    ) -> Result<(), Rejected<()>> {
        todo!()
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

impl Counted for SerializedSessions {
    fn quantities(&self) -> Quantities {
        todo!()
    }
}

#[derive(Debug)]
pub struct ExpandedSessions {
    headers: EnvelopeHeaders,

    updates: Vec<SessionUpdate>,
    aggregates: Vec<SessionAggregates>,
}

impl Counted for ExpandedSessions {
    fn quantities(&self) -> Quantities {
        todo!()
    }
}
