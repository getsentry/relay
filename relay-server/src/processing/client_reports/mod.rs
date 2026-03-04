use relay_common::time::UnixTimestamp;
use relay_quotas::DataCategory;
use relay_system::Addr;

use crate::envelope::{EnvelopeHeaders, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{self, Context, Nothing, Output};
use crate::services::outcome::{Outcome, TrackOutcome};

type Result<T, E = Error> = std::result::Result<T, E>;

mod process;
mod validate;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The client report could not be parsed from JSON.
    #[error("invalid json: {0}")]
    CouldNotParse(#[from] serde_json::Error),

    /// The client report outcome reason was too long.
    #[error("outcome with an overlong reason")]
    OverlongOutcomeReason,

    /// The timestamp of the client report outcome could not be parsed.
    #[error("invalid timestamp")]
    InvalidTimestamp,

    /// The timestamp of the client report outcome is too far in the past.
    #[error("outcome is too old")]
    OutcomeTooOld,

    /// The timestamp of the client report outcome is too far in the future.
    #[error("outcome in the future")]
    OutcomeInFuture,

    /// Could not reconstruct an [`Outcome`] from the client report outcome.
    #[error("invalid outcome")]
    InvalidOutcome,
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        // Client reports are outcomes, and we do not emit outcomes for outcomes.
        (None, self)
    }
}

/// A processor for client reports.
///
/// Converts client reports into outcomes and emits them to the outcome
/// aggregator. Does not produce any other output.
pub struct ClientReportsProcessor {
    outcome_aggregator: Addr<TrackOutcome>,
}

impl ClientReportsProcessor {
    /// Creates a new [`Self`].
    pub fn new(outcome_aggregator: Addr<TrackOutcome>) -> Self {
        Self { outcome_aggregator }
    }
}

impl processing::Processor for ClientReportsProcessor {
    type UnitOfWork = SerializedClientReports;
    type Output = Nothing;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();
        let reports = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::ClientReport));

        if reports.is_empty() {
            return None;
        }

        let work = SerializedClientReports { headers, reports };
        Some(Managed::with_meta_from(envelope, work))
    }

    async fn process(
        &self,
        reports: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let mut outcomes = process::expand(reports);

        process::normalize(&mut outcomes);
        validate::validate(&mut outcomes, ctx);
        process::emit(outcomes, &self.outcome_aggregator);

        Ok(Output::empty())
    }
}

/// Serialized client reports extracted from an envelope.
#[derive(Debug)]
pub struct SerializedClientReports {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// A list of client reports.
    reports: Items,
}

impl Counted for SerializedClientReports {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![]
    }
}

/// Client report outcomes extracted from [`SerializedClientReports`].
#[derive(Debug)]
pub struct ClientOutcomes {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// A list of client report outcomes.
    outcomes: Vec<ClientOutcome>,
}

impl Counted for ClientOutcomes {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![]
    }
}

/// The source of a client report outcome.
#[derive(Clone, Copy, Debug)]
pub enum ClientOutcomeType {
    /// Filtered by an inbound data filter.
    Filtered,
    /// Filtered by a sampling rule.
    FilteredSampling,
    /// Rate limited.
    RateLimited,
    /// Discarded on the client side.
    ClientDiscard,
}

/// An individual client outcome.
#[derive(Debug)]
pub struct ClientOutcome {
    outcome_type: ClientOutcomeType,
    timestamp: UnixTimestamp,
    reason: String,
    category: DataCategory,
    quantity: u32,
}

impl Counted for ClientOutcome {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![]
    }
}
