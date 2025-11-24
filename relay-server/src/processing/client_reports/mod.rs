use relay_metrics::UnixTimestamp;
use relay_quotas::DataCategory;
use relay_system::Addr;

use crate::envelope::{EnvelopeHeaders, Item, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{self, Context, Nothing, Output};
use crate::services::outcome::{Outcome, TrackOutcome};

mod process;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Client report parsing failed.
    #[error("invalid client report")]
    InvalidClientReport,
    /// Processing relay has client outcomes disabled.
    #[error("client outcomes disabled")]
    OutcomesDisabled,
    /// Client report timestamp is too old.
    #[error("client report timestamp too old")]
    TimestampTooOld,
    /// Client report timestamp is too far in the future.
    #[error("client report timestamp in future")]
    TimestampInFuture,
    /// Client report contains an invalid outcome combination.
    #[error("invalid outcome combination")]
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
pub struct ClientReportsProcessor {
    aggregator: Addr<TrackOutcome>,
}

impl ClientReportsProcessor {
    /// Creates a new [`Self`].
    pub fn new(aggregator: Addr<TrackOutcome>) -> Self {
        Self { aggregator }
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

        let client_reports = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::ClientReport))
            .into_vec();

        let work = SerializedClientReports {
            headers,
            client_reports,
        };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        client_reports: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        if !ctx.config.emit_outcomes().any() && ctx.config.processing_enabled() {
            // if a processing relay has client outcomes disabled we drop them without processing.
            return Err(client_reports.reject_err(Error::OutcomesDisabled));
        }

        let client_reports = process::expand(client_reports);

        // FIXME: This guard is taken over from the old code not sure if we still need this.
        if !client_reports.output_events.is_empty() {
            let client_reports = process::validate(client_reports, ctx.config, ctx.project_info)?;
            process::emit(client_reports, &self.aggregator);
        }

        Ok(Output::empty())
    }
}

/// Client-Reports in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedClientReports {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// A list of client-reports waiting to be processed.
    ///
    /// All items contained here must be client-reports.
    client_reports: Vec<Item>,
}

impl Counted for SerializedClientReports {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![]
    }
}

/// Client reports which have been parsed and aggregated from their serialized state.
pub struct ExpandedClientReports {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// The timestamp of when the reports were created.
    timestamp: Option<UnixTimestamp>,
    /// Aggregated outcome events from all parsed client reports.
    output_events: Vec<OutcomeEvent>,
}

impl Counted for ExpandedClientReports {
    fn quantities(&self) -> crate::managed::Quantities {
        smallvec::smallvec![]
    }
}

/// A aggregated single outcome event.
pub struct OutcomeEvent {
    outcome_type: ClientReportField,
    reason: String,
    category: DataCategory,
    quantity: u32,
}

impl Counted for OutcomeEvent {
    fn quantities(&self) -> crate::managed::Quantities {
        smallvec::smallvec![]
    }
}

/// Client reports which have been validated and converted to trackable outcomes.
pub struct ValidatedClientReports {
    outcomes: Vec<TrackOutcome>,
}

impl Counted for ValidatedClientReports {
    fn quantities(&self) -> crate::managed::Quantities {
        smallvec::smallvec![]
    }
}

/// Fields of client reports that map to specific [`Outcome`]s without content.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClientReportField {
    /// The event has been filtered by an inbound data filter.
    Filtered,
    /// The event has been filtered by a sampling rule.
    FilteredSampling,
    /// The event has been rate limited.
    RateLimited,
    /// The event has already been discarded on the client side.
    ClientDiscard,
}
