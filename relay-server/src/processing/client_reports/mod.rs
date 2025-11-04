use std::sync::Arc;

use relay_quotas::RateLimits;

use crate::envelope::{EnvelopeHeaders, Item, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{
    self, Context, CountRateLimited, Forward, ForwardContext, Output, QuotaRateLimiter,
};
use crate::services::outcome::Outcome;

mod process;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The client-reports are rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
        };
        (outcome, self)
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

/// A processor for Client-Reports.
pub struct ClientReportsProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl ClientReportsProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl processing::Processor for ClientReportsProcessor {
    type UnitOfWork = SerializedClientReport;
    type Output = ClientReportOutput;
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

        let work = SerializedClientReport {
            headers,
            client_reports,
        };
        Some(Managed::from_envelope(envelope, work))
    }

    async fn process(
        &self,
        mut client_reports: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        // FIXME: Decide if we want to make the TrackedOutcomes the output of this processor.
        let outcomes =
            process::process_client_reports(&mut client_reports, ctx.config, ctx.project_info);

        // FIXME: Are there even quotas on a client_report (the old code did check quotas but seems strange)
        self.limiter
            .enforce_quotas(&mut client_reports, ctx)
            .await?;

        // FIXME: Looking at the 'old' processing code seems like we might still need to emit some
        // metrics here
        Ok(Output::just(ClientReportOutput(client_reports)))
    }
}

// FIXME: The correct output might actually be the TrackedOutcomes that we generate
/// Output produced by the [`ClientReportsProcessor`].
#[derive(Debug)]
pub struct ClientReportOutput(Managed<SerializedClientReport>);

impl Forward for ClientReportOutput {
    fn serialize_envelope(
        self,
        ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<crate::Envelope>>, Rejected<()>> {
        // FIXME: Understand what should happen here
        todo!()
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: &relay_system::Addr<crate::services::store::Store>,
        ctx: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        // FIXME: Understand what should happen here
        todo!()
    }
}

/// Client-Reports in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedClientReport {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// A list of client-reports waiting to be processed.
    ///
    /// All items contained here must be client-reports.
    client_reports: Vec<Item>,
}

impl Counted for SerializedClientReport {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![]
    }
}

impl CountRateLimited for Managed<SerializedClientReport> {
    type Error = Error;
}
