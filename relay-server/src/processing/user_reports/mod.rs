use std::sync::Arc;

use relay_quotas::RateLimits;

use crate::envelope::{EnvelopeHeaders, Item, ItemType};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{Context, CountRateLimited, Output, Processor, QuotaRateLimiter};
use crate::services::outcome::Outcome;
use crate::statsd::RelayCounters;

mod forward;
mod process;

pub use process::process_user_reports;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The user report was rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),

    /// The envelope did not contain an event ID.
    #[cfg(feature = "processing")]
    #[error("missing event ID")]
    NoEventId,
}

impl OutcomeError for Error {
    type Error = Error;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            #[cfg(feature = "processing")]
            Self::NoEventId => Some(Outcome::Invalid(
                crate::services::outcome::DiscardReason::Internal,
            )),
        };
        (outcome, self)
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

/// A processor for user reports.
#[derive(Debug)]
pub struct UserReportsProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl UserReportsProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl Processor for UserReportsProcessor {
    type Input = SerializedUserReports;
    type Output = UserReportsOutput;
    type Error = Error;

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();
        let reports: Vec<Item> = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::UserReport))
            .into_vec();

        if reports.is_empty() {
            return None;
        }

        let reports = SerializedUserReports { headers, reports };
        Some(Managed::with_meta_from(envelope, reports))
    }

    async fn process(
        &self,
        mut reports: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        process::process(&mut reports);

        let reports = self.limiter.enforce_quotas(reports, ctx).await?;

        Ok(Output::just(UserReportsOutput(reports)))
    }
}

/// Serialized user reports extracted from an envelope.
#[derive(Debug)]
pub struct SerializedUserReports {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// A list of reports.
    reports: Vec<Item>,
}

impl Counted for SerializedUserReports {
    fn quantities(&self) -> Quantities {
        self.reports.quantities()
    }
}

impl CountRateLimited for Managed<SerializedUserReports> {
    type Error = Error;
}

/// Output produced by the [`UserReportsProcessor`].
#[derive(Debug)]
pub struct UserReportsOutput(Managed<SerializedUserReports>);
