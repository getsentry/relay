use std::sync::Arc;

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
    /// Failed to process the check-in.
    #[error("failed to process checkin: {0}")]
    Processing(#[from] relay_monitors::ProcessCheckInError),
}

impl OutcomeError for Error {
    type Error = Self;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Self::RateLimited(limits) => {
                let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
                Some(Outcome::RateLimited(reason_code))
            }
            Self::Processing(relay_monitors::ProcessCheckInError::Json(_)) => {
                Some(Outcome::Invalid(DiscardReason::InvalidJson))
            }
            Self::Processing(_) => Some(Outcome::Invalid(DiscardReason::InvalidCheckIn)),
        };
        (outcome, self)
    }
}

impl From<RateLimits> for Error {
    fn from(value: RateLimits) -> Self {
        Self::RateLimited(value)
    }
}

/// A processor for Check-Ins.
pub struct CheckInsProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl CheckInsProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl processing::Processor for CheckInsProcessor {
    type UnitOfWork = SerializedCheckIns;
    type Output = CheckInsOutput;
    type Error = Error;

    fn prepare_envelope(
        &self,
        envelope: &mut ManagedEnvelope,
    ) -> Option<Managed<Self::UnitOfWork>> {
        let headers = envelope.envelope().headers().clone();

        let check_ins = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::CheckIn))
            .into_vec();

        let work = SerializedCheckIns { headers, check_ins };
        Some(Managed::with_meta_from(envelope, work))
    }

    async fn process(
        &self,
        mut check_ins: Managed<Self::UnitOfWork>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        if ctx.is_processing() {
            process::normalize(&mut check_ins);
        }

        let check_ins = self.limiter.enforce_quotas(check_ins, ctx).await?;

        Ok(Output::just(CheckInsOutput(check_ins)))
    }
}

/// Output produced by the [`CheckInsProcessor`].
#[derive(Debug)]
pub struct CheckInsOutput(Managed<SerializedCheckIns>);

impl Forward for CheckInsOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let envelope = self.0.map(|SerializedCheckIns { headers, check_ins }, _| {
            Envelope::from_parts(headers, Items::from_vec(check_ins))
        });

        Ok(envelope)
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let envelope = self.serialize_envelope(ctx)?;
        let envelope = ManagedEnvelope::from(envelope).into_processed();

        s.store(crate::services::store::StoreEnvelope { envelope });

        Ok(())
    }
}

/// Check-Ins in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedCheckIns {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// A list of check-ins waiting to be processed.
    ///
    /// All items contained here must be check-ins.
    check_ins: Vec<Item>,
}

impl Counted for SerializedCheckIns {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Monitor, self.check_ins.len())]
    }
}

impl CountRateLimited for Managed<SerializedCheckIns> {
    type Error = Error;
}
