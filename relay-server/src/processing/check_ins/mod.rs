use std::sync::Arc;

use relay_cogs::{AppFeature, FeatureWeights};
use relay_monitors::{CheckIn, ProcessCheckInError};
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{ContentType, EnvelopeHeaders, Item, ItemType};
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

/// An expanded/deserialized CheckIn, including the originating item.
#[derive(Debug)]
pub struct ExpandedCheckIn {
    headers: EnvelopeHeaders,
    check_in: CheckIn,
    item: Item,
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
    type Input = SerializedCheckIn;
    type Output = CheckInsOutput;
    type Error = Error;

    fn cogs() -> FeatureWeights {
        AppFeature::CheckIns.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();

        let check_in = envelope
            .envelope_mut()
            .take_item_by(|item| matches!(*item.ty(), ItemType::CheckIn))?;

        let work = SerializedCheckIn { headers, check_in };
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        input: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let mut ex_check_in = process::expand(input)?;

        process::normalize(&mut ex_check_in)?;

        if ctx.is_processing() {
            ex_check_in.try_modify(|e, _| {
                let s = serde_json::to_vec(&e.check_in)
                    .map_err(ProcessCheckInError::from)
                    .map_err(Error::from)?;
                e.item.set_payload(ContentType::Json, s);

                Ok::<_, Error>(())
            })?;
        }
        let ex_check_in = self.limiter.enforce_quotas(ex_check_in, ctx).await?;

        Ok(Output::just(CheckInsOutput(ex_check_in)))
    }
}

/// Output produced by the [`CheckInsProcessor`].
#[derive(Debug)]
pub struct CheckInsOutput(Managed<ExpandedCheckIn>);

impl Forward for CheckInsOutput {
    fn serialize_envelope(
        self,
        _: processing::ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let envelope = self.0.map(
            |ExpandedCheckIn {
                 headers,
                 check_in: _,
                 item,
             },
             _| { Envelope::from_parts(headers, smallvec::smallvec![item]) },
        );

        Ok(envelope)
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: processing::StoreHandle<'_>,
        ctx: processing::ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        use crate::services::store::StoreCheckIn;

        let sdk = self.0.headers.meta().client().map(str::to_owned);
        let retention_days = ctx.event_retention().standard;
        let project_id = self.0.scoping().project_id;

        s.send_to_store(self.0.map(|mut ser_check_in, _| {
            let routing_hint = relay_monitors::routing_hint(&ser_check_in.check_in, &project_id);

            ser_check_in.item.set_routing_hint(routing_hint);
            StoreCheckIn {
                check_in: ser_check_in.item,
                sdk: sdk.clone(),
                retention_days,
            }
        }));

        Ok(())
    }
}

/// Check-Ins in their serialized state, as transported in an envelope.
#[derive(Debug)]
pub struct SerializedCheckIn {
    /// Original envelope headers.
    headers: EnvelopeHeaders,

    /// The check-in waiting to be processed.
    check_in: Item,
}

impl Counted for SerializedCheckIn {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Monitor, 1)]
    }
}

impl Counted for ExpandedCheckIn {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Monitor, 1)]
    }
}

impl CountRateLimited for Managed<SerializedCheckIn> {
    type Error = Error;
}

impl CountRateLimited for Managed<ExpandedCheckIn> {
    type Error = Error;
}
