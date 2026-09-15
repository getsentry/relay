use std::sync::Arc;

use relay_cogs::{AppFeature, FeatureWeights};
use relay_monitors::{CheckIn, ProcessCheckInError};
use relay_quotas::{DataCategory, Dimension, RateLimits};

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
    type Input = SerializedCheckIns;
    type Output = CheckInsOutput;
    type Error = Error;

    fn cogs() -> FeatureWeights {
        AppFeature::CheckIns.into()
    }

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();

        let check_ins = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(*item.ty(), ItemType::CheckIn))
            .into_vec();

        if check_ins.is_empty() {
            return None;
        }

        let work = SerializedCheckIns { headers, check_ins };
        Some(Managed::with_meta_from_managed_envelope(envelope, work))
    }

    async fn process(
        &self,
        input: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        let mut ex_check_in = process::expand(input)?;

        process::normalize(&mut ex_check_in)?;

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
        let envelope = self.0.try_map(|ExpandedCheckIn { headers, check_in }, _| {
            let mut item = Item::new(ItemType::CheckIn);
            item.set_payload(
                ContentType::Json,
                serde_json::to_vec(&check_in)
                    .map_err(ProcessCheckInError::from)
                    .map_err(Error::from)?,
            );
            Ok::<Box<Envelope>, Error>(Envelope::from_parts(headers, smallvec::smallvec![item]))
        });

        envelope.map_err(|e| e.map(|_| ()))
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

        s.send_to_store(
            self.0
                .try_map(|expanded_check_in, _| {
                    let routing_hint =
                        relay_monitors::routing_hint(&expanded_check_in.check_in, &project_id);
                    let mut item = Item::new(ItemType::CheckIn);
                    item.set_payload(
                        ContentType::Json,
                        serde_json::to_vec(&expanded_check_in.check_in)
                            .map_err(ProcessCheckInError::from)
                            .map_err(Error::from)?,
                    );

                    item.set_routing_hint(routing_hint);
                    Ok::<StoreCheckIn, Error>(StoreCheckIn {
                        check_in: item,
                        sdk: sdk.clone(),
                        retention_days,
                    })
                })
                .map_err(|e| e.map(|_| ()))?,
        );

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

impl Counted for ExpandedCheckIn {
    fn quantities(&self) -> Quantities {
        smallvec::smallvec![(DataCategory::Monitor, 1)]
    }
}

impl CountRateLimited for Managed<ExpandedCheckIn> {
    type Error = Error;

    fn dimensions(&self) -> Option<Arc<[(Dimension, String)]>> {
        let dims = [
            (
                relay_quotas::Dimension::CheckInEnvironment,
                self.check_in.environment.clone().unwrap_or_default(),
            ),
            (
                relay_quotas::Dimension::CheckInSlug,
                self.check_in.monitor_slug.clone(),
            ),
        ];

        Some(dims.into())
    }
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::EventId;

    use crate::extractors::RequestMeta;
    use crate::managed::ManagedTestHandle;
    use crate::processing::limits::CountRateLimited;

    use super::*;

    /// Builds a [`Managed<ExpandedCheckIn>`] from the passed check-in payload.
    fn expanded_check_in(payload: &str) -> (Managed<ExpandedCheckIn>, ManagedTestHandle) {
        let dsn = "https://a94ae32be2584e0bbd7a4cbb95971fee:@sentry.io/42"
            .parse()
            .unwrap();
        let envelope = Envelope::from_request(Some(EventId::new()), RequestMeta::new(dsn));

        let mut item = Item::new(ItemType::CheckIn);
        item.set_payload(ContentType::Json, payload.to_owned());

        let check_in = ExpandedCheckIn {
            headers: envelope.headers().to_owned(),
            check_in: serde_json::from_str(payload).unwrap(),
        };

        Managed::for_test(check_in).build()
    }

    #[test]
    fn test_check_in_dimensions() {
        let (check_in, mut handle) = expanded_check_in(
            r#"{
                "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
                "monitor_slug": "my-monitor",
                "status": "ok",
                "environment": "production"
            }"#,
        );

        assert_eq!(
            check_in.dimensions().as_deref(),
            Some(
                &[
                    (Dimension::CheckInEnvironment, "production".to_owned()),
                    (Dimension::CheckInSlug, "my-monitor".to_owned()),
                ][..]
            )
        );

        drop(check_in);
        handle.assert_internal_outcome(DataCategory::Monitor, 1);
    }

    #[test]
    fn test_check_in_dimensions_without_environment() {
        let (check_in, mut handle) = expanded_check_in(
            r#"{
                "check_in_id": "a460c25ff2554577b920fcfacae4e5eb",
                "monitor_slug": "my-monitor",
                "status": "ok"
            }"#,
        );

        // A check-in without an environment still has to produce a dimension for it, otherwise
        // it would not match a quota which keys on the environment.
        assert_eq!(
            check_in.dimensions().as_deref(),
            Some(
                &[
                    (Dimension::CheckInEnvironment, String::new()),
                    (Dimension::CheckInSlug, "my-monitor".to_owned()),
                ][..]
            )
        );

        drop(check_in);
        handle.assert_internal_outcome(DataCategory::Monitor, 1);
    }
}
