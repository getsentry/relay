use std::sync::Arc;

#[cfg(feature = "processing")]
use futures::future;
use relay_cogs::{AppFeature, FeatureWeights};
use relay_quotas::{DataCategory, RateLimits};

use crate::Envelope;
use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{Counted, Managed, ManagedEnvelope, OutcomeError, Quantities, Rejected};
use crate::processing::{self, Context, CountRateLimited, Forward, Output, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};

#[cfg(feature = "processing")]
mod limiter;
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
    #[cfg(feature = "processing")]
    redis: Option<Arc<relay_quotas::RedisRateLimiter>>,
}

impl CheckInsProcessor {
    /// Creates a new [`Self`].
    pub fn new(
        limiter: Arc<QuotaRateLimiter>,
        #[cfg(feature = "processing")] redis: Option<Arc<relay_quotas::RedisRateLimiter>>,
    ) -> Self {
        Self {
            limiter,
            #[cfg(feature = "processing")]
            redis,
        }
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
        mut check_ins: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        #[cfg_attr(not(feature = "processing"), allow(unused_variables))]
        let monitors = ctx
            .is_processing()
            .then(|| process::normalize(&mut check_ins));

        #[cfg_attr(not(feature = "processing"), allow(unused_mut))]
        let mut check_ins = self.limiter.enforce_quotas(check_ins, ctx).await?;

        #[cfg(feature = "processing")]
        if let (Some(monitors), Some(redis)) = (monitors, &self.redis) {
            self.enforce_monitor_limits(&mut check_ins, &monitors, redis, ctx)
                .await;
        }

        Ok(Output::just(CheckInsOutput(check_ins)))
    }
}

#[cfg(feature = "processing")]
impl CheckInsProcessor {
    /// Drops check-ins whose monitor has exceeded its own limit.
    async fn enforce_monitor_limits(
        &self,
        check_ins: &mut Managed<SerializedCheckIns>,
        monitors: &[(String, String)],
        redis: &relay_quotas::RedisRateLimiter,
        ctx: Context<'_>,
    ) {
        let limit = ctx
            .global_config
            .options
            .cron_monitor_rate_limit
            .unwrap_or(limiter::DEFAULT_LIMIT);

        if limit == 0 {
            return;
        }

        let item_scoping = check_ins.scoping().item(DataCategory::Monitor);
        let mut limited = future::join_all(monitors.iter().map(|(slug, environment)| {
            let quota = limiter::monitor_quota(slug, environment, limit, limiter::DEFAULT_WINDOW);

            async move {
                match redis
                    .is_rate_limited(&[quota], item_scoping, 1, false)
                    .await
                {
                    Ok(limits) => limits.is_limited().then_some(limits),
                    Err(err) => {
                        relay_log::error!(
                            error = &err as &dyn std::error::Error,
                            "failed to check monitor check-in rate limit"
                        );
                        None
                    }
                }
            }
        }))
        .await;

        let mut i = 0;
        check_ins.retain(
            |check_ins| &mut check_ins.check_ins,
            |_check_in, _| {
                let limits = limited.get_mut(i).and_then(Option::take);
                i += 1;

                match limits {
                    Some(limits) => Err(Error::RateLimited(limits)),
                    None => Ok(()),
                }
            },
        );
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
        use crate::services::store::StoreCheckIn;

        let sdk = self.0.headers.meta().client().map(str::to_owned);
        let retention_days = ctx.event_retention().standard;

        for check_in in self.0.split(|work| work.check_ins.into_iter()) {
            s.send_to_store(check_in.map(|check_in, _| StoreCheckIn {
                check_in,
                sdk: sdk.clone(),
                retention_days,
            }));
        }

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
