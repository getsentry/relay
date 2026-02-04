use crate::Envelope;
use crate::envelope::EnvelopeHeaders;
use crate::managed::{Counted, Managed, Quantities, Rejected};
use crate::processing::transactions::Error;
use crate::processing::transactions::types::ExpandedProfile;
use crate::processing::{Context, RateLimited, RateLimiter};

/// A standalone profile, which is no longer attached to a transaction as the transaction was
/// dropped by dynamic sampling.
#[derive(Debug)]
pub struct StandaloneProfile {
    pub headers: EnvelopeHeaders,
    pub profile: ExpandedProfile,
}

impl StandaloneProfile {
    pub fn serialize_envelope(self) -> Box<Envelope> {
        let mut item = self.profile.serialize_item();
        // This profile is now standalone, as dynamic sampling dropped the transaction.
        item.set_sampled(false);

        Envelope::from_parts(self.headers, smallvec::smallvec![item])
    }
}

impl Counted for StandaloneProfile {
    fn quantities(&self) -> Quantities {
        self.profile.quantities()
    }
}

impl RateLimited for Managed<Box<StandaloneProfile>> {
    type Output = Self;
    type Error = Error;

    async fn enforce<R>(
        self,
        mut rate_limiter: R,
        _ctx: Context<'_>,
    ) -> Result<Self::Output, Rejected<Self::Error>>
    where
        R: RateLimiter,
    {
        let scoping = self.scoping();

        for (category, quantity) in self.profile.rate_limiting_quantities() {
            let limits = rate_limiter
                .try_consume(scoping.item(category), quantity)
                .await;

            if !limits.is_empty() {
                return Err(self.reject_err(Error::from(limits)));
            }
        }

        Ok(self)
    }
}
