use std::sync::Arc;

use relay_profiling::{ProfileError, ProfileType};
use relay_quotas::{DataCategory, RateLimits};
use smallvec::smallvec;

use crate::envelope::{EnvelopeHeaders, Item, ItemType, Items};
use crate::managed::{
    Counted, Managed, ManagedEnvelope, ManagedResult, OutcomeError, Quantities, Rejected,
};
use crate::processing::{Context, CountRateLimited, Output, Processor, QuotaRateLimiter};
use crate::services::outcome::{DiscardReason, Outcome};
use crate::statsd::RelayCounters;

mod filter;
mod forward;
mod process;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Profiles filtered because of a missing feature flag.
    #[error("profile feature flag missing")]
    FeatureDisabled,

    /// No profiles to expand.
    ///
    /// Internal error that should never happen if `prepare_envelope` works correctly.
    #[error("no profiles")]
    NoProfiles,

    /// The profile is invalid.
    ///
    /// See [`ProfileError`] for specific reasons.
    #[error(transparent)]
    InvalidProfile(#[from] ProfileError),

    /// The profile was rate limited.
    #[error("rate limited")]
    RateLimited(RateLimits),
}

impl OutcomeError for Error {
    type Error = Error;

    fn consume(self) -> (Option<Outcome>, Self::Error) {
        let outcome = match &self {
            Error::FeatureDisabled => None,
            Error::NoProfiles => Some(Outcome::Invalid(DiscardReason::Internal)),
            Error::InvalidProfile(err) => Some(Outcome::Invalid(DiscardReason::Profiling(
                relay_profiling::discard_reason(err),
            ))),
            Error::RateLimited(limits) => {
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

/// A processor for profiles.
#[derive(Debug)]
pub struct ProfilesProcessor {
    limiter: Arc<QuotaRateLimiter>,
}

impl ProfilesProcessor {
    /// Creates a new [`Self`].
    pub fn new(limiter: Arc<QuotaRateLimiter>) -> Self {
        Self { limiter }
    }
}

impl Processor for ProfilesProcessor {
    type Input = SerializedProfiles;
    type Output = ProfilesOutput;
    type Error = Error;

    fn prepare_envelope(&self, envelope: &mut ManagedEnvelope) -> Option<Managed<Self::Input>> {
        let headers = envelope.envelope().headers().clone();
        let profiles = envelope
            .envelope_mut()
            .take_items_by(|item| matches!(item.ty(), ItemType::Profile));

        if profiles.is_empty() {
            return None;
        }

        let profiles = SerializedProfiles { headers, profiles };
        Some(Managed::with_meta_from(envelope, profiles))
    }

    async fn process(
        &self,
        profiles: Managed<Self::Input>,
        ctx: Context<'_>,
    ) -> Result<Output<Self::Output>, Rejected<Self::Error>> {
        relay_statsd::metric!(
            counter(RelayCounters::StandaloneItem) += profiles.profiles.len() as u64,
            processor = "new",
            item_type = "profile",
        );

        filter::feature_flag(ctx).reject(&profiles)?;

        let profile = process::expand(profiles)?;

        let profile = self.limiter.enforce_quotas(profile, ctx).await?;

        Ok(Output::just(ProfilesOutput(profile)))
    }
}

/// Serialized profiles extracted from an envelope.
#[derive(Debug)]
pub struct SerializedProfiles {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// A list of profiles.
    profiles: Items,
}

impl Counted for SerializedProfiles {
    fn quantities(&self) -> Quantities {
        self.profiles.quantities()
    }
}

/// A profile after extracting metadata.
#[derive(Debug)]
pub struct ExpandedProfile {
    /// Original envelope headers.
    headers: EnvelopeHeaders,
    /// The profile received in an envelope.
    profile: Item,
    /// The type of the profile, inferred from the platform.
    profile_type: ProfileType,
}

impl Counted for ExpandedProfile {
    fn quantities(&self) -> Quantities {
        smallvec![
            (DataCategory::Profile, 1),
            (DataCategory::ProfileIndexed, 1),
            (
                match self.profile_type {
                    ProfileType::Backend => DataCategory::ProfileBackend,
                    ProfileType::Ui => DataCategory::ProfileUi,
                },
                1
            ),
        ]
    }
}

impl CountRateLimited for Managed<ExpandedProfile> {
    type Error = Error;
}

/// Output produced by the [`ProfilesProcessor`].
#[derive(Debug)]
pub struct ProfilesOutput(Managed<ExpandedProfile>);
