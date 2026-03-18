use smallvec::smallvec;

use crate::Envelope;
#[cfg(feature = "processing")]
use crate::managed::Counted;
use crate::managed::{Managed, Rejected};
use crate::processing::profiles::{ExpandedProfile, ProfilesOutput};
use crate::processing::{Forward, ForwardContext};
#[cfg(feature = "processing")]
use crate::services::store::StoreProfile;

impl Forward for ProfilesOutput {
    fn serialize_envelope(
        self,
        _ctx: ForwardContext<'_>,
    ) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
        let Self(profile) = self;
        let envelope = profile.map(
            |ExpandedProfile {
                 headers,
                 profile,
                 profile_type: _,
             },
             _| { Envelope::from_parts(headers, smallvec![profile]) },
        );

        Ok(envelope)
    }

    #[cfg(feature = "processing")]
    fn forward_store(
        self,
        s: crate::processing::StoreHandle<'_>,
        ctx: ForwardContext<'_>,
    ) -> Result<(), Rejected<()>> {
        let Self(profile) = self;

        let store_profile = profile.map(|profile, _| convert(profile, &ctx));
        s.send_to_store(store_profile);

        Ok(())
    }
}

/// Converts a [`ExpandedProfile`] into a [`StoreProfile`].
#[cfg(feature = "processing")]
fn convert(profile: ExpandedProfile, ctx: &ForwardContext) -> StoreProfile {
    let retention_days = ctx.event_retention().standard;
    let quantities = profile.quantities();

    StoreProfile {
        retention_days,
        profile: profile.profile,
        quantities,
    }
}
