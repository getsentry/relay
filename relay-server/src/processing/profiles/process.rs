use relay_profiling::{ProfileError, ProfileType};
use relay_quotas::DataCategory;

use crate::managed::{Managed, RecordKeeper, Rejected};
use crate::processing::profiles::{Error, ExpandedProfile, SerializedProfiles};
use crate::services::outcome::{DiscardReason, Outcome};

/// Parses serialized profiles into the [`ExpandedProfile`] representation.
///
/// If there are multiple profiles, rejects all except the first.
pub fn expand(
    profiles: Managed<SerializedProfiles>,
) -> Result<Managed<ExpandedProfile>, Rejected<Error>> {
    profiles.try_map(expand_profile)
}

fn expand_profile(
    profiles: SerializedProfiles,
    record_keeper: &mut RecordKeeper<'_>,
) -> Result<ExpandedProfile, Error> {
    let SerializedProfiles { headers, profiles } = profiles;
    let mut profiles = profiles.into_iter();

    // Accept at most one profile:
    let mut profile = profiles.next().ok_or(Error::NoProfiles)?;
    for additional_profile in profiles {
        record_keeper.reject_err(
            Outcome::Invalid(DiscardReason::Profiling(relay_profiling::discard_reason(
                &ProfileError::TooManyProfiles,
            ))),
            additional_profile,
        );
    }

    if profile.sampled() {
        return Err(ProfileError::InvalidStandalone.into());
    }

    let meta = relay_profiling::parse_metadata(&profile.payload())?;
    let profile_type = meta.profile_type();

    // If the profile type is new information, we now count the profile in an additional data category.
    if profile.profile_type().is_none() {
        record_keeper.modify_by(
            match profile_type {
                ProfileType::Backend => DataCategory::ProfileBackend,
                ProfileType::Ui => DataCategory::ProfileUi,
            },
            1,
        );
    }
    profile.set_platform(meta.platform);

    Ok(ExpandedProfile {
        headers,
        profile,
        profile_type,
    })
}
