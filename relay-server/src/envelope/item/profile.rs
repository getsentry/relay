//! ProfileItem APIs and accounting.

use super::{ItemCount, ItemHeaderKey, ProfileItem};
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_profiling::ProfileType;
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl ProfileItem {
    /// Returns the associated platform.
    pub fn platform(&self) -> Option<&str> {
        self.data().headers.get(ItemHeaderKey::Platform)
    }

    /// Set the associated platform.
    pub fn set_platform(&mut self, platform: String) {
        self.data_mut()
            .headers
            .set(ItemHeaderKey::Platform, platform);
    }

    /// Infers the [`ProfileType`] from [`Self::platform`].
    pub fn profile_type(&self) -> Option<ProfileType> {
        self.platform().map(ProfileType::from_platform)
    }

    /// Gets the profile sampling flag.
    pub fn sampled(&self) -> bool {
        self.data()
            .headers
            .get(ItemHeaderKey::Sampled)
            .unwrap_or(true)
    }

    /// Sets the `sampled` flag.
    pub fn set_sampled(&mut self, sampled: bool) {
        self.data_mut().headers.set(ItemHeaderKey::Sampled, sampled);
    }
}

impl Counted for ProfileItem {
    fn quantities(&self) -> Quantities {
        let count = self.item_count().unwrap_or(1) as usize;
        let mut quantities = smallvec![
            (DataCategory::Profile, count),
            (DataCategory::ProfileIndexed, count),
        ];
        if let Some(profile_type) = self.profile_type() {
            let category = match profile_type {
                ProfileType::Backend => DataCategory::ProfileBackend,
                ProfileType::Ui => DataCategory::ProfileUi,
            };
            quantities.push((category, count));
        }
        quantities
    }
}

impl RejectionQuantities for ProfileItem {}
