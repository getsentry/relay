//! ProfileChunkItem APIs and accounting.

use super::{ItemCount, ItemHeaderKey, ProfileChunkItem};
use crate::envelope::RejectionQuantities;
use crate::managed::{Counted, Quantities};
use relay_profiling::ProfileType;
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl ProfileChunkItem {
    /// Returns the associated platform.
    pub fn platform(&self) -> Option<&str> {
        self.data().headers().get(ItemHeaderKey::Platform)
    }

    /// Set the associated platform.
    pub fn set_platform(&mut self, platform: String) {
        self.data_mut()
            .headers_mut()
            .set(ItemHeaderKey::Platform, platform);
    }

    /// Infers the [`ProfileType`] from [`Self::platform`].
    pub fn profile_type(&self) -> Option<ProfileType> {
        self.platform().map(ProfileType::from_platform)
    }

    /// Returns the length of the metadata segment for a Perfetto profile chunk.
    pub fn meta_length(&self) -> Option<u32> {
        self.data().headers().get(ItemHeaderKey::MetaLength)
    }
}

impl Counted for ProfileChunkItem {
    fn quantities(&self) -> Quantities {
        let category = match self.profile_type() {
            Some(ProfileType::Backend) => DataCategory::ProfileChunk,
            Some(ProfileType::Ui) => DataCategory::ProfileChunkUi,
            None => return Quantities::new(),
        };
        smallvec![(category, self.item_count().unwrap_or(1) as usize)]
    }
}

impl RejectionQuantities for ProfileChunkItem {}
