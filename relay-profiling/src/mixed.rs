use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::ProfileError;
use crate::profile_metadata::ProfileMetadata;
use crate::sample;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AndroidProfile {
    platform: String,
    // Encoded Android Profile
    data: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SampleProfile {
    platform: String,
    data: sample::Profile,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
enum Profile {
    Android(AndroidProfile),
    Sample(SampleProfile),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MixedProfile {
    #[serde(flatten)]
    metadata: ProfileMetadata,
    profiles: Vec<Profile>,
}

pub fn parse_mixed_profile(
    _payload: &[u8],
    _transaction_metadata: BTreeMap<String, String>,
    _transaction_tags: BTreeMap<String, String>,
) -> Result<Vec<u8>, ProfileError> {
    // TODO: Implement this function.
    return Err(ProfileError::CannotSerializePayload);
}
