use android_trace_log::AndroidTraceLog;
use serde::{Deserialize, Serialize};

use relay_general::protocol::EventId;

use crate::transaction_metadata::TransactionMetadata;
use crate::utils::{deserialize_number_from_string, is_zero};
use crate::ProfileError;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AndroidProfile {
    android_api_level: u16,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    build_id: String,

    device_cpu_frequencies: Vec<u32>,
    device_is_emulator: bool,
    device_locale: String,
    device_manufacturer: String,
    device_model: String,
    device_os_name: String,
    device_os_version: String,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    device_physical_memory_bytes: u64,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    environment: String,

    platform: String,
    profile_id: EventId,
    version_code: String,
    version_name: String,

    #[serde(default, skip_serializing_if = "EventId::is_nil")]
    transaction_id: EventId,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    transaction_name: String,
    #[serde(default, skip_serializing_if = "EventId::is_nil")]
    trace_id: EventId,
    #[serde(
        default,
        deserialize_with = "deserialize_number_from_string",
        skip_serializing_if = "is_zero"
    )]
    duration_ns: u64,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    transactions: Vec<TransactionMetadata>,

    #[serde(default, skip_serializing)]
    sampled_profile: String,

    #[serde(default)]
    profile: Option<AndroidTraceLog>,
}

impl AndroidProfile {
    fn parse(&mut self) -> Result<(), ProfileError> {
        let profile_bytes = match base64::decode(&self.sampled_profile) {
            Ok(profile) => profile,
            Err(_) => return Err(ProfileError::InvalidBase64Value),
        };
        self.profile = match android_trace_log::parse(&profile_bytes) {
            Ok(profile) => Some(profile),
            Err(_) => return Err(ProfileError::InvalidSampledProfile),
        };
        Ok(())
    }

    fn set_transaction(&mut self, transaction: TransactionMetadata) {
        self.transaction_name = transaction.name;
        self.transaction_id = transaction.id;
        self.trace_id = transaction.trace_id;
        self.duration_ns = transaction.relative_end_ns - transaction.relative_start_ns;
    }

    fn has_transaction_metadata(&self) -> bool {
        !self.transaction_name.is_empty() && self.duration_ns > 0
    }
}

pub fn expand_android_profile(payload: &[u8]) -> Result<Vec<Vec<u8>>, ProfileError> {
    let profile = parse_android_profile(payload)?;
    let mut items: Vec<Vec<u8>> = Vec::new();

    if profile.transactions.is_empty() && profile.has_transaction_metadata() {
        match serde_json::to_vec(&profile) {
            Ok(payload) => items.push(payload),
            Err(_) => {
                return Err(ProfileError::CannotSerializePayload);
            }
        };

        return Ok(items);
    }

    for transaction in &profile.transactions {
        let mut new_profile = profile.clone();
        new_profile.set_transaction(transaction.clone());
        new_profile.transactions.clear();
        match serde_json::to_vec(&new_profile) {
            Ok(payload) => items.push(payload),
            Err(_) => {
                return Err(ProfileError::CannotSerializePayload);
            }
        };
    }
    Ok(items)
}

fn parse_android_profile(payload: &[u8]) -> Result<AndroidProfile, ProfileError> {
    let mut profile: AndroidProfile =
        serde_json::from_slice(payload).map_err(ProfileError::InvalidJson)?;

    if profile.sampled_profile.is_empty() {
        return Ok(profile);
    }

    if profile.transactions.is_empty() && !profile.has_transaction_metadata() {
        return Err(ProfileError::NoTransactionAssociated);
    }

    for transaction in &profile.transactions {
        if !transaction.clone().valid() {
            return Err(ProfileError::InvalidTransactionMetadata);
        }
    }

    profile.parse()?;

    if profile.profile.as_ref().unwrap().events.len() < 2 {
        return Err(ProfileError::NotEnoughSamples);
    }

    Ok(profile)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_android() {
        let payload = include_bytes!("../tests/fixtures/profiles/android/valid.json");
        let profile = parse_android_profile(payload);
        assert!(profile.is_ok());
        let data = serde_json::to_vec(&profile.unwrap());
        assert!(parse_android_profile(&(data.unwrap())[..]).is_ok());
    }

    #[test]
    fn test_multiple_transactions() {
        let payload =
            include_bytes!("../tests/fixtures/profiles/android/multiple_transactions.json");
        let data = parse_android_profile(payload);
        assert!(data.is_ok());
    }

    #[test]
    fn test_no_transaction() {
        let payload = include_bytes!("../tests/fixtures/profiles/android/no_transaction.json");
        let data = parse_android_profile(payload);
        assert!(data.is_err());
    }
}
