use chrono::{DateTime, Utc};
use relay_event_schema::protocol::EventId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

use crate::measurements::Measurement;
use crate::native_debug_image::NativeDebugImage;
use crate::profile_version::Version;
use crate::transaction_metadata::TransactionMetadata;

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct DebugMeta {
    pub images: Vec<NativeDebugImage>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OSMetadata {
    pub name: String,
    pub version: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_number: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RuntimeMetadata {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DeviceMetadata {
    pub architecture: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_emulator: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manufacturer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ProfileMetadata {
    pub version: Version,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug_meta: Option<DebugMeta>,

    pub device: DeviceMetadata,
    pub os: OSMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeMetadata>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub environment: String,
    #[serde(alias = "profile_id")]
    pub event_id: EventId,
    pub platform: String,
    pub timestamp: DateTime<Utc>,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub release: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub dist: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transactions: Vec<TransactionMetadata>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TransactionMetadata>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurements: Option<HashMap<String, Measurement>>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub transaction_metadata: BTreeMap<String, String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub transaction_tags: BTreeMap<String, String>,
}
