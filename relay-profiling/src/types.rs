use serde::{Deserialize, Serialize};

use crate::native_debug_image::NativeDebugImage;

/// This is a serde-friendly version of https://github.com/getsentry/relay/blob/52bc345871b4e5cca19ed73c17730eeef092028b/relay-event-schema/src/protocol/debugmeta.rs#L516
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct DebugMeta {
    images: Vec<NativeDebugImage>,
}

/// This is a serde-friendly version of https://github.com/getsentry/relay/blob/52bc345871b4e5cca19ed73c17730eeef092028b/relay-event-schema/src/protocol/clientsdk.rs#L8
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientSdk {
    pub name: String,
    pub version: String,
}
