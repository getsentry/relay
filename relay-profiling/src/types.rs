use serde::{Deserialize, Serialize};

use crate::debug_image::DebugImage;

/// This is a serde-friendly version of <https://github.com/getsentry/relay/blob/52bc345871b4e5cca19ed73c17730eeef092028b/relay-event-schema/src/protocol/debugmeta.rs#L516>
#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct DebugMeta {
    pub(crate) images: Vec<DebugImage>,
}

/// This is a serde-friendly version of <https://github.com/getsentry/relay/blob/52bc345871b4e5cca19ed73c17730eeef092028b/relay-event-schema/src/protocol/clientsdk.rs#L8>
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientSdk {
    pub name: String,

    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
}
