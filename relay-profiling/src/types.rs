use serde::{Deserialize, Serialize};

use crate::native_debug_image::NativeDebugImage;

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct DebugMeta {
    images: Vec<NativeDebugImage>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientSdk {
    pub name: String,
    pub version: String,
}
