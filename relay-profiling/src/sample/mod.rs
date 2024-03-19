use serde::{Deserialize, Serialize};

use crate::native_debug_image::NativeDebugImage;
use relay_event_schema::protocol::Addr;

pub mod v1;
pub mod v2;

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub enum Version {
    #[default]
    Unknown,
    #[serde(rename = "1")]
    V1,
    #[serde(rename = "2")]
    V2,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Frame {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abs_path: Option<String>,
    #[serde(alias = "column", skip_serializing_if = "Option::is_none")]
    pub colno: Option<u32>,
    #[serde(alias = "file", skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(alias = "name", skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_app: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instruction_addr: Option<Addr>,
    #[serde(alias = "line", skip_serializing_if = "Option::is_none")]
    pub lineno: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
}

impl Frame {
    pub fn strip_pointer_authentication_code(&mut self, pac_code: u64) {
        if let Some(address) = self.instruction_addr {
            self.instruction_addr = Some(Addr(address.0 & pac_code));
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize, Clone)]
pub struct DebugMeta {
    pub images: Vec<NativeDebugImage>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ThreadMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
}
