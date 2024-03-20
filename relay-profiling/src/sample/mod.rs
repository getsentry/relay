use serde::{Deserialize, Serialize};

use crate::native_debug_image::NativeDebugImage;
use relay_event_schema::protocol::Addr;

pub mod v1;
pub mod v2;

/// Possible values for the version field of the Sample Format.
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
    /// `abs_path` contains the absolute path the function is called.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abs_path: Option<String>,
    /// `colno` contains the column number of where the function is called.
    #[serde(alias = "column", skip_serializing_if = "Option::is_none")]
    pub colno: Option<u32>,
    #[serde(alias = "file", skip_serializing_if = "Option::is_none")]
    /// `filename` contains the file name only where the function is called.
    pub filename: Option<String>,
    #[serde(alias = "name", skip_serializing_if = "Option::is_none")]
    /// `function` contains the function's name that was called.
    pub function: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `in_app` indicates if the function is from the user application or a third-party/system
    /// library.
    pub in_app: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `instruction_addr` contains the address in memory where the function is called.
    pub instruction_addr: Option<Addr>,
    #[serde(alias = "line", skip_serializing_if = "Option::is_none")]
    /// `lineno` contains the line number of the file where the function is called.
    pub lineno: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `module` contains the package or module name of the function.
    pub module: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `platform` allows you to customize the platform for this frame. For example, in the case of
    /// hybrid profiles (`cocoa` and `javascript`), you would be allowed to pass a JavaScript frame
    /// even though the profile has `cocoa` for the platform.
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
    /// A list of debug files needed to symbolicate/deobfuscate this profile.
    /// Useful to pass source maps, ProGuard files or image libraries.
    pub images: Vec<NativeDebugImage>,
}

impl DebugMeta {
    pub fn is_empty(&self) -> bool {
        self.images.is_empty()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ThreadMetadata {
    /// This contains the name of the thread or queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// This contains the given priority of a thread if needed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
}
