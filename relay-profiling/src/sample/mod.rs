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

/// Holds information about a single stacktrace frame.
///
/// Each object should contain **at least** a `filename`, `function` or `instruction_addr`
/// attribute. All values are optional, but recommended.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Frame {
    /// Absolute path to the source file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub abs_path: Option<String>,

    /// Column number within the source file, starting at 1.
    #[serde(alias = "column", skip_serializing_if = "Option::is_none")]
    pub colno: Option<u32>,

    /// The source file name (basename only).
    #[serde(alias = "file", skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,

    /// Name of the frame's function. This might include the name of a class.
    ///
    /// This function name may be shortened or demangled. If not, Sentry will demangle and shorten
    /// it for some platforms. The original function name will be stored in `raw_function`.
    #[serde(alias = "name", skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,

    /// Override whether this frame should be considered part of application code, or part of
    /// libraries/frameworks/dependencies.
    ///
    /// Setting this attribute to `false` causes the frame to be hidden/collapsed by default and
    /// mostly ignored during issue grouping.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_app: Option<bool>,

    /// (C/C++/Native) An optional instruction address for symbolication.
    ///
    /// This should be a string with a hexadecimal number that includes a 0x prefix.
    /// If this is set and a known image is defined in the
    /// [Debug Meta Interface]({%- link _documentation/development/sdk-dev/event-payloads/debugmeta.md -%}),
    /// then symbolication can take place.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instruction_addr: Option<Addr>,

    /// Line number within the source file, starting at 1.
    #[serde(alias = "line", skip_serializing_if = "Option::is_none")]
    pub lineno: Option<u32>,

    /// Name of the module the frame is contained in.
    ///
    /// Note that this might also include a class name if that is something the
    /// language natively considers to be part of the stack (for instance in Java).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,

    /// Which platform this frame is from.
    ///
    /// This can override the platform for a single frame. Otherwise, the platform of the event is
    /// assumed. This can be used for multi-platform stack traces, such as in React Native.
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
