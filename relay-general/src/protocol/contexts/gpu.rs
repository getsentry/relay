use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::LenientString;
use crate::types::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};

/// GPU information.
///
/// Example:
///
/// ```json
/// "gpu": {
///   "name": "AMD Radeon Pro 560",
///   "vendor_name": "Apple",
///   "memory_size": 4096,
///   "api_type": "Metal",
///   "multi_threaded_rendering": true,
///   "version": "Metal",
///   "npot_support": "Full"
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct GpuContext {
    /// The name of the graphics device.
    #[metastructure(pii = "maybe")]
    pub name: Annotated<String>,

    /// The Version of the graphics device.
    #[metastructure(pii = "maybe")]
    pub version: Annotated<String>,

    /// The PCI identifier of the graphics device.
    #[metastructure(pii = "maybe")]
    pub id: Annotated<Value>,

    /// The PCI vendor identifier of the graphics device.
    #[metastructure(pii = "maybe")]
    pub vendor_id: Annotated<String>,

    /// The vendor name as reported by the graphics device.
    #[metastructure(pii = "maybe")]
    pub vendor_name: Annotated<String>,

    /// The total GPU memory available in Megabytes.
    #[metastructure(pii = "maybe")]
    pub memory_size: Annotated<u64>,

    /// The device low-level API type.
    ///
    /// Examples: `"Apple Metal"` or `"Direct3D11"`
    #[metastructure(pii = "maybe")]
    pub api_type: Annotated<String>,

    /// Whether the GPU has multi-threaded rendering or not.
    #[metastructure(pii = "maybe")]
    pub multi_threaded_rendering: Annotated<bool>,

    /// The Non-Power-Of-Two support.
    #[metastructure(pii = "maybe")]
    pub npot_support: Annotated<String>,

    /// Largest size of a texture that is supported by the graphics hardware.
    ///
    /// For Example: 16384
    pub max_texture_size: Annotated<u64>,

    /// Approximate "shader capability" level of the graphics device.
    ///
    /// For Example: Shader Model 2.0, OpenGL ES 3.0, Metal / OpenGL ES 3.1, 27 (unknown)
    pub graphics_shader_level: Annotated<String>,

    /// Whether GPU draw call instancing is supported.
    pub supports_draw_call_instancing: Annotated<bool>,

    /// Whether ray tracing is available on the device.
    pub supports_ray_tracing: Annotated<bool>,

    /// Whether compute shaders are available on the device.
    pub supports_compute_shaders: Annotated<bool>,

    /// Whether geometry shaders are available on the device.
    pub supports_geometry_shaders: Annotated<bool>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl GpuContext {
    /// The key under which a runtime context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "gpu"
    }
}
