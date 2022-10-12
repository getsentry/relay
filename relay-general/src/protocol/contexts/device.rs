use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Serialize, Serializer};

use crate::processor::ProcessValue;
use crate::protocol::LenientString;
use crate::types::{
    Annotated, Empty, Error, FromValue, IntoValue, Object, SkipSerialization, Value,
};

/// Device information.
///
/// Device context describes the device that caused the event. This is most appropriate for mobile
/// applications.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct DeviceContext {
    /// Name of the device.
    #[metastructure(pii = "maybe")]
    pub name: Annotated<String>,

    /// Family of the device model.
    ///
    /// This is usually the common part of model names across generations. For instance, `iPhone`
    /// would be a reasonable family, so would be `Samsung Galaxy`.
    pub family: Annotated<String>,

    /// Device model.
    ///
    /// This, for example, can be `Samsung Galaxy S3`.
    pub model: Annotated<String>,

    /// Device model (internal identifier).
    ///
    /// An internal hardware revision to identify the device exactly.
    pub model_id: Annotated<String>,

    /// Native cpu architecture of the device.
    pub arch: Annotated<String>,

    /// Current battery level in %.
    ///
    /// If the device has a battery, this can be a floating point value defining the battery level
    /// (in the range 0-100).
    pub battery_level: Annotated<f64>,

    /// Current screen orientation.
    ///
    /// This can be a string `portrait` or `landscape` to define the orientation of a device.
    pub orientation: Annotated<String>,

    /// Manufacturer of the device.
    pub manufacturer: Annotated<String>,

    /// Brand of the device.
    pub brand: Annotated<String>,

    /// Device screen resolution.
    ///
    /// (e.g.: 800x600, 3040x1444)
    #[metastructure(pii = "maybe")]
    pub screen_resolution: Annotated<String>,

    /// Device screen density.
    #[metastructure(pii = "maybe")]
    pub screen_density: Annotated<f64>,

    /// Screen density as dots-per-inch.
    #[metastructure(pii = "maybe")]
    pub screen_dpi: Annotated<u64>,

    /// Whether the device was online or not.
    pub online: Annotated<bool>,

    /// Whether the device was charging or not.
    pub charging: Annotated<bool>,

    /// Whether the device was low on memory.
    pub low_memory: Annotated<bool>,

    /// Simulator/prod indicator.
    pub simulator: Annotated<bool>,

    /// Total memory available in bytes.
    #[metastructure(pii = "maybe")]
    pub memory_size: Annotated<u64>,

    /// How much memory is still available in bytes.
    #[metastructure(pii = "maybe")]
    pub free_memory: Annotated<u64>,

    /// How much memory is usable for the app in bytes.
    #[metastructure(pii = "maybe")]
    pub usable_memory: Annotated<u64>,

    /// Total storage size of the device in bytes.
    #[metastructure(pii = "maybe")]
    pub storage_size: Annotated<u64>,

    /// How much storage is free in bytes.
    #[metastructure(pii = "maybe")]
    pub free_storage: Annotated<u64>,

    /// Total size of the attached external storage in bytes (eg: android SDK card).
    #[metastructure(pii = "maybe")]
    pub external_storage_size: Annotated<u64>,

    /// Free size of the attached external storage in bytes (eg: android SDK card).
    #[metastructure(pii = "maybe")]
    pub external_free_storage: Annotated<u64>,

    /// Indicator when the device was booted.
    #[metastructure(pii = "maybe")]
    pub boot_time: Annotated<String>,

    /// Timezone of the device.
    #[metastructure(pii = "maybe")]
    pub timezone: Annotated<String>,

    /// Number of "logical processors".
    ///
    /// For example, 8.
    pub processor_count: Annotated<u64>,

    /// CPU description.
    ///
    /// For example, Intel(R) Core(TM)2 Quad CPU Q6600 @ 2.40GHz.
    #[metastructure(pii = "maybe")]
    pub cpu_description: Annotated<String>,

    /// Processor frequency in MHz.
    ///
    /// Note that the actual CPU frequency might vary depending on current load and
    /// power conditions, especially on low-powered devices like phones and laptops.
    pub processor_frequency: Annotated<u64>,

    /// Kind of device the application is running on.
    ///
    /// For example, `Unknown`, `Handheld`, `Console`, `Desktop`.
    #[metastructure(pii = "maybe")]
    pub device_type: Annotated<String>,

    /// Status of the device's battery.
    ///
    /// For example, `Unknown`, `Charging`, `Discharging`, `NotCharging`, `Full`.
    #[metastructure(pii = "maybe")]
    pub battery_status: Annotated<String>,

    /// Unique device identifier.
    #[metastructure(pii = "true")]
    pub device_unique_identifier: Annotated<String>,

    /// Whether vibration is available on the device.
    pub supports_vibration: Annotated<bool>,

    /// Whether the accelerometer is available on the device.
    pub supports_accelerometer: Annotated<bool>,

    /// Whether the gyroscope is available on the device.
    pub supports_gyroscope: Annotated<bool>,

    /// Whether audio is available on the device.
    pub supports_audio: Annotated<bool>,

    /// Whether location support is available on the device.
    pub supports_location_service: Annotated<bool>,

    /// Additional arbitrary fields for forwards compatibility
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl DeviceContext {
    /// The key under which a device context is generally stored (in `Contexts`)
    pub fn default_key() -> &'static str {
        "device"
    }
}

#[test]
fn test_device_context_roundtrip() {
    let json = r#"{
  "name": "iphone",
  "family": "iphone",
  "model": "iphone7,3",
  "model_id": "AH223",
  "arch": "arm64",
  "battery_level": 58.5,
  "orientation": "landscape",
  "manufacturer": "Apple",
  "brand": "iphone",
  "screen_resolution": "800x600",
  "screen_density": 1.1,
  "screen_dpi": 1,
  "online": true,
  "charging": false,
  "low_memory": false,
  "simulator": true,
  "memory_size": 3137978368,
  "free_memory": 322781184,
  "usable_memory": 2843525120,
  "storage_size": 63989469184,
  "free_storage": 31994734592,
  "external_storage_size": 2097152,
  "external_free_storage": 2097152,
  "boot_time": "2018-02-08T12:52:12Z",
  "timezone": "Europe/Vienna",
  "processor_count": 8,
  "cpu_description": "Intel(R) Core(TM)2 Quad CPU Q6600 @ 2.40GHz",
  "processor_frequency": 2400,
  "device_type": "Handheld",
  "battery_status": "Charging",
  "device_unique_identifier": "1234567",
  "supports_vibration": true,
  "supports_accelerometer": true,
  "supports_gyroscope": true,
  "supports_audio": true,
  "supports_location_service": true,
  "other": "value",
  "type": "device"
}"#;
    let context = Annotated::new(Context::Device(Box::new(DeviceContext {
        name: Annotated::new("iphone".to_string()),
        family: Annotated::new("iphone".to_string()),
        model: Annotated::new("iphone7,3".to_string()),
        model_id: Annotated::new("AH223".to_string()),
        arch: Annotated::new("arm64".to_string()),
        battery_level: Annotated::new(58.5),
        orientation: Annotated::new("landscape".to_string()),
        simulator: Annotated::new(true),
        manufacturer: Annotated::new("Apple".to_string()),
        brand: Annotated::new("iphone".to_string()),
        screen_resolution: Annotated::new("800x600".to_string()),
        screen_density: Annotated::new(1.1),
        screen_dpi: Annotated::new(1),
        online: Annotated::new(true),
        charging: Annotated::new(false),
        low_memory: Annotated::new(false),
        memory_size: Annotated::new(3_137_978_368),
        free_memory: Annotated::new(322_781_184),
        usable_memory: Annotated::new(2_843_525_120),
        storage_size: Annotated::new(63_989_469_184),
        free_storage: Annotated::new(31_994_734_592),
        external_storage_size: Annotated::new(2_097_152),
        external_free_storage: Annotated::new(2_097_152),
        boot_time: Annotated::new("2018-02-08T12:52:12Z".to_string()),
        timezone: Annotated::new("Europe/Vienna".to_string()),
        processor_count: Annotated::new(8),
        cpu_description: Annotated::new("Intel(R) Core(TM)2 Quad CPU Q6600 @ 2.40GHz".to_string()),
        processor_frequency: Annotated::new(2400),
        device_type: Annotated::new("Handheld".to_string()),
        battery_status: Annotated::new("Charging".to_string()),
        device_unique_identifier: Annotated::new("1234567".to_string()),
        supports_vibration: Annotated::new(true),
        supports_accelerometer: Annotated::new(true),
        supports_gyroscope: Annotated::new(true),
        supports_audio: Annotated::new(true),
        supports_location_service: Annotated::new(true),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    })));

    assert_eq!(context, Annotated::from_json(json).unwrap());
    assert_eq!(json, context.to_json_pretty().unwrap());
}
