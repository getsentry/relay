use crate::types::Annotated;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::fmt;

use crate::protocol::{Context, Contexts};

#[derive(Debug, FromValue, IntoValue, Empty, Clone, PartialEq)]
pub struct DeviceClass(pub u64);

const GIB: u64 = 1024 * 1024 * 1024;

impl DeviceClass {
    pub const LOW: Self = Self(1);
    pub const MEDIUM: Self = Self(2);
    pub const HIGH: Self = Self(3);

    pub fn from_contexts(contexts: &Contexts) -> Option<DeviceClass> {
        if let Some(Context::Device(ref device)) = contexts.get_context("device") {
            if let Some(family) = device.family.value() {
                if family == "iPhone" || family == "iOS" || family == "iOS-Device" {
                    if let Some(model) = device.model.value() {
                        return APPLE_DEVICE_MODEL_TO_CLASS_MAP.get(model.as_str()).cloned();
                    }
                    return None;
                } else if let (Some(&freq), Some(&proc), Some(&mem)) = (
                    device.processor_frequency.value(),
                    device.processor_count.value(),
                    device.memory_size.value(),
                ) {
                    if freq < 2000 || proc < 8 || mem < 4 * GIB {
                        return Some(DeviceClass::LOW);
                    } else if freq < 2500 || mem < 6 * GIB {
                        return Some(DeviceClass::MEDIUM);
                    } else {
                        return Some(DeviceClass::HIGH);
                    }
                }
            }
        }
        None
    }
}

impl fmt::Display for DeviceClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.to_string().fmt(f)
    }
}

static APPLE_DEVICE_MODEL_TO_CLASS_MAP: Lazy<BTreeMap<&str, DeviceClass>> = Lazy::new(|| {
    BTreeMap::from([
        ("iPhone1,1", DeviceClass::LOW),
        ("iPhone1,2", DeviceClass::LOW),
        ("iPhone2,1", DeviceClass::LOW),
        ("iPhone3,1", DeviceClass::LOW),
        ("iPhone3,2", DeviceClass::LOW),
        ("iPhone3,3", DeviceClass::LOW),
        ("iPhone4,1", DeviceClass::LOW),
        ("iPhone5,1", DeviceClass::LOW),
        ("iPhone5,2", DeviceClass::LOW),
        ("iPhone5,3", DeviceClass::LOW),
        ("iPhone5,4", DeviceClass::LOW),
        ("iPhone6,1", DeviceClass::LOW),
        ("iPhone6,2", DeviceClass::LOW),
        ("iPhone7,1", DeviceClass::LOW),
        ("iPhone7,2", DeviceClass::LOW),
        ("iPhone8,1", DeviceClass::LOW),
        ("iPhone8,2", DeviceClass::LOW),
        ("iPhone8,4", DeviceClass::LOW),
        ("iPhone9,1", DeviceClass::MEDIUM),
        ("iPhone9,3", DeviceClass::MEDIUM),
        ("iPhone9,2", DeviceClass::MEDIUM),
        ("iPhone9,4", DeviceClass::MEDIUM),
        ("iPhone10,1", DeviceClass::MEDIUM),
        ("iPhone10,4", DeviceClass::MEDIUM),
        ("iPhone10,2", DeviceClass::MEDIUM),
        ("iPhone10,5", DeviceClass::MEDIUM),
        ("iPhone10,3", DeviceClass::MEDIUM),
        ("iPhone10,6", DeviceClass::MEDIUM),
        ("iPhone11,8", DeviceClass::MEDIUM),
        ("iPhone11,2", DeviceClass::MEDIUM),
        ("iPhone11,4", DeviceClass::MEDIUM),
        ("iPhone11,6", DeviceClass::MEDIUM),
        ("iPhone12,1", DeviceClass::MEDIUM),
        ("iPhone12,3", DeviceClass::MEDIUM),
        ("iPhone12,5", DeviceClass::MEDIUM),
        ("iPhone12,8", DeviceClass::MEDIUM),
        ("iPhone13,1", DeviceClass::HIGH),
        ("iPhone13,2", DeviceClass::HIGH),
        ("iPhone13,3", DeviceClass::HIGH),
        ("iPhone13,4", DeviceClass::HIGH),
        ("iPhone14,4", DeviceClass::HIGH),
        ("iPhone14,5", DeviceClass::HIGH),
        ("iPhone14,2", DeviceClass::HIGH),
        ("iPhone14,3", DeviceClass::HIGH),
        ("iPhone14,6", DeviceClass::HIGH),
        ("iPhone14,7", DeviceClass::HIGH),
        ("iPhone14,8", DeviceClass::HIGH),
        ("iPhone15,2", DeviceClass::HIGH),
        ("iPhone15,3", DeviceClass::HIGH),
    ])
});
