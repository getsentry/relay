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
    let mut map = BTreeMap::new();
    map.insert("iPhone1,1", DeviceClass::LOW);
    map.insert("iPhone1,2", DeviceClass::LOW);
    map.insert("iPhone2,1", DeviceClass::LOW);
    map.insert("iPhone3,1", DeviceClass::LOW);
    map.insert("iPhone3,2", DeviceClass::LOW);
    map.insert("iPhone3,3", DeviceClass::LOW);
    map.insert("iPhone4,1", DeviceClass::LOW);
    map.insert("iPhone5,1", DeviceClass::LOW);
    map.insert("iPhone5,2", DeviceClass::LOW);
    map.insert("iPhone5,3", DeviceClass::LOW);
    map.insert("iPhone5,4", DeviceClass::LOW);
    map.insert("iPhone6,1", DeviceClass::LOW);
    map.insert("iPhone6,2", DeviceClass::LOW);
    map.insert("iPhone7,1", DeviceClass::LOW);
    map.insert("iPhone7,2", DeviceClass::LOW);
    map.insert("iPhone8,1", DeviceClass::LOW);
    map.insert("iPhone8,2", DeviceClass::LOW);
    map.insert("iPhone8,4", DeviceClass::LOW);
    map.insert("iPhone9,1", DeviceClass::MEDIUM);
    map.insert("iPhone9,3", DeviceClass::MEDIUM);
    map.insert("iPhone9,2", DeviceClass::MEDIUM);
    map.insert("iPhone9,4", DeviceClass::MEDIUM);
    map.insert("iPhone10,1", DeviceClass::MEDIUM);
    map.insert("iPhone10,4", DeviceClass::MEDIUM);
    map.insert("iPhone10,2", DeviceClass::MEDIUM);
    map.insert("iPhone10,5", DeviceClass::MEDIUM);
    map.insert("iPhone10,3", DeviceClass::MEDIUM);
    map.insert("iPhone10,6", DeviceClass::MEDIUM);
    map.insert("iPhone11,8", DeviceClass::MEDIUM);
    map.insert("iPhone11,2", DeviceClass::MEDIUM);
    map.insert("iPhone11,4", DeviceClass::MEDIUM);
    map.insert("iPhone11,6", DeviceClass::MEDIUM);
    map.insert("iPhone12,1", DeviceClass::MEDIUM);
    map.insert("iPhone12,3", DeviceClass::MEDIUM);
    map.insert("iPhone12,5", DeviceClass::MEDIUM);
    map.insert("iPhone12,8", DeviceClass::MEDIUM);
    map.insert("iPhone13,1", DeviceClass::HIGH);
    map.insert("iPhone13,2", DeviceClass::HIGH);
    map.insert("iPhone13,3", DeviceClass::HIGH);
    map.insert("iPhone13,4", DeviceClass::HIGH);
    map.insert("iPhone14,4", DeviceClass::HIGH);
    map.insert("iPhone14,5", DeviceClass::HIGH);
    map.insert("iPhone14,2", DeviceClass::HIGH);
    map.insert("iPhone14,3", DeviceClass::HIGH);
    map.insert("iPhone14,6", DeviceClass::HIGH);
    map.insert("iPhone14,7", DeviceClass::HIGH);
    map.insert("iPhone14,8", DeviceClass::HIGH);
    map.insert("iPhone15,2", DeviceClass::HIGH);
    map.insert("iPhone15,3", DeviceClass::HIGH);
    map
});
