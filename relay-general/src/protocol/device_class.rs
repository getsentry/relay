use crate::types::Annotated;
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
                        return device_model_to_class(model);
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

const LOW_PERFORMANCE_APPLE_DEVICES: [&str; 18] = [
    "iPhone1,1",
    "iPhone1,2",
    "iPhone2,1",
    "iPhone3,1",
    "iPhone3,2",
    "iPhone3,3",
    "iPhone4,1",
    "iPhone5,1",
    "iPhone5,2",
    "iPhone5,3",
    "iPhone5,4",
    "iPhone6,1",
    "iPhone6,2",
    "iPhone7,1",
    "iPhone7,2",
    "iPhone8,1",
    "iPhone8,2",
    "iPhone8,4",
];
const MEDIUM_PERFORMANCE_APPLE_DEVICES: [&str; 18] = [
    "iPhone9,1",
    "iPhone9,3",
    "iPhone9,2",
    "iPhone9,4",
    "iPhone10,1",
    "iPhone10,4",
    "iPhone10,2",
    "iPhone10,5",
    "iPhone10,3",
    "iPhone10,6",
    "iPhone11,8",
    "iPhone11,2",
    "iPhone11,4",
    "iPhone11,6",
    "iPhone12,1",
    "iPhone12,3",
    "iPhone12,5",
    "iPhone12,8",
];
const HIGH_PERFORMANCE_APPLE_DEVICES: [&str; 13] = [
    "iPhone13,1",
    "iPhone13,2",
    "iPhone13,3",
    "iPhone13,4",
    "iPhone14,4",
    "iPhone14,5",
    "iPhone14,2",
    "iPhone14,3",
    "iPhone14,6",
    "iPhone14,7",
    "iPhone14,8",
    "iPhone15,2",
    "iPhone15,3",
];

fn device_model_to_class(device_model: &str) -> Option<DeviceClass> {
    match device_model {
        device_model if HIGH_PERFORMANCE_APPLE_DEVICES.contains(&device_model) => {
            Some(DeviceClass::HIGH)
        }
        device_model if MEDIUM_PERFORMANCE_APPLE_DEVICES.contains(&device_model) => {
            Some(DeviceClass::MEDIUM)
        }
        device_model if LOW_PERFORMANCE_APPLE_DEVICES.contains(&device_model) => {
            Some(DeviceClass::LOW)
        }
        _ => None,
    }
}
