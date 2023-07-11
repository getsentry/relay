use crate::types::Annotated;
use once_cell::sync::Lazy;
use std::collections::BTreeMap;
use std::fmt;

use crate::protocol::{Context, Contexts, DeviceContext};

#[derive(Debug, FromValue, IntoValue, Empty, Clone, PartialEq)]
pub struct DeviceClass(pub u64);

const GIB: u64 = 1024 * 1024 * 1024;

impl DeviceClass {
    pub const LOW: Self = Self(1);
    pub const MEDIUM: Self = Self(2);
    pub const HIGH: Self = Self(3);

    pub fn from_contexts(contexts: &Contexts) -> Option<DeviceClass> {
        if let Some(Context::Device(ref device)) =
            contexts.get_context(DeviceContext::default_key())
        {
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
        // iPhones
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
        // iPads
        ("iPad1,1", DeviceClass::LOW),
        ("iPad1,2", DeviceClass::LOW),
        ("iPad2,1", DeviceClass::LOW),
        ("iPad2,2", DeviceClass::LOW),
        ("iPad2,3", DeviceClass::LOW),
        ("iPad2,4", DeviceClass::LOW),
        ("iPad3,1", DeviceClass::LOW),
        ("iPad3,2", DeviceClass::LOW),
        ("iPad3,3", DeviceClass::LOW),
        ("iPad2,5", DeviceClass::LOW),
        ("iPad2,6", DeviceClass::LOW),
        ("iPad2,7", DeviceClass::LOW),
        ("iPad3,4", DeviceClass::LOW),
        ("iPad3,5", DeviceClass::LOW),
        ("iPad3,6", DeviceClass::LOW),
        ("iPad4,1", DeviceClass::LOW),
        ("iPad4,2", DeviceClass::LOW),
        ("iPad4,3", DeviceClass::LOW),
        ("iPad4,4", DeviceClass::LOW),
        ("iPad4,5", DeviceClass::LOW),
        ("iPad4,6", DeviceClass::LOW),
        ("iPad4,7", DeviceClass::LOW),
        ("iPad4,8", DeviceClass::LOW),
        ("iPad4,9", DeviceClass::LOW),
        ("iPad5,1", DeviceClass::LOW),
        ("iPad5,2", DeviceClass::LOW),
        ("iPad5,3", DeviceClass::LOW),
        ("iPad5,4", DeviceClass::LOW),
        ("iPad6,3", DeviceClass::MEDIUM),
        ("iPad6,4", DeviceClass::MEDIUM),
        ("iPad6,7", DeviceClass::MEDIUM),
        ("iPad6,8", DeviceClass::MEDIUM),
        ("iPad6,11", DeviceClass::LOW),
        ("iPad6,12", DeviceClass::LOW),
        ("iPad7,2", DeviceClass::MEDIUM),
        ("iPad7,3", DeviceClass::MEDIUM),
        ("iPad7,4", DeviceClass::MEDIUM),
        ("iPad7,5", DeviceClass::MEDIUM),
        ("iPad7,6", DeviceClass::MEDIUM),
        ("iPad7,1", DeviceClass::MEDIUM),
        ("iPad7,11", DeviceClass::MEDIUM),
        ("iPad7,12", DeviceClass::MEDIUM),
        ("iPad8,1", DeviceClass::MEDIUM),
        ("iPad8,2", DeviceClass::MEDIUM),
        ("iPad8,3", DeviceClass::MEDIUM),
        ("iPad8,4", DeviceClass::MEDIUM),
        ("iPad8,5", DeviceClass::MEDIUM),
        ("iPad8,6", DeviceClass::MEDIUM),
        ("iPad8,7", DeviceClass::MEDIUM),
        ("iPad8,8", DeviceClass::MEDIUM),
        ("iPad8,9", DeviceClass::MEDIUM),
        ("iPad8,10", DeviceClass::MEDIUM),
        ("iPad8,11", DeviceClass::MEDIUM),
        ("iPad8,12", DeviceClass::MEDIUM),
        ("iPad11,1", DeviceClass::MEDIUM),
        ("iPad11,2", DeviceClass::MEDIUM),
        ("iPad11,3", DeviceClass::MEDIUM),
        ("iPad11,4", DeviceClass::MEDIUM),
        ("iPad11,6", DeviceClass::MEDIUM),
        ("iPad11,7", DeviceClass::MEDIUM),
        ("iPad12,1", DeviceClass::MEDIUM),
        ("iPad12,2", DeviceClass::MEDIUM),
        ("iPad14,1", DeviceClass::HIGH),
        ("iPad14,2", DeviceClass::HIGH),
        ("iPad13,1", DeviceClass::HIGH),
        ("iPad13,2", DeviceClass::HIGH),
        ("iPad13,4", DeviceClass::HIGH),
        ("iPad13,5", DeviceClass::HIGH),
        ("iPad13,6", DeviceClass::HIGH),
        ("iPad13,7", DeviceClass::HIGH),
        ("iPad13,8", DeviceClass::HIGH),
        ("iPad13,9", DeviceClass::HIGH),
        ("iPad13,10", DeviceClass::HIGH),
        ("iPad13,11", DeviceClass::HIGH),
        ("iPad13,16", DeviceClass::HIGH),
        ("iPad13,17", DeviceClass::HIGH),
        ("iPad13,18", DeviceClass::HIGH),
        ("iPad13,19", DeviceClass::HIGH),
        ("iPad14,3", DeviceClass::HIGH),
        ("iPad14,4", DeviceClass::HIGH),
        ("iPad14,5", DeviceClass::HIGH),
        ("iPad14,6", DeviceClass::HIGH),
    ])
});
