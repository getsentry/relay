use std::fmt;

use relay_protocol::{Annotated, Empty, FromValue, IntoValue};

use crate::protocol::{Contexts, DeviceContext};

#[derive(Clone, Copy, Debug, FromValue, IntoValue, Empty, PartialEq)]
pub struct DeviceClass(pub u64);

const GIB: u64 = 1024 * 1024 * 1024;

impl DeviceClass {
    pub const LOW: Self = Self(1);
    pub const MEDIUM: Self = Self(2);
    pub const HIGH: Self = Self(3);

    pub fn from_contexts(contexts: &Contexts) -> Option<DeviceClass> {
        let device = contexts.get::<DeviceContext>()?;
        let family = device.family.value()?;

        if family == "iPhone" || family == "iOS" || family == "iOS-Device" {
            model_to_class(device.model.as_str()?)
        } else if let (Some(&freq), Some(&proc), Some(&mem)) = (
            device.processor_frequency.value(),
            device.processor_count.value(),
            device.memory_size.value(),
        ) {
            if freq < 2000 || proc < 8 || mem < 4 * GIB {
                Some(DeviceClass::LOW)
            } else if freq < 2500 || mem < 6 * GIB {
                Some(DeviceClass::MEDIUM)
            } else {
                Some(DeviceClass::HIGH)
            }
        } else {
            None
        }
    }
}

impl fmt::Display for DeviceClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

fn model_to_class(model: &str) -> Option<DeviceClass> {
    match model {
        // iPhones
        "iPhone1,1" => Some(DeviceClass::LOW),
        "iPhone1,2" => Some(DeviceClass::LOW),
        "iPhone2,1" => Some(DeviceClass::LOW),
        "iPhone3,1" => Some(DeviceClass::LOW),
        "iPhone3,2" => Some(DeviceClass::LOW),
        "iPhone3,3" => Some(DeviceClass::LOW),
        "iPhone4,1" => Some(DeviceClass::LOW),
        "iPhone5,1" => Some(DeviceClass::LOW),
        "iPhone5,2" => Some(DeviceClass::LOW),
        "iPhone5,3" => Some(DeviceClass::LOW),
        "iPhone5,4" => Some(DeviceClass::LOW),
        "iPhone6,1" => Some(DeviceClass::LOW),
        "iPhone6,2" => Some(DeviceClass::LOW),
        "iPhone7,1" => Some(DeviceClass::LOW),
        "iPhone7,2" => Some(DeviceClass::LOW),
        "iPhone8,1" => Some(DeviceClass::LOW),
        "iPhone8,2" => Some(DeviceClass::LOW),
        "iPhone8,4" => Some(DeviceClass::LOW),
        "iPhone9,1" => Some(DeviceClass::MEDIUM),
        "iPhone9,3" => Some(DeviceClass::MEDIUM),
        "iPhone9,2" => Some(DeviceClass::MEDIUM),
        "iPhone9,4" => Some(DeviceClass::MEDIUM),
        "iPhone10,1" => Some(DeviceClass::MEDIUM),
        "iPhone10,4" => Some(DeviceClass::MEDIUM),
        "iPhone10,2" => Some(DeviceClass::MEDIUM),
        "iPhone10,5" => Some(DeviceClass::MEDIUM),
        "iPhone10,3" => Some(DeviceClass::MEDIUM),
        "iPhone10,6" => Some(DeviceClass::MEDIUM),
        "iPhone11,8" => Some(DeviceClass::MEDIUM),
        "iPhone11,2" => Some(DeviceClass::MEDIUM),
        "iPhone11,4" => Some(DeviceClass::MEDIUM),
        "iPhone11,6" => Some(DeviceClass::MEDIUM),
        "iPhone12,1" => Some(DeviceClass::MEDIUM),
        "iPhone12,3" => Some(DeviceClass::MEDIUM),
        "iPhone12,5" => Some(DeviceClass::MEDIUM),
        "iPhone12,8" => Some(DeviceClass::MEDIUM),
        "iPhone13,1" => Some(DeviceClass::HIGH),
        "iPhone13,2" => Some(DeviceClass::HIGH),
        "iPhone13,3" => Some(DeviceClass::HIGH),
        "iPhone13,4" => Some(DeviceClass::HIGH),
        "iPhone14,4" => Some(DeviceClass::HIGH),
        "iPhone14,5" => Some(DeviceClass::HIGH),
        "iPhone14,2" => Some(DeviceClass::HIGH),
        "iPhone14,3" => Some(DeviceClass::HIGH),
        "iPhone14,6" => Some(DeviceClass::HIGH),
        "iPhone14,7" => Some(DeviceClass::HIGH),
        "iPhone14,8" => Some(DeviceClass::HIGH),
        "iPhone15,2" => Some(DeviceClass::HIGH),
        "iPhone15,3" => Some(DeviceClass::HIGH),
        "iPhone15,4" => Some(DeviceClass::HIGH),
        "iPhone15,5" => Some(DeviceClass::HIGH),
        "iPhone16,1" => Some(DeviceClass::HIGH),
        "iPhone16,2" => Some(DeviceClass::HIGH),
        "iPhone17,1" => Some(DeviceClass::HIGH),
        "iPhone17,2" => Some(DeviceClass::HIGH),
        "iPhone17,3" => Some(DeviceClass::HIGH),
        "iPhone17,4" => Some(DeviceClass::HIGH),
        "iPhone17,5" => Some(DeviceClass::HIGH),
        "iPhone18,1" => Some(DeviceClass::HIGH),
        "iPhone18,2" => Some(DeviceClass::HIGH),
        "iPhone18,3" => Some(DeviceClass::HIGH),
        "iPhone18,4" => Some(DeviceClass::HIGH),

        // iPads
        "iPad1,1" => Some(DeviceClass::LOW),
        "iPad1,2" => Some(DeviceClass::LOW),
        "iPad2,1" => Some(DeviceClass::LOW),
        "iPad2,2" => Some(DeviceClass::LOW),
        "iPad2,3" => Some(DeviceClass::LOW),
        "iPad2,4" => Some(DeviceClass::LOW),
        "iPad3,1" => Some(DeviceClass::LOW),
        "iPad3,2" => Some(DeviceClass::LOW),
        "iPad3,3" => Some(DeviceClass::LOW),
        "iPad2,5" => Some(DeviceClass::LOW),
        "iPad2,6" => Some(DeviceClass::LOW),
        "iPad2,7" => Some(DeviceClass::LOW),
        "iPad3,4" => Some(DeviceClass::LOW),
        "iPad3,5" => Some(DeviceClass::LOW),
        "iPad3,6" => Some(DeviceClass::LOW),
        "iPad4,1" => Some(DeviceClass::LOW),
        "iPad4,2" => Some(DeviceClass::LOW),
        "iPad4,3" => Some(DeviceClass::LOW),
        "iPad4,4" => Some(DeviceClass::LOW),
        "iPad4,5" => Some(DeviceClass::LOW),
        "iPad4,6" => Some(DeviceClass::LOW),
        "iPad4,7" => Some(DeviceClass::LOW),
        "iPad4,8" => Some(DeviceClass::LOW),
        "iPad4,9" => Some(DeviceClass::LOW),
        "iPad5,1" => Some(DeviceClass::LOW),
        "iPad5,2" => Some(DeviceClass::LOW),
        "iPad5,3" => Some(DeviceClass::LOW),
        "iPad5,4" => Some(DeviceClass::LOW),
        "iPad6,3" => Some(DeviceClass::MEDIUM),
        "iPad6,4" => Some(DeviceClass::MEDIUM),
        "iPad6,7" => Some(DeviceClass::MEDIUM),
        "iPad6,8" => Some(DeviceClass::MEDIUM),
        "iPad6,11" => Some(DeviceClass::LOW),
        "iPad6,12" => Some(DeviceClass::LOW),
        "iPad7,2" => Some(DeviceClass::MEDIUM),
        "iPad7,3" => Some(DeviceClass::MEDIUM),
        "iPad7,4" => Some(DeviceClass::MEDIUM),
        "iPad7,5" => Some(DeviceClass::MEDIUM),
        "iPad7,6" => Some(DeviceClass::MEDIUM),
        "iPad7,1" => Some(DeviceClass::MEDIUM),
        "iPad7,11" => Some(DeviceClass::MEDIUM),
        "iPad7,12" => Some(DeviceClass::MEDIUM),
        "iPad8,1" => Some(DeviceClass::MEDIUM),
        "iPad8,2" => Some(DeviceClass::MEDIUM),
        "iPad8,3" => Some(DeviceClass::MEDIUM),
        "iPad8,4" => Some(DeviceClass::MEDIUM),
        "iPad8,5" => Some(DeviceClass::MEDIUM),
        "iPad8,6" => Some(DeviceClass::MEDIUM),
        "iPad8,7" => Some(DeviceClass::MEDIUM),
        "iPad8,8" => Some(DeviceClass::MEDIUM),
        "iPad8,9" => Some(DeviceClass::MEDIUM),
        "iPad8,10" => Some(DeviceClass::MEDIUM),
        "iPad8,11" => Some(DeviceClass::MEDIUM),
        "iPad8,12" => Some(DeviceClass::MEDIUM),
        "iPad11,1" => Some(DeviceClass::MEDIUM),
        "iPad11,2" => Some(DeviceClass::MEDIUM),
        "iPad11,3" => Some(DeviceClass::MEDIUM),
        "iPad11,4" => Some(DeviceClass::MEDIUM),
        "iPad11,6" => Some(DeviceClass::MEDIUM),
        "iPad11,7" => Some(DeviceClass::MEDIUM),
        "iPad12,1" => Some(DeviceClass::MEDIUM),
        "iPad12,2" => Some(DeviceClass::MEDIUM),
        "iPad14,1" => Some(DeviceClass::HIGH),
        "iPad14,2" => Some(DeviceClass::HIGH),
        "iPad13,1" => Some(DeviceClass::HIGH),
        "iPad13,2" => Some(DeviceClass::HIGH),
        "iPad13,4" => Some(DeviceClass::HIGH),
        "iPad13,5" => Some(DeviceClass::HIGH),
        "iPad13,6" => Some(DeviceClass::HIGH),
        "iPad13,7" => Some(DeviceClass::HIGH),
        "iPad13,8" => Some(DeviceClass::HIGH),
        "iPad13,9" => Some(DeviceClass::HIGH),
        "iPad13,10" => Some(DeviceClass::HIGH),
        "iPad13,11" => Some(DeviceClass::HIGH),
        "iPad13,16" => Some(DeviceClass::HIGH),
        "iPad13,17" => Some(DeviceClass::HIGH),
        "iPad13,18" => Some(DeviceClass::HIGH),
        "iPad13,19" => Some(DeviceClass::HIGH),
        "iPad14,3" => Some(DeviceClass::HIGH),
        "iPad14,4" => Some(DeviceClass::HIGH),
        "iPad14,5" => Some(DeviceClass::HIGH),
        "iPad14,6" => Some(DeviceClass::HIGH),
        "iPad14,8" => Some(DeviceClass::HIGH),
        "iPad14,9" => Some(DeviceClass::HIGH),
        "iPad14,10" => Some(DeviceClass::HIGH),
        "iPad14,11" => Some(DeviceClass::HIGH),
        "iPad16,1" => Some(DeviceClass::HIGH),
        "iPad16,2" => Some(DeviceClass::HIGH),
        "iPad16,3" => Some(DeviceClass::HIGH),
        "iPad16,4" => Some(DeviceClass::HIGH),
        "iPad16,5" => Some(DeviceClass::HIGH),
        "iPad16,6" => Some(DeviceClass::HIGH),

        // If we don't know the model it's a new device and therefore must be high.
        _ => Some(DeviceClass::HIGH),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iphone17_5_returns_device_class_high() {
        let mut contexts = Contexts::new();
        contexts.add(DeviceContext {
            family: Annotated::new("iOS".to_owned()),
            model: Annotated::new("iPhone17,5".to_owned()),
            ..DeviceContext::default()
        });
        assert_eq!(
            DeviceClass::from_contexts(&contexts),
            Some(DeviceClass::HIGH)
        );
    }

    #[test]
    fn test_iphone99_1_returns_device_class_high() {
        let mut contexts = Contexts::new();
        contexts.add(DeviceContext {
            family: Annotated::new("iOS".to_owned()),
            model: Annotated::new("iPhone99,1".to_owned()),
            ..DeviceContext::default()
        });
        assert_eq!(
            DeviceClass::from_contexts(&contexts),
            Some(DeviceClass::HIGH)
        );
    }

    #[test]
    fn test_ipad99_1_returns_device_class_high() {
        let mut contexts = Contexts::new();
        contexts.add(DeviceContext {
            family: Annotated::new("iOS".to_owned()),
            model: Annotated::new("iPad99,1".to_owned()),
            ..DeviceContext::default()
        });
        assert_eq!(
            DeviceClass::from_contexts(&contexts),
            Some(DeviceClass::HIGH)
        );
    }

    #[test]
    fn test_garbage_device_model_returns_device_class_high() {
        let mut contexts = Contexts::new();
        contexts.add(DeviceContext {
            family: Annotated::new("iOS".to_owned()),
            model: Annotated::new("garbage-device-model".to_owned()),
            ..DeviceContext::default()
        });
        assert_eq!(
            DeviceClass::from_contexts(&contexts),
            Some(DeviceClass::HIGH)
        );
    }

    #[test]
    fn test_wrong_family_returns_none() {
        let mut contexts = Contexts::new();
        contexts.add(DeviceContext {
            family: Annotated::new("iOSS".to_owned()),
            model: Annotated::new("iPhone17,5".to_owned()),
            ..DeviceContext::default()
        });
        assert_eq!(DeviceClass::from_contexts(&contexts), None);
    }
}
