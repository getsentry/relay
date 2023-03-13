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
                    if let Some(processor_frequency) = device.processor_frequency.value() {
                        if processor_frequency < &2000 {
                            return Some(DeviceClass::LOW);
                        } else if processor_frequency < &3000 {
                            return Some(DeviceClass::MEDIUM);
                        } else {
                            return Some(DeviceClass::HIGH);
                        }
                    }
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
