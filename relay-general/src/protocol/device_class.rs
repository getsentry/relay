use crate::types::Annotated;
use std::fmt;

#[derive(Debug, FromValue, IntoValue, Empty, Clone, PartialEq)]
pub struct DeviceClass(pub u64);

impl DeviceClass {
    pub const LOW: Self = Self(1);
    pub const MEDIUM: Self = Self(2);
    pub const HIGH: Self = Self(3);
}

impl fmt::Display for DeviceClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.to_string().fmt(f)
    }
}
