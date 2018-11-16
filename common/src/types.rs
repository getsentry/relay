use std::fmt;
use std::str;

use serde::{de, ser::Serializer, Serialize};

pub use human_size::ParsingError as ByteSizeParseError;
use human_size::{Byte, Kibibyte, Kilobyte, Mebibyte, Megabyte, Size, SpecificSize};

/// Represents a size in bytes.
pub struct ByteSize(Size);

impl str::FromStr for ByteSize {
    type Err = ByteSizeParseError;

    fn from_str(value: &str) -> Result<ByteSize, Self::Err> {
        if let Ok(value) = value.parse::<u64>() {
            return Ok(ByteSize(
                SpecificSize::new(value as f64, Byte).unwrap().into(),
            ));
        }
        value.parse().map(ByteSize)
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}", self.0.value(), self.0.multiple())
    }
}

impl fmt::Debug for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        pub struct Helper<'a>(&'a ByteSize);
        impl<'a> fmt::Debug for Helper<'a> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
        f.debug_tuple("ByteSize").field(&Helper(self)).finish()
    }
}

impl ByteSize {
    /// Create a byte size from bytes.
    pub fn from_bytes(value: u64) -> ByteSize {
        let bytes = SpecificSize::new(value as f64, Byte).unwrap();
        macro_rules! try_multiple {
            ($ty:ty) => {
                let v: SpecificSize<$ty> = bytes.into();
                #[cfg_attr(feature = "cargo-clippy", allow(float_cmp))]
                {
                    if v.value() == v.value().trunc() {
                        return ByteSize(v.into());
                    }
                }
            };
        }
        try_multiple!(Megabyte);
        try_multiple!(Mebibyte);
        try_multiple!(Kilobyte);
        try_multiple!(Kibibyte);
        ByteSize(bytes.into())
    }

    /// Create a byte size from kilobytes
    pub fn from_kilobytes(value: u64) -> ByteSize {
        ByteSize::from_bytes(value * 1000)
    }

    /// Create a byte size from megabytes
    pub fn from_megabytes(value: u64) -> ByteSize {
        ByteSize::from_bytes(value * 1_000_000)
    }

    /// Return the value in bytes.
    pub fn as_bytes(&self) -> u64 {
        let size: SpecificSize<Byte> = self.0.into();
        size.value() as u64
    }
}

impl Serialize for ByteSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> de::Deserialize<'de> for ByteSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        struct V;

        impl<'de> de::Visitor<'de> for V {
            type Value = ByteSize;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("data size")
            }

            fn visit_u64<E>(self, value: u64) -> Result<ByteSize, E>
            where
                E: de::Error,
            {
                Ok(ByteSize::from_bytes(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<ByteSize, E>
            where
                E: de::Error,
            {
                value
                    .parse()
                    .map_err(|_| de::Error::invalid_value(de::Unexpected::Str(value), &self))
            }
        }

        deserializer.deserialize_str(V)
    }
}

impl From<u64> for ByteSize {
    fn from(value: u64) -> ByteSize {
        ByteSize::from_bytes(value)
    }
}

#[test]
fn test_byte_size() {
    let size: ByteSize = "42MiB".parse().unwrap();
    assert_eq!(size.as_bytes(), 44_040_192);
    assert_eq!(size.to_string(), "42MiB");

    let size: ByteSize = ByteSize::from_kilobytes(1);
    assert_eq!(size.as_bytes(), 1000);
    assert_eq!(size.to_string(), "1kB");

    let size: ByteSize = ByteSize::from_bytes(1024);
    assert_eq!(size.as_bytes(), 1024);
    assert_eq!(size.to_string(), "1KiB");

    let size: ByteSize = ByteSize::from_megabytes(1);
    assert_eq!(size.as_bytes(), 1_000_000);
    assert_eq!(size.to_string(), "1MB");

    let size: ByteSize = ByteSize::from_bytes(1024 * 1024);
    assert_eq!(size.as_bytes(), 1024 * 1024);
    assert_eq!(size.to_string(), "1MiB");

    let size: ByteSize = ByteSize::from_bytes(1025);
    assert_eq!(size.as_bytes(), 1025);
    assert_eq!(size.to_string(), "1025B");

    let size: ByteSize = "1025".parse().unwrap();
    assert_eq!(size.as_bytes(), 1025);
    assert_eq!(size.to_string(), "1025B");

    let size: ByteSize = "1024KiB".parse().unwrap();
    assert_eq!(size.as_bytes(), 1024 * 1024);
    assert_eq!(size.to_string(), "1024KiB");
}
