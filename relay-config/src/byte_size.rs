use std::convert::TryInto;
use std::fmt;
use std::str::FromStr;

use serde::{de, ser::Serializer, Serialize};

pub use human_size::ParsingError as ByteSizeParseError;
use human_size::{Any, Size, SpecificSize};

/// Represents a size in bytes.
///
/// `ByteSize` can be parsed from strings or with Serde, and remembers the original unit that was
/// used to describe it for stable serialization. Use `ByteSize::infer` to infer the most
/// appropriate unit for a number of bytes.
///
/// Units based on 1000 and 1024 are both supported:
///  - To refer to the 1000-based versions, use "kB" and "MB".
///  - To refer to the 1024-based versions, use "KiB" and "MiB".
///
/// # Examples
///
/// Infer the best unit:
///
/// ```
/// use relay_config::ByteSize;
///
/// let size = ByteSize::infer(42 * 1000 * 1000);
/// assert_eq!("42MB", size.to_string());
/// ```
///
/// Format a 1024-based size to string:
///
/// ```
/// use relay_config::ByteSize;
///
/// let size = ByteSize::kibibytes(42);
/// assert_eq!("42KiB", size.to_string());
/// ```
#[derive(Clone)]
pub struct ByteSize(Size);

impl ByteSize {
    fn multiple(value: u32, multiple: Any) -> Self {
        // Can be unwrapped because f64::from<u32> always returns a "normal" number.
        // See https://doc.rust-lang.org/nightly/std/primitive.f64.html#method.is_normal
        Self(SpecificSize::new(value, multiple).unwrap())
    }

    fn try_multiple(value: u32, multiple: Any) -> Option<Self> {
        let factor = match multiple {
            Any::Mebibyte => 1024 * 1024,
            Any::Megabyte => 1000 * 1000,
            Any::Kibibyte => 1024,
            Any::Kilobyte => 1000,
            _ => 1,
        };

        match value % factor {
            0 => Some(Self::multiple(value / factor, multiple)),
            _ => None,
        }
    }

    /// Create a byte size from bytes, inferring the most appropriate unit.
    pub fn infer(value: u32) -> Self {
        Self::try_multiple(value, Any::Mebibyte)
            .or_else(|| Self::try_multiple(value, Any::Megabyte))
            .or_else(|| Self::try_multiple(value, Any::Kibibyte))
            .or_else(|| Self::try_multiple(value, Any::Kilobyte))
            .unwrap_or_else(|| Self::bytes(value))
    }

    /// Create a byte size from bytes.
    pub fn bytes(value: u32) -> Self {
        Self::multiple(value, Any::Byte)
    }

    /// Create a byte size from 1024-based kibibytes.
    pub fn kibibytes(value: u32) -> Self {
        Self::multiple(value, Any::Kibibyte)
    }

    /// Create a byte size from 1024-based mebibytes.
    pub fn mebibytes(value: u32) -> Self {
        Self::multiple(value, Any::Mebibyte)
    }

    /// Return the value in bytes.
    pub fn as_bytes(&self) -> usize {
        let byte_size = self.0.into::<human_size::Byte>();
        byte_size.value() as usize
    }
}

impl From<u32> for ByteSize {
    fn from(value: u32) -> ByteSize {
        ByteSize::infer(value)
    }
}

impl FromStr for ByteSize {
    type Err = ByteSizeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.parse::<u32>() {
            Ok(bytes) => Ok(Self::bytes(bytes)),
            Err(_) => value.parse().map(ByteSize),
        }
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}", self.0.value(), self.0.multiple())
    }
}

impl fmt::Debug for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ByteSize")
            .field(&format_args!("{}", self.0))
            .finish()
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

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("data size")
            }

            fn visit_u64<E>(self, value: u64) -> Result<ByteSize, E>
            where
                E: de::Error,
            {
                match value.try_into() {
                    Ok(value32) => Ok(ByteSize::infer(value32)),
                    Err(_) => Err(de::Error::invalid_value(
                        de::Unexpected::Unsigned(value),
                        &self,
                    )),
                }
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

        deserializer.deserialize_any(V)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer() {
        let size = ByteSize::infer(42);
        assert_eq!(42, size.as_bytes());
        assert_eq!("42B", size.to_string());

        let size = ByteSize::infer(1000);
        assert_eq!(1000, size.as_bytes());
        assert_eq!("1kB", size.to_string());

        let size = ByteSize::infer(1024);
        assert_eq!(1024, size.as_bytes());
        assert_eq!("1KiB", size.to_string());

        let size = ByteSize::infer(1000 * 1000);
        assert_eq!(1000 * 1000, size.as_bytes());
        assert_eq!("1MB", size.to_string());

        let size = ByteSize::infer(1024 * 1024);
        assert_eq!(1024 * 1024, size.as_bytes());
        assert_eq!("1MiB", size.to_string());
    }

    #[test]
    fn test_parse() {
        let size = ByteSize::from_str("4242").unwrap();
        assert_eq!(4242, size.as_bytes());
        assert_eq!("4242B", size.to_string());

        let size = ByteSize::from_str("42B").unwrap();
        assert_eq!(42, size.as_bytes());
        assert_eq!("42B", size.to_string());

        // NOTE: Lowercase k is kilo
        let size = ByteSize::from_str("1kB").unwrap();
        assert_eq!(1000, size.as_bytes());
        assert_eq!("1kB", size.to_string());

        // NOTE: Uppercase K is kibi
        let size = ByteSize::from_str("1KB").unwrap();
        assert_eq!(1024, size.as_bytes());
        assert_eq!("1KiB", size.to_string());

        let size = ByteSize::from_str("1KiB").unwrap();
        assert_eq!(1024, size.as_bytes());
        assert_eq!("1KiB", size.to_string());

        let size = ByteSize::from_str("1MB").unwrap();
        assert_eq!(1000 * 1000, size.as_bytes());
        assert_eq!("1MB", size.to_string());

        let size = ByteSize::from_str("1MiB").unwrap();
        assert_eq!(1024 * 1024, size.as_bytes());
        assert_eq!("1MiB", size.to_string());
    }

    #[test]
    fn test_as_bytes() {
        let size = ByteSize::bytes(42);
        assert_eq!(42, size.as_bytes());

        let size = ByteSize::kibibytes(42);
        assert_eq!(42 * 1024, size.as_bytes());

        let size = ByteSize::mebibytes(42);
        assert_eq!(42 * 1024 * 1024, size.as_bytes());
    }

    #[test]
    fn test_serde_number() {
        let size = serde_json::from_str::<ByteSize>("1024").unwrap();
        let json = serde_json::to_string(&size).unwrap();
        assert_eq!(json, "\"1KiB\"");
    }

    #[test]
    fn test_serde_string() {
        let size = serde_json::from_str::<ByteSize>("\"1KiB\"").unwrap();
        let json = serde_json::to_string(&size).unwrap();
        assert_eq!(json, "\"1KiB\"");
    }
}
