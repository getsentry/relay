use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use semaphore_common::LogError;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ErrorBoundary<T> {
    Err,
    Ok(T),
}

impl<T> ErrorBoundary<T> {
    #[inline]
    pub fn unwrap_or_else<F>(self, op: F) -> T
    where
        F: FnOnce() -> T,
    {
        match self {
            Self::Ok(t) => t,
            Self::Err => op(),
        }
    }
}

impl<'de, T> Deserialize<'de> for ErrorBoundary<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        Ok(match T::deserialize(value) {
            Ok(t) => Self::Ok(t),
            Err(error) => {
                log::error!("error fetching project state: {}", LogError(&error));
                Self::Err
            }
        })
    }
}

impl<T> Serialize for ErrorBoundary<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let option = match *self {
            Self::Ok(ref t) => Some(t),
            Self::Err => None,
        };

        option.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_ok() {
        let boundary = serde_json::from_str::<ErrorBoundary<u32>>("42").unwrap();
        assert_eq!(boundary, ErrorBoundary::Ok(42));
    }

    #[test]
    fn test_deserialize_err() {
        let boundary = serde_json::from_str::<ErrorBoundary<u32>>("-1").unwrap();
        assert_eq!(boundary, ErrorBoundary::Err);
    }

    #[test]
    fn test_deserialize_syntax_err() {
        serde_json::from_str::<ErrorBoundary<u32>>("---")
            .expect_err("syntax errors should bubble through");
    }

    #[test]
    fn test_serialize_ok() {
        let boundary = ErrorBoundary::Ok(42);
        assert_eq!(serde_json::to_string(&boundary).unwrap(), "42");
    }

    #[test]
    fn test_serialize_err() {
        let boundary = ErrorBoundary::<u32>::Err;
        assert_eq!(serde_json::to_string(&boundary).unwrap(), "null");
    }
}
