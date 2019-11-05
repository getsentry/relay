use std::fmt::Debug;

use failure::Fail;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

#[derive(Debug)]
pub enum ErrorBoundary<T> {
    Err(Box<dyn Fail>),
    Ok(T),
}

impl<T> ErrorBoundary<T> {
    #[inline]
    #[allow(unused)]
    pub fn is_ok(&self) -> bool {
        match *self {
            Self::Ok(_) => true,
            Self::Err(_) => false,
        }
    }

    #[inline]
    #[allow(unused)]
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }

    #[inline]
    pub fn unwrap_or_else<F>(self, op: F) -> T
    where
        F: FnOnce(&dyn Fail) -> T,
    {
        match self {
            Self::Ok(t) => t,
            Self::Err(e) => op(e.as_ref()),
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
            Err(error) => Self::Err(Box::new(error)),
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
            Self::Err(_) => None,
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
        assert!(boundary.is_ok());
    }

    #[test]
    fn test_deserialize_err() {
        let boundary = serde_json::from_str::<ErrorBoundary<u32>>("-1").unwrap();
        assert!(boundary.is_err());
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
        let boundary = ErrorBoundary::<u32>::Err(Box::new(std::fmt::Error));
        assert_eq!(serde_json::to_string(&boundary).unwrap(), "null");
    }
}
