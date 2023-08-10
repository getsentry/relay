use std::error::Error;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// Wraps a serialization / deserialization result to prevent error from bubbling up.
///
/// This is useful for keeping errors in an experimental part of a schema contained.
#[derive(Clone, Debug)]
pub enum ErrorBoundary<T> {
    /// Contains the error value.
    Err(Arc<dyn Error + Send + Sync + 'static>),
    /// Contains the success value.
    Ok(T),
}

impl<T> ErrorBoundary<T> {
    /// Returns `true` if the result is [`Ok`].
    #[inline]
    #[allow(unused)]
    pub fn is_ok(&self) -> bool {
        match *self {
            Self::Ok(_) => true,
            Self::Err(_) => false,
        }
    }

    /// Returns `true` if the result is [`Err`].
    #[inline]
    #[allow(unused)]
    pub fn is_err(&self) -> bool {
        !self.is_ok()
    }

    /// Converts from `Result<T, E>` to [`Option<T>`].
    #[inline]
    pub fn ok(self) -> Option<T> {
        match self {
            ErrorBoundary::Err(_) => None,
            ErrorBoundary::Ok(value) => Some(value),
        }
    }

    /// Returns the contained [`Ok`] value or computes it from a closure.
    #[inline]
    pub fn unwrap_or_else<F>(self, op: F) -> T
    where
        F: FnOnce(&(dyn Error + 'static)) -> T,
    {
        match self {
            Self::Ok(t) => t,
            Self::Err(e) => op(e.as_ref()),
        }
    }

    /// Inserts a value computed from `f` into the error boundary if it is [`Err`],
    /// then returns a mutable reference to the contained value.
    pub fn get_or_insert_with<F>(&mut self, f: F) -> &mut T
    where
        F: FnOnce() -> T,
    {
        if let Self::Err(_) = self {
            *self = Self::Ok(f());
        }

        // SAFETY: an `Err` variant for `self` would have been replaced by a `Ok`
        // variant in the code above.
        match self {
            Self::Ok(t) => t,
            Self::Err(_) => unsafe { std::hint::unreachable_unchecked() },
        }
    }
}

impl<T> Default for ErrorBoundary<T>
where
    T: Default,
{
    fn default() -> Self {
        Self::Ok(T::default())
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
            Err(error) => Self::Err(Arc::new(error)),
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
        let boundary = ErrorBoundary::<u32>::Err(Arc::new(std::fmt::Error));
        assert_eq!(serde_json::to_string(&boundary).unwrap(), "null");
    }
}
