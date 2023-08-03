//! This module contains [`ProjectKey`] and [`ProjectId`] types and necessary traits
//! implementations.
//!
//! - [`ProjectId`] is just a wrapper over `u64` and should be considered as implementations details,
//! as it can change in the future,
//! - [`ProjectKey`] is a byte array (`[u8; 32]`) and represents a DSN to identify and authenticate
//! for a project at Sentry.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::macros::impl_str_serde;

/// Raised if a project ID cannot be parsed from a string.
#[derive(Debug, Error, Eq, Ord, PartialEq, PartialOrd)]
pub enum ParseProjectIdError {
    /// Raised if the value is not an integer in the supported range.
    #[error("invalid value for project id")]
    InvalidValue,
    /// Raised if an empty value is parsed.
    #[error("empty or missing project id")]
    EmptyValue,
}

/// The unique identifier of a Sentry project.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Deserialize, Serialize)]
pub struct ProjectId(u64);

impl ProjectId {
    /// Creates a new project ID from its numeric value.
    #[inline]
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the numeric value of this project ID.
    #[inline]
    pub fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.value())
    }
}

impl FromStr for ProjectId {
    type Err = ParseProjectIdError;

    fn from_str(s: &str) -> Result<ProjectId, ParseProjectIdError> {
        if s.is_empty() {
            return Err(ParseProjectIdError::EmptyValue);
        }

        match s.parse::<u64>() {
            Ok(val) => Ok(ProjectId::new(val)),
            Err(_) => Err(ParseProjectIdError::InvalidValue),
        }
    }
}

/// An error parsing [`ProjectKey`].
#[derive(Clone, Copy, Debug)]
pub struct ParseProjectKeyError;

impl fmt::Display for ParseProjectKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid project key")
    }
}

impl std::error::Error for ParseProjectKeyError {}

/// The public key used in a DSN to identify and authenticate for a project at Sentry.
///
/// Project keys are always 32-character hexadecimal strings.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct ProjectKey([u8; 32]);

impl_str_serde!(ProjectKey, "a project key string");

impl ProjectKey {
    /// Parses a `ProjectKey` from a string.
    pub fn parse(key: &str) -> Result<Self, ParseProjectKeyError> {
        if key.len() != 32 || !key.is_ascii() {
            return Err(ParseProjectKeyError);
        }

        let mut project_key = Self(Default::default());
        project_key.0.copy_from_slice(key.as_bytes());
        Ok(project_key)
    }

    /// Parses a `ProjectKey` from a string with flags.
    pub fn parse_with_flags(key: &str) -> Result<(Self, Vec<&str>), ParseProjectKeyError> {
        let mut iter = key.split('.');
        let key = ProjectKey::parse(iter.next().ok_or(ParseProjectKeyError)?)?;
        Ok((key, iter.collect()))
    }

    /// Returns the bytes of the project key.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Returns the string representation of the project key.
    #[inline]
    pub fn as_str(&self) -> &str {
        // Safety: The string is already validated to be of length 32 and valid ASCII when
        // constructing `ProjectKey`.
        unsafe { std::str::from_utf8_unchecked(&self.0) }
    }
}

impl fmt::Debug for ProjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ProjectKey(\"{}\")", self.as_str())
    }
}

impl fmt::Display for ProjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl FromStr for ProjectKey {
    type Err = ParseProjectKeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}
