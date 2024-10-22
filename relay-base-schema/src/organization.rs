//! Contains [`OrganizationId`] which is the ID of a Sentry organization and is currently a
//! a wrapper over `u64`.

use serde::{Deserialize, Serialize};

/// The unique identifier of a Sentry organization
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct OrganizationId(u64);

impl OrganizationId {
    /// Creates a new organization ID from its numeric value
    #[inline]
    pub fn new(id: u64) -> Self {
        OrganizationId(id)
    }

    /// returns the numeric value of the organization ID
    pub fn value(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for OrganizationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}
