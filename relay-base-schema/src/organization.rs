//! Contains [`OrganizationId`] which is the ID of a Sentry organization and is currently a
//! a wrapper over `u64`.

use serde::{Deserialize, Serialize};

/// The unique identifier of a Sentry organization.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct OrganizationId(u64);

impl OrganizationId {
    /// Creates a new organization ID from its numeric value
    #[inline]
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Returns the numeric value of the organization ID.
    pub fn value(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for OrganizationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let json = r#"[42]"#;
        let ids: Vec<OrganizationId> =
            serde_json::from_str(json).expect("deserialize organization ids");
        assert_eq!(ids, vec![OrganizationId::new(42)]);
    }

    #[test]
    fn test_serialize() {
        let ids = vec![OrganizationId::new(42)];
        let json = serde_json::to_string(&ids).expect("serialize organization ids");
        assert_eq!(json, r#"[42]"#);
    }
}
