use relay_base_schema::project::ProjectKey;

use crate::Envelope;

/// Struct that represents two project keys.
#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct ProjectKeyPair {
    pub own_key: ProjectKey,
    pub sampling_key: ProjectKey,
}

impl ProjectKeyPair {
    pub fn new(own_key: ProjectKey, sampling_key: ProjectKey) -> Self {
        Self {
            own_key,
            sampling_key,
        }
    }

    pub fn has_distinct_sampling_key(&self) -> bool {
        self.own_key != self.sampling_key
    }

    pub fn from_envelope(envelope: &Envelope) -> Self {
        let own_key = envelope.meta().public_key();
        let sampling_key = envelope.sampling_key().unwrap_or(own_key);

        Self::new(own_key, sampling_key)
    }

    pub fn iter(&self) -> impl Iterator<Item = ProjectKey> {
        let Self {
            own_key,
            sampling_key,
        } = self;

        std::iter::once(*own_key).chain((own_key != sampling_key).then_some(*sampling_key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_project_key_pair_new() {
        let own = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let sampling = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let pair = ProjectKeyPair::new(own, sampling);
        assert_eq!(pair.own_key, own);
        assert_eq!(pair.sampling_key, sampling);
    }

    #[test]
    fn test_project_key_pair_equality() {
        let key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key2 = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let pair1 = ProjectKeyPair::new(key1, key2);
        let pair2 = ProjectKeyPair::new(key1, key2);
        let pair3 = ProjectKeyPair::new(key2, key1);

        assert_eq!(pair1, pair2);
        assert_ne!(pair1, pair3);
    }

    #[test]
    fn test_project_key_pair_ordering() {
        let key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key2 = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let pair1 = ProjectKeyPair::new(key1, key2);
        let pair2 = ProjectKeyPair::new(key2, key1);

        assert!(pair1 < pair2);
    }

    #[test]
    fn test_project_key_pair_hash() {
        let key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key2 = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        let pair1 = ProjectKeyPair::new(key1, key2);
        let pair2 = ProjectKeyPair::new(key1, key2);
        let pair3 = ProjectKeyPair::new(key2, key1);

        let mut set = HashSet::new();
        set.insert(pair1);
        assert!(set.contains(&pair2));
        assert!(!set.contains(&pair3));
    }

    #[test]
    fn test_project_key_pair_iter() {
        let key1 = ProjectKey::parse("a94ae32be2584e0bbd7a4cbb95971fee").unwrap();
        let key2 = ProjectKey::parse("b94ae32be2584e0bbd7a4cbb95971fee").unwrap();

        // Test with different sampling key
        let pair = ProjectKeyPair::new(key1, key2);
        let keys: Vec<_> = pair.iter().collect();
        assert_eq!(keys, vec![key1, key2]);

        // Test with same key (should only yield one key)
        let pair = ProjectKeyPair::new(key1, key1);
        let keys: Vec<_> = pair.iter().collect();
        assert_eq!(keys, vec![key1]);
    }
}
